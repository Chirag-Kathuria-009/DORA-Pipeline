"""Main Airflow DAG for the DORA ICT Incident Intelligence Pipeline.

TaskFlow (@task) DAG that runs the full compliance pipeline every 15 minutes:

    check_kafka_health
      -> sync_iceberg_to_postgres
      -> run_great_expectations
      -> run_dbt_staging
      -> run_dbt_marts
      -> check_compliance_alerts
      -> update_pipeline_metadata

Design notes:
  * Heavy dependencies (pyiceberg, pandas, sqlalchemy, dbt, great_expectations) are
    imported INSIDE each task, not at module level, so the DAG file parses with only
    Airflow installed (required for scheduler parsing and tests/test_dag.py).
  * The task logic reuses the rest of the repo, so the project root must be importable
    at run time. Set DORA_PROJECT_ROOT in the Airflow environment (and mount the repo +
    install the pipeline deps into the Airflow image); it defaults to the path relative
    to this file for local/venv runs.
  * sync_iceberg_to_postgres is the PERMANENT replacement for the throwaway one-off load
    used during Phase 5 testing. It is incremental on the `timestamp` column —
    incidents_classified has no `processed_at` column (that lives in audit_log, per
    decisions.md 2026-06-05), so `timestamp` is the available watermark.
"""

from __future__ import annotations

import logging
import os
import pathlib
from datetime import datetime, timedelta, timezone

from airflow.decorators import dag, task
from airflow.exceptions import AirflowSkipException

logger = logging.getLogger("airflow.task")

# ── Paths / connection helpers (stdlib only at module scope) ─────────────────────

def _project_root() -> pathlib.Path:
    """Return the repo root, from DORA_PROJECT_ROOT or relative to this DAG file."""
    env = os.environ.get("DORA_PROJECT_ROOT")
    if env:
        return pathlib.Path(env)
    return pathlib.Path(__file__).resolve().parents[2]


def _dbt_project_dir() -> pathlib.Path:
    """Return the dbt project directory (transform/dbt_project)."""
    return _project_root() / "transform" / "dbt_project"


def _pg_engine():
    """Create a SQLAlchemy engine for the DORA Postgres (host-side port 5433 default)."""
    from sqlalchemy import create_engine

    return create_engine(
        "postgresql+psycopg2://{u}:{p}@{h}:{port}/{db}".format(
            u=os.environ.get("POSTGRES_USER", "dora"),
            p=os.environ.get("POSTGRES_PASSWORD", "dora"),
            h=os.environ.get("POSTGRES_HOST", "localhost"),
            port=os.environ.get("POSTGRES_PORT", "5433"),
            db=os.environ.get("POSTGRES_DB", "dora"),
        )
    )


def _run_dbt(select: list[str]) -> None:
    """Run `dbt run --select <models>` against the project; raise on non-zero exit.

    Args:
        select: dbt model selectors to run.
    """
    import subprocess

    proj = str(_dbt_project_dir())
    cmd = ["dbt", "run", "--select", *select, "--project-dir", proj, "--profiles-dir", proj]
    logger.info("Running: %s", " ".join(cmd))
    result = subprocess.run(cmd)
    if result.returncode != 0:
        raise RuntimeError(f"dbt run --select {' '.join(select)} failed (exit {result.returncode})")


# ── SLA miss callback ────────────────────────────────────────────────────────────

def _sla_miss_callback(dag, task_list, blocking_task_list, slas, blocking_tis) -> None:
    """Log a warning when any task misses its 5-minute SLA.

    Args:
        dag: The DAG whose task(s) missed SLA.
        task_list: String of tasks that missed SLA.
        blocking_task_list: Tasks blocking the SLA-missing tasks.
        slas: SlaMiss objects for the missed SLAs.
        blocking_tis: TaskInstances blocking the missed SLAs.
    """
    missed = [s.task_id for s in slas]
    logger.warning("[SLA MISS] DAG %s — tasks exceeded the 5-minute SLA: %s", dag.dag_id, missed)


# ── DAG-wide defaults ──────────────────────────────────────────────────────────

default_args = {
    "owner": "dora-pipeline",
    "retries": 1,                          # tests/test_dag.py (6.2) asserts retries >= 1
    "retry_delay": timedelta(minutes=1),
    "email_on_failure": False,             # local dev — logging only
    "sla": timedelta(minutes=5),           # warn (via callback) if a task runs > 5 min
}


@dag(
    dag_id="dora_incident_pipeline",
    schedule=timedelta(minutes=15),
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["dora", "compliance", "fintech"],
    default_args=default_args,
    sla_miss_callback=_sla_miss_callback,
    doc_md=__doc__,
)
def dora_incident_pipeline():
    """Define the DORA incident pipeline DAG and its task dependencies."""

    @task
    def check_kafka_health() -> bool:
        """Ping Kafka; skip the run (AirflowSkipException) if the broker is unreachable.

        Returns:
            True when the broker responds with at least one broker in its metadata.
        """
        from confluent_kafka.admin import AdminClient

        broker = os.environ.get("KAFKA_BROKER", "localhost:9092")
        try:
            metadata = AdminClient({"bootstrap.servers": broker}).list_topics(timeout=10)
        except Exception as exc:  # broker down / unreachable
            raise AirflowSkipException(f"Kafka not reachable at {broker}: {exc}")
        if not metadata.brokers:
            raise AirflowSkipException(f"Kafka reachable but reports no brokers at {broker}")
        logger.info("Kafka healthy at %s (%d broker(s))", broker, len(metadata.brokers))
        return True

    @task
    def sync_iceberg_to_postgres() -> int:
        """Copy new Iceberg incidents_classified rows into Postgres dora.incidents_classified.

        Incremental on the `timestamp` column: on the first run the target table is created
        (replace); subsequently only rows newer than the current max(timestamp) are appended.
        The list column affected_systems is serialised to JSON text for Postgres.

        Returns:
            The number of rows loaded this run (0 if already up to date).
        """
        import json
        import sys

        import pandas as pd
        from sqlalchemy import text

        sys.path.insert(0, str(_project_root()))  # make `storage` importable at run time
        from storage.iceberg_tables import _load_catalog

        schema, table = "dora", "incidents_classified"
        engine = _pg_engine()

        with engine.begin() as conn:
            conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{schema}"'))
            exists = conn.execute(
                text("SELECT to_regclass(:t)"), {"t": f"{schema}.{table}"}
            ).scalar()
            watermark = None
            if exists:
                watermark = conn.execute(
                    text(f'SELECT max("timestamp") FROM "{schema}"."{table}"')
                ).scalar()

        df = _load_catalog().load_table(f"{schema}.{table}").scan().to_pandas()
        if watermark is not None:
            df = df[df["timestamp"] > watermark]

        if df.empty:
            logger.info("sync: no new rows since %s", watermark)
            return 0

        if "affected_systems" in df.columns:
            df["affected_systems"] = df["affected_systems"].apply(
                lambda v: json.dumps(list(v)) if v is not None else None
            )

        df.to_sql(table, engine, schema=schema,
                  if_exists="append" if exists else "replace", index=False)
        logger.info("sync: loaded %d new rows into %s.%s", len(df), schema, table)
        return len(df)

    @task
    def run_great_expectations() -> None:
        """Run the Great Expectations suite; fail the task (and DAG) if any check fails."""
        import subprocess
        import sys

        result = subprocess.run(
            [sys.executable, "-m", "transform.great_expectations.run_validation"],
            cwd=str(_project_root()),
        )
        if result.returncode != 0:
            raise RuntimeError("Great Expectations validation failed — see task logs.")

    @task
    def run_dbt_staging() -> None:
        """dbt run for the staging models (stg_incidents, stg_ict_vendors)."""
        _run_dbt(["stg_incidents", "stg_ict_vendors"])

    @task
    def run_dbt_marts() -> None:
        """dbt run for the mart models (mart_bafin_report, mart_vendor_risk, mart_sla_breach)."""
        _run_dbt(["mart_bafin_report", "mart_vendor_risk", "mart_sla_breach"])

    @task
    def check_compliance_alerts() -> int:
        """Log a warning for every institution flagged NON_COMPLIANT in mart_bafin_report.

        Returns:
            The number of NON_COMPLIANT (reporting_period, institution) rows found.
        """
        from sqlalchemy import text

        with _pg_engine().connect() as conn:
            rows = conn.execute(
                text(
                    "SELECT reporting_period, institution_id, compliance_rate_pct "
                    "FROM public.mart_bafin_report WHERE compliance_status = 'NON_COMPLIANT'"
                )
            ).fetchall()

        for r in rows:
            logger.warning(
                "[COMPLIANCE ALERT] %s — institution %s is NON_COMPLIANT (compliance_rate_pct=%s)",
                r.reporting_period, r.institution_id, r.compliance_rate_pct,
            )
        if not rows:
            logger.info("Compliance check: no NON_COMPLIANT institutions this run.")
        return len(rows)

    @task
    def update_pipeline_metadata() -> None:
        """Write run_id, record_count, and runtime_seconds to the public.pipeline_runs log table."""
        from airflow.operators.python import get_current_context
        from sqlalchemy import text

        ctx = get_current_context()
        dag_run = ctx["dag_run"]
        run_id = dag_run.run_id
        started = dag_run.start_date
        runtime_seconds = (
            (datetime.now(timezone.utc) - started).total_seconds() if started else None
        )

        engine = _pg_engine()
        with engine.begin() as conn:
            conn.execute(
                text(
                    "CREATE TABLE IF NOT EXISTS public.pipeline_runs ("
                    "run_id text, record_count bigint, runtime_seconds double precision, "
                    "logged_at timestamptz DEFAULT now())"
                )
            )
            record_count = conn.execute(
                text('SELECT count(*) FROM "dora"."incidents_classified"')
            ).scalar()
            conn.execute(
                text(
                    "INSERT INTO public.pipeline_runs (run_id, record_count, runtime_seconds) "
                    "VALUES (:run_id, :record_count, :runtime_seconds)"
                ),
                {"run_id": run_id, "record_count": record_count, "runtime_seconds": runtime_seconds},
            )
        logger.info(
            "pipeline_runs <- run_id=%s record_count=%s runtime_seconds=%.1f",
            run_id, record_count, runtime_seconds or 0.0,
        )

    # ── Task dependency chain (strict order from the spec) ───────────────────────
    (
        check_kafka_health()
        >> sync_iceberg_to_postgres()
        >> run_great_expectations()
        >> run_dbt_staging()
        >> run_dbt_marts()
        >> check_compliance_alerts()
        >> update_pipeline_metadata()
    )


dag = dora_incident_pipeline()

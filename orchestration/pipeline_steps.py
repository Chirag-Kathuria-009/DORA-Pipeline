"""Runner-executable pipeline steps for the DORA Airflow DAG (Option B).

Under Option B (decisions.md, Phase 6) every Airflow task runs inside the
`dora/pipeline-runner` Docker image via DockerOperator, because the heavy
pipeline tools (pyiceberg/pandas/sqlalchemy/dbt/great-expectations/confluent-kafka)
cannot co-exist in Airflow 2.8's own env (Airflow pins SQLAlchemy <2.0, PyIceberg
needs >=2.0). DockerOperator runs a *command*, not a Python closure, so the four
pure-Python task bodies that used to live inside dora_pipeline_dag.py are relocated
here as a small CLI. The dbt and Great-Expectations steps already have their own
CLIs (`dbt run`, `python -m transform.great_expectations.run_validation`) and are
invoked directly by the DAG, so they are not duplicated here.

Subcommands (one per DockerOperator task):
    kafka-health        ping Kafka; exit 99 (skip) if the broker is unreachable
    sync                copy new Iceberg incidents_classified rows into Postgres
    compliance-alerts   log every NON_COMPLIANT row in mart_bafin_report
    pipeline-metadata   write run_id/record_count/runtime to public.pipeline_runs

Connection config is read from the environment (POSTGRES_*, KAFKA_BROKER, MinIO,
ICEBERG_CATALOG_URI), which the DAG injects into each runner container with the
in-network endpoints (postgres:5432, kafka:29092, minio:9000).

Usage:
    python -m orchestration.pipeline_steps kafka-health
    python -m orchestration.pipeline_steps sync
    python -m orchestration.pipeline_steps compliance-alerts
    python -m orchestration.pipeline_steps pipeline-metadata --run-id <id> --started <iso>
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import datetime, timezone

# Exit code DockerOperator maps to a SKIPPED task (skip_on_exit_code=99). This is
# the cross-container equivalent of raising AirflowSkipException in-process.
SKIP_EXIT_CODE = 99

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("dora.pipeline_steps")


# ── Shared connection helper ─────────────────────────────────────────────────────

def _pg_engine():
    """Create a SQLAlchemy engine for the DORA Postgres from environment variables.

    Defaults match the host-side .env (localhost:5433); inside a runner container
    the DAG sets POSTGRES_HOST=postgres / POSTGRES_PORT=5432.

    Returns:
        A SQLAlchemy Engine connected to the DORA Postgres database.
    """
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


# ── Step 1: Kafka health ──────────────────────────────────────────────────────────

def check_kafka_health() -> int:
    """Ping Kafka; return SKIP_EXIT_CODE if the broker is unreachable.

    The DAG runs this with DockerOperator skip_on_exit_code=99, so a return of
    SKIP_EXIT_CODE marks the task (and the downstream chain) SKIPPED instead of
    failing the run — the cross-container replacement for AirflowSkipException.

    Returns:
        0 when the broker responds with at least one broker in its metadata,
        SKIP_EXIT_CODE otherwise.
    """
    from confluent_kafka.admin import AdminClient

    broker = os.environ.get("KAFKA_BROKER", "localhost:9092")
    try:
        metadata = AdminClient({"bootstrap.servers": broker}).list_topics(timeout=10)
    except Exception as exc:  # broker down / unreachable
        logger.warning("Kafka not reachable at %s: %s — skipping run", broker, exc)
        return SKIP_EXIT_CODE
    if not metadata.brokers:
        logger.warning("Kafka reachable but reports no brokers at %s — skipping run", broker)
        return SKIP_EXIT_CODE
    logger.info("Kafka healthy at %s (%d broker(s))", broker, len(metadata.brokers))
    return 0


# ── Step 2: Iceberg -> Postgres sync ───────────────────────────────────────────────

def sync_iceberg_to_postgres() -> int:
    """Copy new Iceberg incidents_classified rows into Postgres dora.incidents_classified.

    Incremental on the `timestamp` column: on the first run the target table is created
    (replace); subsequently only rows newer than the current max(timestamp) are appended.
    The list column affected_systems is serialised to JSON text for Postgres. This is the
    PERMANENT replacement for the throwaway one-off load used during Phase 5 testing.
    incidents_classified has no `processed_at` column (that lives in audit_log, per
    decisions.md 2026-06-05), so `timestamp` is the available watermark.

    Returns:
        0 always (logs the number of rows loaded; 0 rows is a valid no-op).
    """
    import json

    import pandas as pd
    from sqlalchemy import text

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
    return 0


# ── Step 6: Compliance alerts ───────────────────────────────────────────────────────

def check_compliance_alerts() -> int:
    """Log a warning for every institution flagged NON_COMPLIANT in mart_bafin_report.

    Returns:
        0 always (the count of NON_COMPLIANT rows is logged, not a failure signal).
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
    return 0


# ── Step 7: Pipeline metadata ───────────────────────────────────────────────────────

def update_pipeline_metadata(run_id: str, started: str | None) -> int:
    """Write run_id, record_count, and runtime_seconds to public.pipeline_runs.

    run_id and started are passed in from the DAG via DockerOperator Jinja templating
    ({{ run_id }} and {{ dag_run.start_date }}) because a DockerOperator task has no
    Airflow context of its own.

    Args:
        run_id:  The Airflow DAG run id for this execution.
        started: ISO-8601 timestamp of the DAG run start, or None if unavailable.

    Returns:
        0 on success.
    """
    from sqlalchemy import text

    runtime_seconds = None
    if started:
        start_dt = datetime.fromisoformat(started)
        if start_dt.tzinfo is None:
            start_dt = start_dt.replace(tzinfo=timezone.utc)
        runtime_seconds = (datetime.now(timezone.utc) - start_dt).total_seconds()

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
    return 0


# ── CLI dispatch ────────────────────────────────────────────────────────────────────

def main() -> None:
    """Parse the subcommand and run the matching pipeline step, exiting with its code."""
    parser = argparse.ArgumentParser(description="DORA pipeline steps (run inside the runner image).")
    sub = parser.add_subparsers(dest="step", required=True)

    sub.add_parser("kafka-health", help="ping Kafka; exit 99 (skip) if unreachable")
    sub.add_parser("sync", help="copy new Iceberg rows into Postgres dora.incidents_classified")
    sub.add_parser("compliance-alerts", help="log NON_COMPLIANT rows from mart_bafin_report")
    meta = sub.add_parser("pipeline-metadata", help="write run metadata to public.pipeline_runs")
    meta.add_argument("--run-id", required=True, help="Airflow DAG run id ({{ run_id }})")
    meta.add_argument("--started", default=None, help="DAG run start ISO ts ({{ dag_run.start_date }})")

    args = parser.parse_args()

    if args.step == "kafka-health":
        code = check_kafka_health()
    elif args.step == "sync":
        code = sync_iceberg_to_postgres()
    elif args.step == "compliance-alerts":
        code = check_compliance_alerts()
    elif args.step == "pipeline-metadata":
        code = update_pipeline_metadata(args.run_id, args.started)
    else:  # argparse(required=True) prevents this, but be explicit
        parser.error(f"unknown step: {args.step}")

    sys.exit(code)


if __name__ == "__main__":
    main()

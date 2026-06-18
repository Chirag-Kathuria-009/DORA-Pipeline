"""Main Airflow DAG for the DORA ICT Incident Intelligence Pipeline (Option B).

TaskFlow @dag that runs the full compliance pipeline every 15 minutes. Each task is a
DockerOperator that runs a command inside the `dora/pipeline-runner` image:

    check_kafka_health
      -> sync_iceberg_to_postgres
      -> run_great_expectations
      -> run_dbt_staging
      -> run_dbt_marts
      -> check_compliance_alerts
      -> update_pipeline_metadata

Why DockerOperator (decisions.md, Phase 6 — Option B): the heavy pipeline tools
(pyiceberg/pandas/sqlalchemy/dbt/great-expectations/confluent-kafka) cannot live in
Airflow 2.8's own Python env — Airflow pins SQLAlchemy <2.0 while PyIceberg's SqlCatalog
needs >=2.0. So every task runs in a separate runner container that carries those deps,
which also maps 1:1 to a prod KubernetesPodOperator. This DAG file therefore imports only
Airflow + the Docker provider — no pipeline deps — so the scheduler parses it cleanly and
tests/test_dag.py (Task 6.2) can import it in an airflow-only venv.

The pure-Python step logic lives in orchestration/pipeline_steps.py (a CLI run inside the
container); dbt and Great Expectations are invoked via their own CLIs. The runner container
talks to the in-network service endpoints (postgres:5432, kafka:29092, minio:9000) and
bind-mounts the host repo so it shares the local SQLite Iceberg catalog (dora_catalog.db)
with the host streaming job. Set DORA_HOST_PROJECT_ROOT (host absolute path to the repo)
in the Airflow environment — docker.sock bind-mount sources resolve on the host.
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta

from airflow.decorators import dag
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

logger = logging.getLogger("airflow.task")

# ── Runner-container configuration ────────────────────────────────────────────────

_RUNNER_IMAGE = "dora/pipeline-runner:latest"
_DORA_NET = "dora-net"
# Host absolute path to the repo, bind-mounted into each task container. docker.sock
# mount sources resolve on the HOST, not inside the scheduler container. Falls back to
# /opt/dora so the DAG still *imports* (test_dag.py) when the var is unset.
_HOST_PROJECT_ROOT = os.environ.get("DORA_HOST_PROJECT_ROOT", "/opt/dora")
_SKIP_EXIT_CODE = 99  # matches orchestration/pipeline_steps.SKIP_EXIT_CODE

# Pipeline env handed to every runner container: in-network endpoints + creds. Creds fall
# back to the local .env defaults; the catalog URI points at the bind-mounted repo so the
# runner shares dora_catalog.db with host processes.
_RUNNER_ENV = {
    "DORA_PROJECT_ROOT": "/opt/dora",
    "PYTHONPATH": "/opt/dora",
    "POSTGRES_HOST": "postgres",
    "POSTGRES_PORT": "5432",
    "POSTGRES_USER": os.environ.get("POSTGRES_USER", "dora"),
    "POSTGRES_PASSWORD": os.environ.get("POSTGRES_PASSWORD", "dora"),
    "POSTGRES_DB": os.environ.get("POSTGRES_DB", "dora"),
    "KAFKA_BROKER": "kafka:29092",
    "MINIO_ENDPOINT": "http://minio:9000",
    "MINIO_ACCESS_KEY": os.environ.get("MINIO_ACCESS_KEY", "minioadmin"),
    "MINIO_SECRET_KEY": os.environ.get("MINIO_SECRET_KEY", "minioadmin"),
    "ICEBERG_CATALOG_URI": "sqlite:////opt/dora/dora_catalog.db",
}


def _runner_task(task_id: str, command, *, skip_on_exit_code: int | None = None) -> DockerOperator:
    """Build a DockerOperator that runs a command in the dora/pipeline-runner image.

    Centralises the shared runner config (image, network, repo bind-mount, env) so the
    seven tasks differ only by command. mount_tmp_dir=False avoids the Windows host-path
    issue DockerOperator hits when mounting its scratch dir over docker.sock.

    Args:
        task_id: Airflow task id.
        command: Command (list or templated string) to run inside the container.
        skip_on_exit_code: Container exit code that marks the task SKIPPED (else None).

    Returns:
        A configured DockerOperator instance.
    """
    return DockerOperator(
        task_id=task_id,
        image=_RUNNER_IMAGE,
        command=command,
        api_version="auto",
        docker_url="unix://var/run/docker.sock",
        network_mode=_DORA_NET,
        mounts=[Mount(source=_HOST_PROJECT_ROOT, target="/opt/dora", type="bind")],
        mount_tmp_dir=False,
        working_dir="/opt/dora",
        environment=_RUNNER_ENV,
        auto_remove="success",
        skip_on_exit_code=skip_on_exit_code,
    )


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
    """Define the DORA incident pipeline DAG and its DockerOperator task chain."""

    # Commands are LISTS (argv) so DockerOperator passes them straight through without
    # shell-splitting; Jinja templates are rendered per-element (no quoting needed).
    _STEPS = ["python", "-m", "orchestration.pipeline_steps"]
    _DBT = ["dbt", "run", "--project-dir", "transform/dbt_project",
            "--profiles-dir", "transform/dbt_project", "--select"]

    check_kafka_health = _runner_task(
        "check_kafka_health",
        _STEPS + ["kafka-health"],
        skip_on_exit_code=_SKIP_EXIT_CODE,
    )
    sync_iceberg_to_postgres = _runner_task(
        "sync_iceberg_to_postgres",
        _STEPS + ["sync"],
    )
    run_great_expectations = _runner_task(
        "run_great_expectations",
        ["python", "-m", "transform.great_expectations.run_validation"],
    )
    run_dbt_staging = _runner_task(
        "run_dbt_staging",
        _DBT + ["stg_incidents", "stg_ict_vendors"],
    )
    run_dbt_marts = _runner_task(
        "run_dbt_marts",
        # int_dora_classified (the intermediate view all 3 marts read via ref()) MUST be
        # selected here — dbt builds selected models in dependency order, so it is created
        # before the marts. Without it the marts fail: relation int_dora_classified missing.
        _DBT + ["int_dora_classified", "mart_bafin_report", "mart_vendor_risk", "mart_sla_breach"],
    )
    check_compliance_alerts = _runner_task(
        "check_compliance_alerts",
        _STEPS + ["compliance-alerts"],
    )
    update_pipeline_metadata = _runner_task(
        "update_pipeline_metadata",
        _STEPS + ["pipeline-metadata", "--run-id", "{{ run_id }}",
                  "--started", "{{ dag_run.start_date }}"],
    )

    # ── Task dependency chain (strict order from the spec) ───────────────────────
    (
        check_kafka_health
        >> sync_iceberg_to_postgres
        >> run_great_expectations
        >> run_dbt_staging
        >> run_dbt_marts
        >> check_compliance_alerts
        >> update_pipeline_metadata
    )


dag = dora_incident_pipeline()

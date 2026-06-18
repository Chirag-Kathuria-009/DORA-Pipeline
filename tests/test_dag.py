"""Static validation of the DORA Airflow DAG — Task 6.2.

These tests parse the DAG WITHOUT running it (no scheduler, no Docker daemon, no DB).
They run in the dedicated airflow venv (see requirements-airflow.txt):

    python -m venv .venv-airflow
    .\\.venv-airflow\\Scripts\\Activate.ps1
    pip install -r requirements-airflow.txt \\
      --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.8.0/constraints-3.11.txt"
    pytest tests/test_dag.py -v

The DAG file imports only airflow + the docker provider (Option B), so it loads here
with no pyiceberg/SQLAlchemy conflict and without a running Docker daemon.

We load ONLY orchestration/dags/dora_pipeline_dag.py into the DagBag (not the whole
dags/ folder) so these tests validate THIS DAG in isolation and stay robust to any other
DAG files added under dags/ in future.
"""

from __future__ import annotations

import pathlib
from datetime import timedelta

import pytest

# Skip the whole module (rather than erroring) if it's run in an env without Airflow,
# e.g. the main .venv — these tests belong in .venv-airflow.
pytest.importorskip("airflow", reason="run in .venv-airflow (see requirements-airflow.txt)")
pytest.importorskip(
    "airflow.providers.docker",
    reason="apache-airflow-providers-docker is required to import the DockerOperator DAG",
)

from airflow.models import DagBag  # noqa: E402  (import after importorskip)
from airflow.utils.dag_cycle_tester import check_cycle  # noqa: E402

DAG_ID = "dora_incident_pipeline"
DAG_FILE = (
    pathlib.Path(__file__).resolve().parents[1]
    / "orchestration" / "dags" / "dora_pipeline_dag.py"
)

# The 7 tasks in their required execution order (the strict linear chain).
EXPECTED_TASK_ORDER = [
    "check_kafka_health",
    "sync_iceberg_to_postgres",
    "run_great_expectations",
    "run_dbt_staging",
    "run_dbt_marts",
    "check_compliance_alerts",
    "update_pipeline_metadata",
]


@pytest.fixture(scope="module")
def dagbag() -> "DagBag":
    """Parse only the DORA pipeline DAG file into a DagBag (once per module).

    Returns:
        A DagBag built from orchestration/dags/dora_pipeline_dag.py.
    """
    return DagBag(dag_folder=str(DAG_FILE), include_examples=False)


@pytest.fixture(scope="module")
def dag(dagbag: "DagBag"):
    """Return the parsed dora_incident_pipeline DAG object.

    Args:
        dagbag: The module-scoped DagBag fixture.

    Returns:
        The DAG registered under DAG_ID.
    """
    return dagbag.get_dag(DAG_ID)


def test_dag_imports_without_errors(dagbag: "DagBag") -> None:
    """The DAG file parses with zero import errors and registers the expected dag_id."""
    assert dagbag.import_errors == {}, f"DAG import errors: {dagbag.import_errors}"
    assert DAG_ID in dagbag.dags, f"{DAG_ID} not found; loaded: {list(dagbag.dags)}"


def test_dag_has_exactly_seven_tasks(dag) -> None:
    """The DAG defines exactly the 7 expected tasks (by task_id)."""
    assert dag is not None
    assert len(dag.tasks) == 7, f"expected 7 tasks, found {len(dag.tasks)}"
    assert set(dag.task_ids) == set(EXPECTED_TASK_ORDER)


def test_task_dependencies_are_sequential(dag) -> None:
    """Each task depends on the previous one — a strict linear chain in order."""
    for i, task_id in enumerate(EXPECTED_TASK_ORDER):
        task = dag.get_task(task_id)
        expected_upstream = {EXPECTED_TASK_ORDER[i - 1]} if i > 0 else set()
        expected_downstream = {EXPECTED_TASK_ORDER[i + 1]} if i < len(EXPECTED_TASK_ORDER) - 1 else set()
        assert task.upstream_task_ids == expected_upstream, (
            f"{task_id} upstream={task.upstream_task_ids}, expected {expected_upstream}"
        )
        assert task.downstream_task_ids == expected_downstream, (
            f"{task_id} downstream={task.downstream_task_ids}, expected {expected_downstream}"
        )


def test_dag_has_no_cycles(dag) -> None:
    """The DAG contains no dependency cycles (check_cycle raises if it does)."""
    check_cycle(dag)  # raises AirflowDagCycleException on a cycle → test fails


def test_schedule_interval_is_correct(dag) -> None:
    """The DAG runs every 15 minutes."""
    assert dag.schedule_interval == timedelta(minutes=15)


def test_all_tasks_have_retries(dag) -> None:
    """Every task is configured with at least one retry (via default_args)."""
    for task in dag.tasks:
        assert task.retries >= 1, f"{task.task_id} has retries={task.retries} (< 1)"

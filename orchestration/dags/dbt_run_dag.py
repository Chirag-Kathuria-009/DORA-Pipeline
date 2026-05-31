"""Airflow DAG for dbt transformation runs.

Runs all dbt models in dependency order: staging → intermediate → marts.
Triggered by the main dora_pipeline_dag after streaming data lands in
the Iceberg tables.
"""

from datetime import datetime


def build_dbt_dag():
    """Construct and return the dbt transformation Airflow DAG.

    Defines dbt task sequence:
      run_staging >> run_intermediate >> run_marts >> run_dbt_tests

    Returns:
        An Airflow DAG object for the dbt transformation workflow.
    """
    raise NotImplementedError("Implemented in Phase 6")


dag = build_dbt_dag()

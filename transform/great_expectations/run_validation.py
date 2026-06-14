"""Run the dora_incidents_quality Great Expectations suite against Postgres.

Loads the expectation suite from expectations/incidents_suite.json, reads the
incidents_classified table from the DORA Postgres database into a DataFrame,
validates it against the suite, prints a pass/fail summary, and exits non-zero
if any expectation fails — so Airflow (Phase 6) can fail the task on a data
quality regression.

Targets Great Expectations 0.18.x (the version pinned in requirements.txt).

Prerequisites (this script does NOT set them up):
  - pip install 'great-expectations>=0.18,<1.0'
  - The Iceberg->Postgres sync must have populated <schema>.incidents_classified
    in the Postgres database (default schema 'dora').

Usage:
    python -m transform.great_expectations.run_validation
"""

import json
import os
import pathlib
import sys

import great_expectations as ge
import pandas as pd
from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.core.expectation_suite import ExpectationSuite
from sqlalchemy import create_engine, text

_SUITE_PATH = pathlib.Path(__file__).resolve().parent / "expectations" / "incidents_suite.json"


def build_engine():
    """Create a SQLAlchemy engine for the host-side DORA Postgres connection.

    Reads POSTGRES_* from the environment, falling back to the local-dev defaults
    from .env (the Docker Postgres is published on host port 5433 — see
    decisions.md 2026-06-12 | infra).

    Returns:
        A SQLAlchemy Engine connected to the DORA Postgres database.
    """
    host = os.environ.get("POSTGRES_HOST", "localhost")
    port = os.environ.get("POSTGRES_PORT", "5433")
    user = os.environ.get("POSTGRES_USER", "dora")
    password = os.environ.get("POSTGRES_PASSWORD", "dora")
    dbname = os.environ.get("POSTGRES_DB", "dora")
    return create_engine(f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}")


def load_suite():
    """Load the dora_incidents_quality expectation suite from its JSON file.

    Returns:
        A Great Expectations ExpectationSuite built from incidents_suite.json.
    """
    raw = json.loads(_SUITE_PATH.read_text(encoding="utf-8"))
    expectations = [ExpectationConfiguration(**cfg) for cfg in raw["expectations"]]
    return ExpectationSuite(
        expectation_suite_name=raw["expectation_suite_name"],
        expectations=expectations,
        meta=raw.get("meta", {}),
    )


def fetch_incidents(engine, schema: str, table: str) -> pd.DataFrame:
    """Read the incidents_classified table from Postgres into a DataFrame.

    Args:
        engine: An active SQLAlchemy engine.
        schema: Postgres schema holding the table (default 'dora').
        table:  Table name (default 'incidents_classified').

    Returns:
        A pandas DataFrame of all rows in <schema>.<table>.
    """
    with engine.connect() as conn:
        return pd.read_sql(text(f'SELECT * FROM "{schema}"."{table}"'), conn)


def run_validation() -> int:
    """Validate the Postgres incidents table against the suite and report results.

    Loads the suite, reads the table, runs every expectation, prints a per-
    expectation PASS/FAIL summary with counts, and returns an exit code.

    Returns:
        0 if every expectation passed, 1 if any failed or the table is unreadable.
    """
    schema = os.environ.get("DORA_PG_SCHEMA", "dora")
    table = os.environ.get("DORA_PG_TABLE", "incidents_classified")

    suite = load_suite()
    engine = build_engine()

    try:
        df = fetch_incidents(engine, schema, table)
    except Exception as exc:  # connection error or missing table — fail clearly
        print(f"[FATAL] could not read {schema}.{table}: {exc}")
        print("        Ensure Postgres is up (localhost:5433) and the Iceberg->Postgres")
        print("        sync has populated the table before running validation.")
        return 1

    dataset = ge.from_pandas(df, expectation_suite=suite)
    result = dataset.validate(result_format="SUMMARY")

    total = len(result.results)
    passed = sum(1 for r in result.results if r.success)
    failed = total - passed

    print("=" * 66)
    print(f"  GE SUITE: {suite.expectation_suite_name}  (rows validated: {len(df):,})")
    print("=" * 66)
    for r in result.results:
        cfg = r.expectation_config
        column = cfg.kwargs.get("column", cfg.kwargs.get("column_A", "-"))
        mark = "PASS" if r.success else "FAIL"
        unexpected = r.result.get("unexpected_count")
        detail = f"unexpected={unexpected}" if unexpected is not None else ""
        print(f"  [{mark}] {cfg.expectation_type:<42} {column:<28} {detail}")
    print("-" * 66)
    print(f"  {passed}/{total} expectations passed, {failed} failed")
    print("=" * 66)

    return 0 if result.success else 1


def main() -> None:
    """Entry point: run validation and exit with its result code (0 pass / 1 fail)."""
    sys.exit(run_validation())


if __name__ == "__main__":
    main()

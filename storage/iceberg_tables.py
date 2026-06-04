"""Apache Iceberg table definitions for the DORA pipeline.

Creates three tables under the 'dora' namespace using a SqlCatalog whose
metadata pointer lives in a local SQLite file; the actual Iceberg data and
metadata files are written to MinIO (s3://dora-lakehouse/iceberg).

NOTE on the catalog choice: PyIceberg (the Python library) has never shipped a
HadoopCatalog — that is a Java-Iceberg concept. SqlCatalog backed by a local
SQLite file is the closest equivalent that needs no extra running service,
which was the original intent. See decisions.md (Phase 4, REVISED).

Table schemas exactly mirror ingestion/simulator/schema.py (IncidentEvent).
Field IDs are stable — do not renumber once tables exist in production.

Usage:
    python -m storage.iceberg_tables
    python -m storage.iceberg_tables --warehouse s3://my-bucket/iceberg
"""

import argparse
import os
import pathlib

from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.exceptions import NamespaceAlreadyExistsError, NoSuchTableError
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import DayTransform, IdentityTransform
from pyiceberg.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    ListType,
    NestedField,
    StringType,
    TimestamptzType,
)

# Project root = parent of this storage/ directory. Used so the SQLite catalog
# file resolves to the same absolute path regardless of the caller's CWD.
_PROJECT_ROOT        = pathlib.Path(__file__).resolve().parent.parent
_DEFAULT_WAREHOUSE   = "s3://dora-lakehouse/iceberg"
_DEFAULT_ENDPOINT    = "http://localhost:9000"
_DEFAULT_CATALOG_URI = f"sqlite:///{(_PROJECT_ROOT / 'dora_catalog.db').as_posix()}"
_DEFAULT_REGION      = "us-east-1"
_NAMESPACE           = "dora"


# ── Catalog ───────────────────────────────────────────────────────────────────

def _load_catalog(warehouse: str | None = None, endpoint: str | None = None):
    """Return a SqlCatalog whose metadata lives in SQLite and data lives in MinIO.

    The catalog pointer database is a local SQLite file (ICEBERG_CATALOG_URI env
    var, else <project_root>/dora_catalog.db). Table data and metadata files are
    written to the S3/MinIO warehouse via PyArrow's native S3 implementation.

    All parameters fall back to the corresponding environment variable, then
    to local-dev defaults so the function works out-of-the-box after
    `cp .env.example .env`.

    Args:
        warehouse: S3 URI for the warehouse root directory.
                   Defaults to s3://dora-lakehouse/iceberg.
        endpoint:  MinIO endpoint URL. Reads MINIO_ENDPOINT from env if None.

    Returns:
        A configured PyIceberg SqlCatalog instance.
    """
    resolved_warehouse = warehouse or _DEFAULT_WAREHOUSE
    resolved_endpoint  = endpoint  or os.environ.get("MINIO_ENDPOINT",  _DEFAULT_ENDPOINT)
    catalog_uri        = os.environ.get("ICEBERG_CATALOG_URI", _DEFAULT_CATALOG_URI)
    access_key         = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
    secret_key         = os.environ.get("MINIO_SECRET_KEY", "minioadmin")
    # Explicit region avoids a needless AWS region-resolution call (and warning)
    # that PyArrow otherwise makes against the MinIO endpoint.
    region             = os.environ.get("AWS_REGION") or os.environ.get("MINIO_REGION", _DEFAULT_REGION)

    return SqlCatalog(
        "dora",
        **{
            "uri":                  catalog_uri,
            "warehouse":            resolved_warehouse,
            "py-io-impl":           "pyiceberg.io.pyarrow.PyArrowFileIO",
            "s3.endpoint":          resolved_endpoint,
            "s3.region":            region,
            "s3.access-key-id":     access_key,
            "s3.secret-access-key": secret_key,
        },
    )


# ── Schema builders ───────────────────────────────────────────────────────────

def _base_incident_fields() -> list[NestedField]:
    """Return the 15 NestedFields that mirror every IncidentEvent field.

    Field IDs are intentionally stable integers (1–15 top-level, 100 for
    the list element inside affected_systems). Renaming or reassigning them
    after first write would corrupt Iceberg metadata.

    Returns:
        A list of NestedField objects ready to be unpacked into a Schema.
    """
    return [
        NestedField(field_id=1,  name="incident_id",
                    field_type=StringType(),       required=True),
        NestedField(field_id=2,  name="timestamp",
                    field_type=TimestamptzType(),   required=True),
        NestedField(field_id=3,  name="institution_id",
                    field_type=StringType(),       required=True),
        NestedField(field_id=4,  name="institution_type",
                    field_type=StringType(),       required=True),
        NestedField(field_id=5,  name="incident_type",
                    field_type=StringType(),       required=True),
        NestedField(field_id=6,  name="affected_systems",
                    field_type=ListType(
                        element_id=100,
                        element_type=StringType(),
                        element_required=False,
                    ),
                    required=True),
        NestedField(field_id=7,  name="clients_affected_pct",
                    field_type=DoubleType(),       required=True),
        NestedField(field_id=8,  name="financial_impact_eur",
                    field_type=DoubleType(),       required=True),
        NestedField(field_id=9,  name="detection_timestamp",
                    field_type=TimestamptzType(),   required=True),
        NestedField(field_id=10, name="containment_timestamp",
                    field_type=TimestamptzType(),   required=False),
        NestedField(field_id=11, name="ict_third_party_provider",
                    field_type=StringType(),       required=False),
        NestedField(field_id=12, name="is_cross_border",
                    field_type=BooleanType(),      required=True),
        NestedField(field_id=13, name="dora_severity",
                    field_type=StringType(),       required=False),
        NestedField(field_id=14, name="bafin_notification_required",
                    field_type=BooleanType(),      required=False),
        NestedField(field_id=15, name="bafin_notification_deadline_hours",
                    field_type=IntegerType(),      required=False),
    ]


def _incidents_raw_schema() -> Schema:
    """Return the Iceberg schema for dora.incidents_raw.

    Mirrors IncidentEvent exactly. DORA classification fields are optional
    because raw events arrive before the classifier runs.
    """
    return Schema(*_base_incident_fields())


def _incidents_classified_schema() -> Schema:
    """Return the Iceberg schema for dora.incidents_classified.

    Extends the base IncidentEvent schema with classification_reason (field_id=16),
    the human-readable string produced by DORAClassifier.get_classification_reason().
    """
    fields = _base_incident_fields()
    fields.append(
        NestedField(
            field_id=16,
            name="classification_reason",
            field_type=StringType(),
            required=False,
        )
    )
    return Schema(*fields)


def _audit_log_schema() -> Schema:
    """Return the Iceberg schema for dora.audit_log.

    Stores one row per processed incident for end-to-end lineage:
    which pipeline/classifier version processed each event and its content hash.
    """
    return Schema(
        NestedField(field_id=1, name="incident_id",        field_type=StringType(),      required=True),
        NestedField(field_id=2, name="processed_at",       field_type=TimestamptzType(),  required=True),
        NestedField(field_id=3, name="pipeline_version",   field_type=StringType(),      required=True),
        NestedField(field_id=4, name="classifier_version", field_type=StringType(),      required=True),
        NestedField(field_id=5, name="record_hash",        field_type=StringType(),      required=True),
    )


# ── Partition specs ───────────────────────────────────────────────────────────

def _raw_partition_spec() -> PartitionSpec:
    """Partition incidents_raw by calendar day derived from the event timestamp.

    One UTC-day partition per source_id=2 (timestamp). Keeps file sizes
    manageable at sustained simulator load and aligns with daily dbt runs.
    """
    return PartitionSpec(
        PartitionField(
            source_id=2,
            field_id=1000,
            name="timestamp_day",
            transform=DayTransform(),
        )
    )


def _classified_partition_spec() -> PartitionSpec:
    """Partition incidents_classified by days(timestamp) then dora_severity.

    Two-level layout lets queries such as 'all CRITICAL incidents today'
    skip ~93% of files (minor + major partitions are separate sub-dirs).
    source_id=2 = timestamp, source_id=13 = dora_severity.
    """
    return PartitionSpec(
        PartitionField(
            source_id=2,
            field_id=1000,
            name="timestamp_day",
            transform=DayTransform(),
        ),
        PartitionField(
            source_id=13,
            field_id=1001,
            name="dora_severity",
            transform=IdentityTransform(),
        ),
    )


def _audit_partition_spec() -> PartitionSpec:
    """Partition audit_log by calendar day derived from processed_at.

    Aligns with the 15-minute Airflow DAG cadence — all audit rows for a
    given processing day land in the same partition directory.
    source_id=2 = processed_at.
    """
    return PartitionSpec(
        PartitionField(
            source_id=2,
            field_id=1000,
            name="processed_at_day",
            transform=DayTransform(),
        )
    )


# ── Table creation ────────────────────────────────────────────────────────────

def _create_or_skip(
    catalog,
    identifier: str,
    schema: Schema,
    partition_spec: PartitionSpec,
) -> None:
    """Create an Iceberg table, or print [SKIP] if it already exists.

    Idempotent — safe to call on an already-bootstrapped catalog without
    raising errors or overwriting existing data.

    Args:
        catalog:        Active PyIceberg catalog instance.
        identifier:     Fully-qualified table name, e.g. "dora.incidents_raw".
        schema:         Iceberg Schema to create the table with.
        partition_spec: Partition layout for the new table.
    """
    try:
        catalog.load_table(identifier)
        print(f"  [SKIP]    {identifier} — already exists")
    except NoSuchTableError:
        table = catalog.create_table(
            identifier=identifier,
            schema=schema,
            partition_spec=partition_spec,
        )
        print(f"  [CREATED] {identifier}")
        print(f"            {table.schema()}\n")


def create_all_tables(
    warehouse: str | None = None,
    endpoint: str | None = None,
) -> None:
    """Create all three DORA pipeline Iceberg tables under the 'dora' namespace.

    Tables created (all operations are idempotent):
      dora.incidents_raw        — raw IncidentEvent fields, partition by days(timestamp)
      dora.incidents_classified — base fields + classification_reason, partition by
                                  days(timestamp) and dora_severity
      dora.audit_log            — pipeline audit trail, partition by days(processed_at)

    Args:
        warehouse: S3 URI for the HadoopCatalog root.
                   Defaults to s3://dora-lakehouse/iceberg.
        endpoint:  MinIO endpoint URL. Defaults to MINIO_ENDPOINT env var.
    """
    catalog = _load_catalog(warehouse=warehouse, endpoint=endpoint)
    #test
    try:
        catalog.create_namespace(_NAMESPACE)
        print(f"[CREATED] namespace '{_NAMESPACE}'\n")
    except NamespaceAlreadyExistsError:
        print(f"[SKIP]    namespace '{_NAMESPACE}' already exists\n")

    _create_or_skip(catalog, "dora.incidents_raw",        _incidents_raw_schema(),        _raw_partition_spec())
    _create_or_skip(catalog, "dora.incidents_classified", _incidents_classified_schema(), _classified_partition_spec())
    _create_or_skip(catalog, "dora.audit_log",            _audit_log_schema(),            _audit_partition_spec())

    print("Iceberg table setup complete.")


def main() -> None:
    """Bootstrap all Iceberg tables and print their schemas.

    Accepts optional CLI overrides for warehouse path and MinIO endpoint.
    Credentials are always read from MINIO_ACCESS_KEY / MINIO_SECRET_KEY
    environment variables, not from CLI flags, to prevent secret leakage
    into shell history.
    """
    parser = argparse.ArgumentParser(
        description="Bootstrap DORA pipeline Iceberg tables on MinIO.",
    )
    parser.add_argument(
        "--warehouse",
        default=None,
        help=f"S3 URI for the Iceberg catalog root (default: {_DEFAULT_WAREHOUSE})",
    )
    parser.add_argument(
        "--endpoint",
        default=None,
        help="MinIO endpoint URL (default: MINIO_ENDPOINT env var or http://localhost:9000)",
    )
    args = parser.parse_args()

    print("DORA Pipeline — Iceberg table bootstrap")
    print("=" * 45)
    create_all_tables(warehouse=args.warehouse, endpoint=args.endpoint)


if __name__ == "__main__":
    main()

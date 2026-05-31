"""Apache Iceberg table definitions — create and migrate incident tables.

Uses PyIceberg with HadoopCatalog backed by MinIO (see decisions.md).
Table schemas must mirror the IncidentEvent fields defined in
ingestion/simulator/schema.py.
"""


def create_tables(catalog_uri: str = None) -> None:
    """Create all Iceberg tables required by the DORA pipeline.

    Creates the incidents table with partitioning by severity and date.
    Idempotent — safe to run multiple times; skips tables that already
    exist. Uses HadoopCatalog (not REST catalog — see decisions.md).

    Args:
        catalog_uri: Path to the Hadoop catalog directory in MinIO.
                     Defaults to the value from environment variables.
    """
    raise NotImplementedError("Implemented in Phase 4")

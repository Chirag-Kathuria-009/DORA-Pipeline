"""PySpark Structured Streaming job for real-time incident processing.

Consumes from the dora.incidents.enriched Kafka topic, applies
DORAClassifier, and writes results to Apache Iceberg tables in MinIO.
Uses PySpark Structured Streaming (not Flink — see decisions.md).

────────────────────────────────────────────────────────────────────────────
IMPORTANT — how Iceberg storage is split (read before implementing writes):

  • Table DATA + metadata files  → live in MinIO  (durable, shared by all jobs)
  • Catalog INDEX (the pointer to
    the current metadata.json)     → lives in a LOCAL SQLite file
                                     <project_root>/dora_catalog.db
                                     (override via ICEBERG_CATALOG_URI)

  Consequence for this job:
    - Do NOT create your own catalog here. Always obtain the catalog through
      storage.iceberg_tables._load_catalog() (or call create_all_tables() at
      startup) so this job uses the SAME SQLite index every other script uses.
      Two different catalogs = tables one process writes the other can't see.
    - PyIceberg is NOT a HadoopCatalog (PyIceberg has none) — it is a
      SqlCatalog. See decisions.md (Phase 4, REVISED).
────────────────────────────────────────────────────────────────────────────
"""

from processing.dora_classifier import DORAClassifier
from storage.iceberg_tables import create_all_tables


def run_streaming_job() -> None:
    """Start the PySpark Structured Streaming pipeline.

    Reads from Kafka, deserialises IncidentEvent records, classifies
    each event via DORAClassifier, enriches via enrichment.py, and
    writes to the appropriate Iceberg table partition. Runs until
    manually stopped or the Spark session is terminated.

    Calls create_all_tables() at startup so the Iceberg tables (and the
    shared SQLite catalog index) exist before the first micro-batch writes.
    """
    raise NotImplementedError("Implemented in Phase 4")


if __name__ == "__main__":
    run_streaming_job()

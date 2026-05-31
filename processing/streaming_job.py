"""PySpark Structured Streaming job for real-time incident processing.

Consumes from the dora.incidents.enriched Kafka topic, applies
DORAClassifier, and writes results to Apache Iceberg tables in MinIO.
Uses PySpark Structured Streaming (not Flink — see decisions.md).
"""

from processing.dora_classifier import DORAClassifier
from storage.iceberg_tables import create_tables


def run_streaming_job() -> None:
    """Start the PySpark Structured Streaming pipeline.

    Reads from Kafka, deserialises IncidentEvent records, classifies
    each event via DORAClassifier, enriches via enrichment.py, and
    writes to the appropriate Iceberg table partition. Runs until
    manually stopped or the Spark session is terminated.
    """
    raise NotImplementedError("Implemented in Phase 4")


if __name__ == "__main__":
    run_streaming_job()

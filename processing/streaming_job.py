"""PySpark Structured Streaming job for real-time DORA incident processing.

Consumes the three severity Kafka topics (critical / major / minor), rebuilds
each IncidentEvent, classifies it with DORAClassifier, stamps audit metadata,
and fans every micro-batch out to two sinks:

    1. Apache Iceberg (append) — dora.incidents_classified  (analytical store)
                               + dora.audit_log              (compliance lineage)
    2. Kafka topic            — dora.incidents.enriched      (downstream consumers)

Uses PySpark Structured Streaming, not Flink (see decisions.md).

────────────────────────────────────────────────────────────────────────────
WHY THIS SHAPE (decisions.md, 2026-06-05 | streaming — Option A):

  Spark is ONLY the Kafka reader + micro-batch trigger + checkpoint owner.
  It does NOT write Iceberg. Classification and the Iceberg append run
  DRIVER-SIDE inside foreachBatch via PyIceberg, reusing the SAME SqlCatalog
  (_load_catalog) that iceberg_tables.py created — so there is exactly ONE
  catalog pointer (the local SQLite dora_catalog.db) that every script shares.
  Two catalogs = tables one process writes that the other cannot see.

  The audit columns (processed_at / pipeline_version / record_hash) are NOT
  added to dora.incidents_classified — that table's Iceberg schema is fixed at
  16 fields. They are written to dora.audit_log, whose schema already has them,
  and are also carried on the enriched Kafka payload. processed_at and
  record_hash are computed in Python (the semantics of current_timestamp() and
  sha2(concat(...), 256)) because classification is a driver-side Pydantic
  operation under Option A.
────────────────────────────────────────────────────────────────────────────

Usage:
    python -m processing.streaming_job
    python -m processing.streaming_job --broker localhost:9092
"""

import argparse
import hashlib
import json
import os
import signal
import sys
from collections import Counter
from datetime import datetime, timezone

import pyarrow as pa
from confluent_kafka import Producer
from pyspark.sql import SparkSession

from ingestion.simulator.schema import IncidentEvent
from processing.dora_classifier import DORAClassifier
from storage.iceberg_tables import _load_catalog, create_all_tables

# ── Constants ──────────────────────────────────────────────────────────────────
PIPELINE_VERSION   = "1.0.0"            # stamped on every audit row + enriched msg
CLASSIFIER_VERSION = "1.0.0"            # DORAClassifier rules version (audit lineage)
CHECKPOINT_LOCATION = "/tmp/checkpoints/streaming_job"
APP_NAME            = "dora-incident-streaming"

# Spark needs the Kafka source connector JAR; pinned to the Spark version below.
_SPARK_VERSION       = "3.5.1"
_SPARK_KAFKA_PACKAGE = f"org.apache.spark:spark-sql-kafka-0-10_2.12:{_SPARK_VERSION}"

# Iceberg table identifiers (created by storage.iceberg_tables).
_TBL_CLASSIFIED = "dora.incidents_classified"
_TBL_AUDIT      = "dora.audit_log"

# Local-dev defaults; every value is overridable via .env / environment.
_DEFAULT_BROKER   = "localhost:9092"
_DEFAULT_ENDPOINT = "http://localhost:9000"


# ── Configuration helpers ───────────────────────────────────────────────────────

def _source_topics() -> str:
    """Return the comma-separated list of the three severity source topics.

    Reads each topic name from its KAFKA_TOPIC_* environment variable, falling
    back to the canonical dora.incidents.<tier> names so the job runs without a
    populated .env. Spark's "subscribe" option expects this comma-joined form.

    Returns:
        A string such as
        "dora.incidents.critical,dora.incidents.major,dora.incidents.minor".
    """
    return ",".join(
        (
            os.environ.get("KAFKA_TOPIC_CRITICAL", "dora.incidents.critical"),
            os.environ.get("KAFKA_TOPIC_MAJOR",    "dora.incidents.major"),
            os.environ.get("KAFKA_TOPIC_MINOR",    "dora.incidents.minor"),
        )
    )


def _enriched_topic() -> str:
    """Return the destination topic for enriched records.

    Reads KAFKA_TOPIC_ENRICHED, defaulting to dora.incidents.enriched.

    Returns:
        The enriched Kafka topic name.
    """
    return os.environ.get("KAFKA_TOPIC_ENRICHED", "dora.incidents.enriched")


def build_spark_session() -> SparkSession:
    """Build the SparkSession used to read Kafka and drive the micro-batch trigger.

    Pulls the spark-sql-kafka connector JAR via spark.jars.packages and applies
    the MinIO s3a settings requested by the task spec. Note: under Option A the
    Iceberg write does NOT go through s3a — PyIceberg/PyArrow's native S3 client
    (configured inside _load_catalog) does — and the checkpoint lives on the
    local filesystem, so these s3a values are configured for spec-completeness
    and any future Spark-side S3 access rather than being exercised by this job.

    MinIO credentials and endpoint are read from the environment
    (MINIO_ENDPOINT / MINIO_ACCESS_KEY / MINIO_SECRET_KEY).

    Returns:
        A configured, ready-to-use SparkSession.
    """
    endpoint   = os.environ.get("MINIO_ENDPOINT",   _DEFAULT_ENDPOINT)
    access_key = os.environ.get("MINIO_ACCESS_KEY",  "minioadmin")
    secret_key = os.environ.get("MINIO_SECRET_KEY",  "minioadmin")

    return (
        SparkSession.builder
        .appName(APP_NAME)
        .config("spark.jars.packages", _SPARK_KAFKA_PACKAGE)
        .config("spark.sql.shuffle.partitions", "4")  # small local stream
        .config("spark.hadoop.fs.s3a.endpoint",          endpoint)
        .config("spark.hadoop.fs.s3a.access.key",        access_key)
        .config("spark.hadoop.fs.s3a.secret.key",        secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .getOrCreate()
    )


# ── Record shaping (driver-side, pure Python) ────────────────────────────────────

def compute_record_hash(classified: IncidentEvent) -> str:
    """Return the SHA-256 hex digest of every field of a classified incident.

    Implements the sha2(concat of all fields, 256) audit requirement. The event
    is serialised to its canonical Kafka dict (UUID/datetime -> ISO strings) and
    JSON-dumped with sorted keys so the concatenation is deterministic and
    reproducible regardless of field insertion order. The digest is
    tamper-evidence: any later change to a stored field will not match this hash.

    Args:
        classified: A fully classified IncidentEvent (all DORA fields populated).

    Returns:
        A 64-character lowercase hex SHA-256 digest string.
    """
    canonical = json.dumps(classified.to_kafka_message(), sort_keys=True)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def classified_to_iceberg_row(classified: IncidentEvent, reason: str) -> dict:
    """Build one dora.incidents_classified row dict from a classified incident.

    Returns the 16 columns of the incidents_classified Iceberg schema using the
    incident's typed fields directly (datetimes stay tz-aware, the UUID becomes a
    string to match Iceberg's StringType). No audit columns are added here — the
    table schema does not contain them.

    Args:
        classified: A classified IncidentEvent.
        reason:     The human-readable string from get_classification_reason().

    Returns:
        A dict keyed by Iceberg column name, ready for pa.Table.from_pylist().
    """
    return {
        "incident_id":                       str(classified.incident_id),
        "timestamp":                         classified.timestamp,
        "institution_id":                    classified.institution_id,
        "institution_type":                  classified.institution_type,
        "incident_type":                     classified.incident_type,
        "affected_systems":                  classified.affected_systems,
        "clients_affected_pct":              classified.clients_affected_pct,
        "financial_impact_eur":              classified.financial_impact_eur,
        "detection_timestamp":               classified.detection_timestamp,
        "containment_timestamp":             classified.containment_timestamp,
        "ict_third_party_provider":          classified.ict_third_party_provider,
        "is_cross_border":                   classified.is_cross_border,
        "dora_severity":                     classified.dora_severity,
        "bafin_notification_required":       classified.bafin_notification_required,
        "bafin_notification_deadline_hours": classified.bafin_notification_deadline_hours,
        "classification_reason":             reason,
    }


def audit_row(classified: IncidentEvent, processed_at: datetime, record_hash: str) -> dict:
    """Build one dora.audit_log row dict for a processed incident.

    Returns the 5 columns of the audit_log Iceberg schema — the lineage record
    that proves which pipeline and classifier version processed each incident
    and pins its content hash.

    Args:
        classified:   A classified IncidentEvent.
        processed_at: The tz-aware UTC timestamp this record was processed.
        record_hash:  The SHA-256 digest from compute_record_hash().

    Returns:
        A dict keyed by audit_log column name, ready for pa.Table.from_pylist().
    """
    return {
        "incident_id":        str(classified.incident_id),
        "processed_at":       processed_at,
        "pipeline_version":   PIPELINE_VERSION,
        "classifier_version": CLASSIFIER_VERSION,
        "record_hash":        record_hash,
    }


def build_enriched_message(
    classified: IncidentEvent,
    reason: str,
    processed_at: datetime,
    record_hash: str,
) -> dict:
    """Build the enriched JSON payload published to dora.incidents.enriched.

    Combines the classified IncidentEvent (via its canonical Kafka dict) with the
    audit metadata so real-time downstream consumers receive both the regulatory
    classification and the lineage fields in a single message.

    Args:
        classified:   A classified IncidentEvent.
        reason:       The get_classification_reason() string.
        processed_at: The tz-aware UTC processing timestamp.
        record_hash:  The SHA-256 digest from compute_record_hash().

    Returns:
        A JSON-serialisable dict (all values are primitives / ISO strings).
    """
    enriched = classified.to_kafka_message()
    enriched["classification_reason"] = reason
    enriched["processed_at"]          = processed_at.isoformat()
    enriched["pipeline_version"]      = PIPELINE_VERSION
    enriched["record_hash"]           = record_hash
    return enriched


# ── Micro-batch processing ───────────────────────────────────────────────────────

def make_batch_processor(classifier, catalog, producer, enriched_topic):
    """Return a foreachBatch handler closed over the shared driver-side resources.

    foreachBatch only passes (dataframe, batch_id), so the classifier, Iceberg
    catalog, Kafka producer, and enriched topic name are captured here in a
    closure. Loading the two Iceberg tables once (rather than per batch) avoids
    repeated SQLite catalog reads; sequential appends on the same table object
    are the single-writer pattern SqlCatalog/SQLite expects.

    Args:
        classifier:     A DORAClassifier instance.
        catalog:        The shared PyIceberg SqlCatalog from _load_catalog().
        producer:       A confluent_kafka.Producer for the enriched topic.
        enriched_topic: Destination topic name for enriched records.

    Returns:
        A function(dataframe, batch_id) suitable for DataStreamWriter.foreachBatch.
    """
    classified_tbl   = catalog.load_table(_TBL_CLASSIFIED)
    audit_tbl        = catalog.load_table(_TBL_AUDIT)
    classified_arrow = classified_tbl.schema().as_arrow()
    audit_arrow      = audit_tbl.schema().as_arrow()

    def process_batch(batch_df, batch_id: int) -> None:
        """Classify one micro-batch and write it to both Iceberg tables and Kafka.

        Steps: collect the batch's Kafka values to the driver, rebuild and
        classify each IncidentEvent, compute audit metadata, append the classified
        and audit rows to Iceberg, publish enriched payloads to Kafka, then log a
        one-line per-batch summary with severity counts. Malformed messages are
        logged and skipped so a single poison record cannot abort the stream.

        Args:
            batch_df:  The micro-batch DataFrame (Kafka source rows).
            batch_id:  The monotonically increasing Spark batch identifier.
        """
        rows = batch_df.selectExpr("CAST(value AS STRING) AS json_str").collect()
        processed_at = datetime.now(timezone.utc)

        classified_rows: list[dict] = []
        audit_rows: list[dict] = []
        enriched_msgs: list[tuple[str, dict]] = []
        severity_counts: Counter = Counter()

        for row in rows:
            try:
                incident = IncidentEvent.from_kafka_message(json.loads(row.json_str))
            except Exception as exc:  # poison message — log and skip, do not crash
                print(f"[batch {batch_id}] [PARSE ERROR] skipped record: {exc}", flush=True)
                continue

            classified  = classifier.classify(incident)
            reason      = classifier.get_classification_reason(incident)
            record_hash = compute_record_hash(classified)

            classified_rows.append(classified_to_iceberg_row(classified, reason))
            audit_rows.append(audit_row(classified, processed_at, record_hash))
            enriched_msgs.append(
                (
                    classified.institution_id,
                    build_enriched_message(classified, reason, processed_at, record_hash),
                )
            )
            severity_counts[classified.dora_severity] += 1

        # ── Sink 1a: Iceberg incidents_classified (append) ──────────────────────
        if classified_rows:
            classified_tbl.append(pa.Table.from_pylist(classified_rows, schema=classified_arrow))
        # ── Sink 1b: Iceberg audit_log (append) ─────────────────────────────────
        if audit_rows:
            audit_tbl.append(pa.Table.from_pylist(audit_rows, schema=audit_arrow))

        # ── Sink 2: Kafka enriched topic ────────────────────────────────────────
        for key, message in enriched_msgs:
            producer.produce(
                topic=enriched_topic,
                key=key.encode("utf-8"),
                value=json.dumps(message).encode("utf-8"),
            )
        producer.flush(timeout=10)

        # ── Per-batch observability log ─────────────────────────────────────────
        print(
            f"[batch {batch_id}] "
            f"records_processed={len(classified_rows)} | "
            f"critical={severity_counts.get('critical', 0)} | "
            f"major={severity_counts.get('major', 0)} | "
            f"minor={severity_counts.get('minor', 0)}",
            flush=True,
        )

    return process_batch


# ── Job wiring & lifecycle ───────────────────────────────────────────────────────

def run_streaming_job(broker: str = _DEFAULT_BROKER) -> None:
    """Start the PySpark Structured Streaming pipeline and run until stopped.

    Ensures the Iceberg tables (and shared SQLite catalog index) exist, builds
    the SparkSession, subscribes to the three severity topics, and registers a
    foreachBatch sink on a 10-second processing-time trigger writing to Iceberg
    and Kafka. Installs SIGTERM/SIGINT handlers for graceful shutdown, then blocks
    on awaitTermination.

    Args:
        broker: Kafka bootstrap server in host:port form (host-side default
                localhost:9092).
    """
    # Guarantee tables + shared catalog index exist before the first append.
    create_all_tables()
    catalog = _load_catalog()

    spark = build_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    producer = Producer({"bootstrap.servers": broker, "acks": "all"})

    source_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", broker)
        .option("subscribe", _source_topics())
        .option("startingOffsets", "latest")
        .load()
    )

    processor = make_batch_processor(
        classifier=DORAClassifier(),
        catalog=catalog,
        producer=producer,
        enriched_topic=_enriched_topic(),
    )

    query = (
        source_df.writeStream
        .foreachBatch(processor)
        .option("checkpointLocation", CHECKPOINT_LOCATION)
        .trigger(processingTime="10 seconds")
        .start()
    )

    _install_signal_handlers(query, producer, spark)

    print(
        f"DORA streaming job started  broker={broker!r}  "
        f"topics=[{_source_topics()}]  ->  Iceberg + {_enriched_topic()}",
        flush=True,
    )
    query.awaitTermination()


def _install_signal_handlers(query, producer, spark) -> None:
    """Install SIGTERM/SIGINT handlers that stop the stream cleanly.

    On signal, stops the streaming query without forcing (lets the in-flight
    micro-batch finish and its offsets commit to the checkpoint), flushes any
    buffered enriched Kafka messages, stops the SparkSession, and exits. This
    lets Docker / Airflow terminate the job without corrupting the checkpoint or
    losing produced messages.

    Args:
        query:    The active StreamingQuery to stop.
        producer: The confluent_kafka.Producer to flush.
        spark:    The SparkSession to stop.
    """
    def _handler(signum, _frame):
        """Stop the query, flush Kafka, and stop Spark, then exit cleanly."""
        print(f"\n[shutdown] signal {signum} received — stopping gracefully ...", flush=True)
        query.stop()
        producer.flush(timeout=10)
        spark.stop()
        print("[shutdown] streaming job stopped.", flush=True)
        sys.exit(0)

    signal.signal(signal.SIGTERM, _handler)
    signal.signal(signal.SIGINT, _handler)


def main() -> None:
    """Parse CLI arguments and launch the streaming job.

    Reads --broker from the command line (default localhost:9092) and delegates
    to run_streaming_job().
    """
    parser = argparse.ArgumentParser(
        description="DORA ICT incident streaming processor — Kafka -> classify -> Iceberg + Kafka."
    )
    parser.add_argument(
        "--broker",
        default=_DEFAULT_BROKER,
        help=f"Kafka bootstrap server (default: {_DEFAULT_BROKER})",
    )
    args = parser.parse_args()
    run_streaming_job(broker=args.broker)


if __name__ == "__main__":
    main()

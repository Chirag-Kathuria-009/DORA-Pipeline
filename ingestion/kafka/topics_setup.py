"""Kafka topic creation script — run once after the broker is healthy.

Creates the four DORA incident topics with partition counts and retention
policies matching the pipeline's throughput and compliance requirements.
Safe to run multiple times: existing topics are detected and skipped.

Usage:
    python ingestion/kafka/topics_setup.py
    python ingestion/kafka/topics_setup.py --broker localhost:9092
"""

import argparse
import time

from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.error import KafkaException

# ms retention values
_7_DAYS_MS  = str(7  * 24 * 60 * 60 * 1000)
_3_DAYS_MS  = str(3  * 24 * 60 * 60 * 1000)
_30_DAYS_MS = str(30 * 24 * 60 * 60 * 1000)

# (topic_name, num_partitions, retention_ms)
TOPIC_SPECS: list[tuple[str, int, str]] = [
    ("dora.incidents.critical", 3, _7_DAYS_MS),
    ("dora.incidents.major",    3, _7_DAYS_MS),
    ("dora.incidents.minor",    2, _3_DAYS_MS),
    ("dora.incidents.enriched", 3, _30_DAYS_MS),
]


def _wait_for_broker(broker: str, timeout_secs: int = 30) -> AdminClient:
    """Poll the Kafka broker until it accepts connections or the timeout expires.

    Args:
        broker: Kafka broker address in host:port format.
        timeout_secs: Maximum seconds to wait before raising an error.

    Returns:
        A connected AdminClient instance.

    Raises:
        RuntimeError: If the broker is not reachable within timeout_secs.
    """
    deadline = time.time() + timeout_secs
    last_exc: Exception | None = None

    while time.time() < deadline:
        try:
            client = AdminClient({"bootstrap.servers": broker, "socket.timeout.ms": 3000})
            # list_topics() with a short timeout proves the broker is alive
            client.list_topics(timeout=3)
            return client
        except KafkaException as exc:
            last_exc = exc
            remaining = int(deadline - time.time())
            print(f"  Kafka not ready yet — retrying ({remaining}s remaining)...")
            time.sleep(3)

    raise RuntimeError(
        f"Kafka broker at {broker!r} did not respond within {timeout_secs}s. "
        f"Last error: {last_exc}"
    )


def _existing_topics(client: AdminClient) -> set[str]:
    """Return the set of topic names that already exist on the broker.

    Args:
        client: An active AdminClient connected to the broker.

    Returns:
        Set of existing topic name strings.
    """
    metadata = client.list_topics(timeout=10)
    return set(metadata.topics.keys())


def create_topics(broker: str = "localhost:9092") -> None:
    """Connect to Kafka and create all DORA pipeline topics if absent.

    Skips topics that already exist so the function is idempotent.
    Waits up to 30 seconds for the broker to become available before
    giving up with a clear error message.

    Args:
        broker: Kafka broker address in host:port format.
    """
    print(f"Connecting to Kafka broker at {broker!r}...")
    client = _wait_for_broker(broker, timeout_secs=30)
    print("  Connected.\n")

    existing = _existing_topics(client)

    to_create = [
        NewTopic(
            topic=name,
            num_partitions=partitions,
            replication_factor=1,
            config={"retention.ms": retention_ms},
        )
        for name, partitions, retention_ms in TOPIC_SPECS
        if name not in existing
    ]

    already_exist = [name for name, _, _ in TOPIC_SPECS if name in existing]
    for name in already_exist:
        print(f"  [SKIP]    {name!r} already exists")

    if not to_create:
        print("\nAll topics already exist — nothing to do.")
        return

    futures = client.create_topics(to_create)

    for topic, future in futures.items():
        try:
            future.result()  # blocks until the broker confirms creation
            spec = next(s for s in TOPIC_SPECS if s[0] == topic)
            retention_days = int(spec[2]) // (24 * 60 * 60 * 1000)
            print(f"  [CREATED] {topic!r}  "
                  f"({spec[1]} partitions, {retention_days}d retention)")
        except KafkaException as exc:
            # TOPIC_ALREADY_EXISTS can race if another process created it
            if "TOPIC_ALREADY_EXISTS" in str(exc):
                print(f"  [SKIP]    {topic!r} already exists (race)")
            else:
                raise

    print("\nTopic setup complete.")


def main() -> None:
    """Parse CLI arguments and run create_topics().

    Reads --broker from the command line, defaulting to localhost:9092.
    """
    parser = argparse.ArgumentParser(
        description="Create DORA Kafka topics if they do not already exist."
    )
    parser.add_argument(
        "--broker",
        default="localhost:9092",
        help="Kafka broker address (default: localhost:9092)",
    )
    args = parser.parse_args()
    create_topics(broker=args.broker)


if __name__ == "__main__":
    main()

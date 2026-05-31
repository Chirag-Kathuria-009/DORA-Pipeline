"""Fake ICT incident event producer for local development and testing.

Generates synthetic IncidentEvent records and publishes them to Kafka
at a configurable rate. Imports IncidentEvent from schema.py — never
defines its own field names.
"""

from ingestion.simulator.schema import IncidentEvent


def main(rate: int = 1) -> None:
    """Run the incident generator loop.

    Produces synthetic IncidentEvent messages to the Kafka topic
    at the given rate (events per second). Runs until interrupted.

    Args:
        rate: Number of incident events to produce per second.
    """
    raise NotImplementedError("Implemented in Phase 2")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="DORA incident event simulator")
    parser.add_argument("--rate", type=int, default=1, help="Events per second")
    args = parser.parse_args()
    main(rate=args.rate)

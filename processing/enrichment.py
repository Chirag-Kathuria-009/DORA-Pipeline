"""Incident enrichment — adds vendor metadata and severity labels.

Augments a classified IncidentEvent with third-party vendor details
and human-readable severity labels before the record is written to
Iceberg storage or forwarded to downstream Kafka topics.
"""

from ingestion.simulator.schema import IncidentEvent


def enrich_event(event: IncidentEvent, severity: str) -> dict:
    """Enrich an incident event with vendor metadata and severity label.

    Looks up vendor information based on the event's third_party_provider
    field and attaches it alongside the severity classification result.

    Args:
        event: A validated IncidentEvent from the Kafka stream.
        severity: Classification result — "CRITICAL", "MAJOR", or "MINOR".

    Returns:
        A dict containing all original event fields plus enrichment keys.
    """
    raise NotImplementedError("Implemented in Phase 4")

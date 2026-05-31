"""DORA threshold rules engine.

Classifies ICT incidents into CRITICAL, MAJOR, or MINOR severity tiers
based on BaFin Article 18 thresholds. Imports IncidentEvent from
ingestion/simulator/schema.py — never re-defines field names here.

Severity rules (from CLAUDE.md):
  CRITICAL: clients_affected_pct >= 25%, OR financial_impact_eur >= 1_000_000,
            OR (cyber_attack AND clients_affected_pct >= 10%),
            OR (cross_border AND clients_affected_pct >= 10%)
  MAJOR:    clients_affected_pct >= 10% (not CRITICAL),
            OR financial_impact_eur >= 100_000 (not CRITICAL),
            OR (third_party_provider set AND incident_type == "system_outage")
  MINOR:    everything else
"""

from ingestion.simulator.schema import IncidentEvent


class DORAClassifier:
    """Applies DORA BaFin Article 18 classification rules to incidents.

    Instantiate once and call classify() for each IncidentEvent.
    Stateless — all logic is derived from the event fields only.
    """

    def classify(self, event: IncidentEvent) -> str:
        """Classify an incident into CRITICAL, MAJOR, or MINOR.

        Evaluates the eight DORA threshold rules in priority order:
        CRITICAL checks first, then MAJOR, then falls back to MINOR.

        Args:
            event: A validated IncidentEvent from the Kafka stream.

        Returns:
            One of the strings: "CRITICAL", "MAJOR", or "MINOR".
        """
        raise NotImplementedError("Implemented in Phase 3")

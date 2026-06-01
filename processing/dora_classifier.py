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

from dataclasses import dataclass

from ingestion.simulator.schema import IncidentEvent


@dataclass(frozen=True)
class DORAThresholds:
    """Configurable BaFin Article 18 threshold values.

    All fields default to the official regulatory values. Pass a custom
    instance to DORAClassifier to override any subset without touching
    classifier logic — useful for testing, staging environments, or when
    BaFin publishes revised RTS values.

    frozen=True prevents accidental mutation after the classifier is built.

    Example — lower the MAJOR client threshold to 5% for a test environment:
        thresholds = DORAThresholds(major_client_pct=5.0)
        classifier = DORAClassifier(thresholds=thresholds)
    """

    critical_client_pct: float = 25.0
    critical_financial_eur: float = 1_000_000.0
    critical_cyber_client_pct: float = 10.0
    critical_cross_border_client_pct: float = 10.0
    major_client_pct: float = 10.0
    major_financial_eur: float = 100_000.0


class DORAClassifier:
    """Applies DORA BaFin Article 18 classification rules to IncidentEvent objects.

    Instantiate once and call classify() or classify_batch() per event.
    Classification logic is fixed; only the numeric thresholds are configurable
    via a DORAThresholds instance passed at construction time.

    Example — default BaFin thresholds:
        classifier = DORAClassifier()

    Example — custom thresholds (e.g. loaded from YAML or an API response):
        classifier = DORAClassifier(thresholds=DORAThresholds(critical_client_pct=20.0))
    """

    def __init__(self, thresholds: DORAThresholds | None = None) -> None:
        """Initialise the classifier with the given thresholds.

        Args:
            thresholds: A DORAThresholds instance. If None, the official
                        BaFin Article 18 default values are used.
        """
        self.thresholds: DORAThresholds = thresholds if thresholds is not None else DORAThresholds()

    # ------------------------------------------------------------------ #
    # Rule methods — one per BaFin threshold, callable on any instance.  #
    # Instance methods (not static) so each rule reads from              #
    # self.thresholds and reflects any custom configuration.             #
    # ------------------------------------------------------------------ #

    def is_critical_by_client_pct(self, incident: IncidentEvent) -> bool:
        """Return True when clients_affected_pct meets the CRITICAL client-share threshold."""
        return incident.clients_affected_pct >= self.thresholds.critical_client_pct

    def is_critical_by_financial_impact(self, incident: IncidentEvent) -> bool:
        """Return True when financial_impact_eur meets the CRITICAL financial threshold."""
        return incident.financial_impact_eur >= self.thresholds.critical_financial_eur

    def is_critical_by_cyber_attack(self, incident: IncidentEvent) -> bool:
        """Return True for a cyber_attack incident that meets the CRITICAL client-share threshold."""
        return (
            incident.incident_type == "cyber_attack"
            and incident.clients_affected_pct >= self.thresholds.critical_cyber_client_pct
        )

    def is_critical_by_cross_border(self, incident: IncidentEvent) -> bool:
        """Return True for a cross-border incident that meets the CRITICAL client-share threshold."""
        return (
            incident.is_cross_border
            and incident.clients_affected_pct >= self.thresholds.critical_cross_border_client_pct
        )

    def is_major_by_client_pct(self, incident: IncidentEvent) -> bool:
        """Return True when clients_affected_pct meets the MAJOR client-share threshold."""
        return incident.clients_affected_pct >= self.thresholds.major_client_pct

    def is_major_by_financial_impact(self, incident: IncidentEvent) -> bool:
        """Return True when financial_impact_eur meets the MAJOR financial threshold."""
        return incident.financial_impact_eur >= self.thresholds.major_financial_eur

    def is_major_by_third_party_outage(self, incident: IncidentEvent) -> bool:
        """Return True when an ICT third-party provider is involved in a system_outage."""
        return (
            incident.ict_third_party_provider is not None
            and incident.incident_type == "system_outage"
        )

    # ------------------------------------------------------------------ #
    # Internal helpers                                                    #
    # ------------------------------------------------------------------ #

    def _determine_severity(self, incident: IncidentEvent) -> str:
        """Evaluate all threshold rules in priority order and return a severity string.

        Checks all four CRITICAL rules first (any match → "critical"), then the
        three MAJOR rules, then falls through to "minor". Shared by classify()
        and get_classification_reason() to avoid duplicate branching logic.

        Args:
            incident: A validated IncidentEvent.

        Returns:
            One of: "critical", "major", "minor".
        """
        if (
            self.is_critical_by_client_pct(incident)
            or self.is_critical_by_financial_impact(incident)
            or self.is_critical_by_cyber_attack(incident)
            or self.is_critical_by_cross_border(incident)
        ):
            return "critical"

        if (
            self.is_major_by_client_pct(incident)
            or self.is_major_by_financial_impact(incident)
            or self.is_major_by_third_party_outage(incident)
        ):
            return "major"

        return "minor"

    # ------------------------------------------------------------------ #
    # Public API                                                          #
    # ------------------------------------------------------------------ #

    def classify(self, incident: IncidentEvent) -> IncidentEvent:
        """Classify an incident and return a new IncidentEvent with DORA fields populated.

        Evaluates BaFin Article 18 threshold rules in priority order
        (CRITICAL → MAJOR → MINOR) and returns a copy of the incident with
        dora_severity, bafin_notification_required, and
        bafin_notification_deadline_hours set. The original incident is never mutated.

        Args:
            incident: A validated IncidentEvent produced by the simulator or Kafka consumer.

        Returns:
            A new IncidentEvent with all three DORA classification fields filled in.
        """
        severity = self._determine_severity(incident)

        if severity == "critical":
            return incident.model_copy(update={
                "dora_severity": "critical",
                "bafin_notification_required": True,
                "bafin_notification_deadline_hours": 4,
            })

        if severity == "major":
            return incident.model_copy(update={
                "dora_severity": "major",
                "bafin_notification_required": True,
                "bafin_notification_deadline_hours": 72,
            })

        return incident.model_copy(update={
            "dora_severity": "minor",
            "bafin_notification_required": False,
            "bafin_notification_deadline_hours": None,
        })

    def classify_batch(self, incidents: list[IncidentEvent]) -> list[IncidentEvent]:
        """Classify a list of incidents, returning a new list with DORA fields set.

        Applies classify() to each incident in order. Input list and its elements
        are never mutated.

        Args:
            incidents: A list of validated IncidentEvent instances.

        Returns:
            A new list of IncidentEvent instances with DORA classification fields populated.
        """
        return [self.classify(incident) for incident in incidents]

    def get_classification_reason(self, incident: IncidentEvent) -> str:
        """Return a human-readable explanation of the incident's severity classification.

        Re-evaluates the BaFin threshold rules in priority order and returns the
        first matching rule as a descriptive string. Does not rely on dora_severity
        already being set on the incident, and reflects the active thresholds
        configured on this classifier instance.

        Args:
            incident: A validated IncidentEvent (classification fields may be None).

        Returns:
            A string such as:
              "CRITICAL: 34.2% of clients affected — exceeds 25% BaFin Article 18 threshold"
              "MAJOR: third-party provider 'Finastra' involved in system_outage"
              "MINOR: no BaFin reporting threshold met — internal logging only"
        """
        t = self.thresholds

        if self.is_critical_by_client_pct(incident):
            return (
                f"CRITICAL: {incident.clients_affected_pct:.1f}% of clients affected"
                f" — exceeds {t.critical_client_pct:.0f}%"
                f" BaFin Article 18 threshold"
            )
        if self.is_critical_by_financial_impact(incident):
            return (
                f"CRITICAL: €{incident.financial_impact_eur:,.0f} financial impact"
                f" — exceeds €{t.critical_financial_eur:,.0f}"
                f" BaFin Article 18 threshold"
            )
        if self.is_critical_by_cyber_attack(incident):
            return (
                f"CRITICAL: cyber_attack with {incident.clients_affected_pct:.1f}%"
                f" clients affected — exceeds {t.critical_cyber_client_pct:.0f}%"
                f" cyber-attack BaFin threshold"
            )
        if self.is_critical_by_cross_border(incident):
            return (
                f"CRITICAL: cross-border incident with {incident.clients_affected_pct:.1f}%"
                f" clients affected — exceeds {t.critical_cross_border_client_pct:.0f}%"
                f" cross-border BaFin threshold"
            )
        if self.is_major_by_client_pct(incident):
            return (
                f"MAJOR: {incident.clients_affected_pct:.1f}% of clients affected"
                f" — exceeds {t.major_client_pct:.0f}%"
                f" BaFin Article 18 threshold"
            )
        if self.is_major_by_financial_impact(incident):
            return (
                f"MAJOR: €{incident.financial_impact_eur:,.0f} financial impact"
                f" — exceeds €{t.major_financial_eur:,.0f}"
                f" BaFin Article 18 threshold"
            )
        if self.is_major_by_third_party_outage(incident):
            return (
                f"MAJOR: third-party provider '{incident.ict_third_party_provider}'"
                f" involved in system_outage"
            )

        return "MINOR: no BaFin reporting threshold met — internal logging only"

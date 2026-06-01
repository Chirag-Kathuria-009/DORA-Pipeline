"""Unit tests for the DORA classification rules engine.

Eight test cases covering every BaFin Article 18 threshold boundary:
CRITICAL (client-share, financial, cyber-attack, cross-border),
MAJOR (third-party outage), MINOR fallback, notification flags,
deadline hours per tier, and get_classification_reason output.
"""

from datetime import datetime, timezone

import pytest

from ingestion.simulator.schema import IncidentEvent
from processing.dora_classifier import DORAClassifier, DORAThresholds


# ------------------------------------------------------------------ #
# Shared helpers                                                      #
# ------------------------------------------------------------------ #

def _make_incident(**overrides) -> IncidentEvent:
    """Build a minimal valid IncidentEvent with safe defaults.

    Every field required by IncidentEvent is provided. Pass keyword
    arguments to override only the fields relevant to a given test.
    """
    defaults: dict = {
        "institution_id": "DE-BANK-001",
        "institution_type": "bank",
        "incident_type": "transaction_failure",
        "affected_systems": ["core-banking"],
        "clients_affected_pct": 1.0,
        "financial_impact_eur": 10_000.0,
        "detection_timestamp": datetime(2026, 6, 1, 10, 0, 0, tzinfo=timezone.utc),
        "is_cross_border": False,
        "ict_third_party_provider": None,
    }
    defaults.update(overrides)
    return IncidentEvent(**defaults)


@pytest.fixture
def classifier() -> DORAClassifier:
    """Return a fresh DORAClassifier shared within each test."""
    return DORAClassifier()


# ------------------------------------------------------------------ #
# Test 1 — CRITICAL: >= 25% clients affected                         #
# ------------------------------------------------------------------ #

def test_classify_critical_by_client_pct(classifier: DORAClassifier) -> None:
    """30% clients affected must produce CRITICAL with a 4-hour BaFin deadline."""
    incident = _make_incident(clients_affected_pct=30.0)

    result = classifier.classify(incident)

    assert result.dora_severity == "critical"
    assert result.bafin_notification_required is True
    assert result.bafin_notification_deadline_hours == 4


# ------------------------------------------------------------------ #
# Test 2 — CRITICAL: financial impact >= €1,000,000                  #
# ------------------------------------------------------------------ #

def test_classify_critical_by_financial_impact(classifier: DORAClassifier) -> None:
    """€1.5M financial impact must produce CRITICAL even with only 5% clients affected."""
    incident = _make_incident(
        clients_affected_pct=5.0,
        financial_impact_eur=1_500_000.0,
    )

    result = classifier.classify(incident)

    assert result.dora_severity == "critical"
    assert result.bafin_notification_required is True
    assert result.bafin_notification_deadline_hours == 4


# ------------------------------------------------------------------ #
# Test 3 — CRITICAL: cross-border cyber_attack with >= 10% clients   #
# ------------------------------------------------------------------ #

def test_classify_critical_cross_border_cyber_attack(classifier: DORAClassifier) -> None:
    """Cross-border cyber_attack with 12% clients must produce CRITICAL.

    Both the cyber-attack rule and the cross-border rule fire; each is
    independently sufficient for CRITICAL. Both rule methods are verified.
    """
    incident = _make_incident(
        incident_type="cyber_attack",
        clients_affected_pct=12.0,
        financial_impact_eur=10_000.0,
        is_cross_border=True,
    )

    result = classifier.classify(incident)

    assert result.dora_severity == "critical"
    assert result.bafin_notification_required is True
    assert result.bafin_notification_deadline_hours == 4
    # Verify both underlying BaFin rules fire independently
    assert classifier.is_critical_by_cyber_attack(incident) is True
    assert classifier.is_critical_by_cross_border(incident) is True


# ------------------------------------------------------------------ #
# Test 4 — MAJOR: third-party provider + system_outage               #
# ------------------------------------------------------------------ #

def test_classify_major_third_party_outage(classifier: DORAClassifier) -> None:
    """AWS system_outage with 15% clients must produce MAJOR with a 72-hour deadline."""
    incident = _make_incident(
        incident_type="system_outage",
        clients_affected_pct=15.0,
        financial_impact_eur=10_000.0,
        is_cross_border=False,
        ict_third_party_provider="AWS",
    )

    result = classifier.classify(incident)

    assert result.dora_severity == "major"
    assert result.bafin_notification_required is True
    assert result.bafin_notification_deadline_hours == 72


# ------------------------------------------------------------------ #
# Test 5 — MINOR: no threshold met                                   #
# ------------------------------------------------------------------ #

def test_classify_minor_no_threshold_met(classifier: DORAClassifier) -> None:
    """Transaction failure with 5% clients and €50k impact must produce MINOR."""
    incident = _make_incident(
        incident_type="transaction_failure",
        clients_affected_pct=5.0,
        financial_impact_eur=50_000.0,
        is_cross_border=False,
        ict_third_party_provider=None,
    )

    result = classifier.classify(incident)

    assert result.dora_severity == "minor"
    assert result.bafin_notification_required is False
    assert result.bafin_notification_deadline_hours is None


# ------------------------------------------------------------------ #
# Test 6 — bafin_notification_required is True for CRITICAL + MAJOR  #
# ------------------------------------------------------------------ #

def test_bafin_notification_required_true_for_critical_and_major(
    classifier: DORAClassifier,
) -> None:
    """bafin_notification_required must be True for both CRITICAL and MAJOR incidents."""
    critical_incident = _make_incident(clients_affected_pct=30.0)
    major_incident = _make_incident(
        clients_affected_pct=15.0,
        financial_impact_eur=10_000.0,
    )

    critical_result = classifier.classify(critical_incident)
    major_result = classifier.classify(major_incident)

    assert critical_result.bafin_notification_required is True
    assert major_result.bafin_notification_required is True


# ------------------------------------------------------------------ #
# Test 7 — Deadline hours: 4 / 72 / None per severity tier           #
# ------------------------------------------------------------------ #

def test_bafin_notification_deadline_hours_per_tier(classifier: DORAClassifier) -> None:
    """CRITICAL must carry a 4-hour deadline, MAJOR 72 hours, MINOR None."""
    critical = classifier.classify(_make_incident(clients_affected_pct=30.0))
    major = classifier.classify(
        _make_incident(clients_affected_pct=15.0, financial_impact_eur=10_000.0)
    )
    minor = classifier.classify(
        _make_incident(clients_affected_pct=5.0, financial_impact_eur=10_000.0)
    )

    assert critical.bafin_notification_deadline_hours == 4
    assert major.bafin_notification_deadline_hours == 72
    assert minor.bafin_notification_deadline_hours is None


# ------------------------------------------------------------------ #
# Test 8 — get_classification_reason: non-empty, correct prefix      #
# ------------------------------------------------------------------ #

@pytest.mark.parametrize(
    "incident,expected_prefix",
    [
        (
            _make_incident(clients_affected_pct=30.0),
            "CRITICAL",
        ),
        (
            _make_incident(clients_affected_pct=5.0, financial_impact_eur=1_500_000.0),
            "CRITICAL",
        ),
        (
            _make_incident(
                incident_type="cyber_attack",
                clients_affected_pct=12.0,
                is_cross_border=True,
            ),
            "CRITICAL",
        ),
        (
            _make_incident(
                incident_type="system_outage",
                clients_affected_pct=15.0,
                ict_third_party_provider="AWS",
            ),
            "MAJOR",
        ),
        (
            _make_incident(clients_affected_pct=5.0, financial_impact_eur=50_000.0),
            "MINOR",
        ),
    ],
)
def test_get_classification_reason_non_empty_and_correct_prefix(
    classifier: DORAClassifier,
    incident: IncidentEvent,
    expected_prefix: str,
) -> None:
    """get_classification_reason must return a non-empty string prefixed with the severity tier."""
    reason = classifier.get_classification_reason(incident)

    assert isinstance(reason, str)
    assert len(reason) > 0
    assert reason.startswith(expected_prefix), (
        f"Expected reason to start with '{expected_prefix}', got: {reason!r}"
    )


# ------------------------------------------------------------------ #
# Test 9 — Custom DORAThresholds override defaults                    #
# ------------------------------------------------------------------ #

def test_custom_thresholds_override_defaults() -> None:
    """Lowering a threshold via DORAThresholds must change classification outcome.

    An incident with 8% clients affected is MINOR under the default 10% MAJOR
    threshold. When the MAJOR threshold is lowered to 5%, the same incident
    must be reclassified as MAJOR — proving thresholds are truly dynamic.
    """
    incident = _make_incident(clients_affected_pct=8.0, financial_impact_eur=10_000.0)

    default_classifier = DORAClassifier()
    custom_classifier = DORAClassifier(thresholds=DORAThresholds(major_client_pct=5.0))

    assert default_classifier.classify(incident).dora_severity == "minor"
    assert custom_classifier.classify(incident).dora_severity == "major"

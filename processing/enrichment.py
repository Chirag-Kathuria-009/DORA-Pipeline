"""Incident enrichment — adds vendor metadata and severity labels.

Augments a classified IncidentEvent with third-party vendor details
and human-readable severity labels before the record is written to
Iceberg storage or forwarded to downstream Kafka topics.

Vendor attributes are looked up from the ict_vendors seed
(transform/dbt_project/seeds/ict_vendors.csv) — the single vendor reference
shared with the dbt stg_ict_vendors model — so there is no second vendor list.
"""

import csv
import functools
import pathlib

from ingestion.simulator.schema import IncidentEvent

# Single vendor reference: the dbt seed. Resolved from the project root so the
# lookup works regardless of the caller's working directory.
_VENDOR_SEED_PATH = (
    pathlib.Path(__file__).resolve().parent.parent
    / "transform" / "dbt_project" / "seeds" / "ict_vendors.csv"
)

# The three public-cloud hyperscalers (matches stg_ict_vendors.is_hyperscaler).
_HYPERSCALERS = {"AWS", "Azure", "GCP"}

# Human-readable severity labels keyed by the classifier's lower-case tiers.
_SEVERITY_LABELS = {
    "critical": "Critical — BaFin notification within 4 hours",
    "major":    "Major — BaFin notification within 72 hours",
    "minor":    "Minor — internal logging only",
}


@functools.lru_cache(maxsize=1)
def _load_vendor_reference() -> dict:
    """Load the ict_vendors seed into a {vendor_name: attributes} lookup.

    Cached so the CSV is read once per process. Returns an empty mapping if the
    seed file is missing, so enrichment degrades gracefully instead of crashing.

    Returns:
        A dict keyed by trimmed vendor_name; each value holds vendor_type (str),
        eu_headquartered (bool), dora_designated_critical (bool) and
        concentration_risk_score (int).
    """
    reference: dict = {}
    if not _VENDOR_SEED_PATH.exists():
        return reference
    with _VENDOR_SEED_PATH.open(newline="", encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            name = row["vendor_name"].strip()
            reference[name] = {
                "vendor_type":              row["vendor_type"].strip(),
                "eu_headquartered":         row["eu_headquartered"].strip().lower() == "true",
                "dora_designated_critical": row["dora_designated_critical"].strip().lower() == "true",
                "concentration_risk_score": int(row["concentration_risk_score"]),
            }
    return reference


def enrich_event(event: IncidentEvent, severity: str) -> dict:
    """Enrich an incident event with vendor metadata and severity label.

    Looks up vendor information based on the event's ict_third_party_provider
    field and attaches it alongside the severity classification result.

    Args:
        event: A validated IncidentEvent from the Kafka stream.
        severity: Classification result — "critical", "major", or "minor"
            (case-insensitive; the upper-case "CRITICAL"/"MAJOR"/"MINOR" forms
            are also accepted).

    Returns:
        A dict containing all original event fields (via to_kafka_message())
        plus enrichment keys: severity_label, vendor_known, and — when the
        provider is found in the vendor reference — vendor_type,
        vendor_eu_headquartered, vendor_dora_designated_critical,
        vendor_concentration_risk_score and vendor_is_hyperscaler.
    """
    enriched = event.to_kafka_message()

    # Human-readable severity label.
    tier = (severity or "").strip().lower()
    enriched["severity_label"] = _SEVERITY_LABELS.get(tier, "Unknown severity")

    # Vendor lookup by third-party provider name.
    provider = event.ict_third_party_provider
    vendor = _load_vendor_reference().get(provider.strip()) if provider else None

    if vendor is not None:
        enriched["vendor_known"] = True
        enriched["vendor_type"] = vendor["vendor_type"]
        enriched["vendor_eu_headquartered"] = vendor["eu_headquartered"]
        enriched["vendor_dora_designated_critical"] = vendor["dora_designated_critical"]
        enriched["vendor_concentration_risk_score"] = vendor["concentration_risk_score"]
        enriched["vendor_is_hyperscaler"] = provider.strip() in _HYPERSCALERS
    else:
        # No provider set, or provider absent from the seed (e.g. "Google Cloud",
        # "Murex" — see the known generator↔seed name mismatch).
        enriched["vendor_known"] = False

    return enriched

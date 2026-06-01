"""Synthetic ICT incident event producer for local development and load testing.

Generates realistic IncidentEvent records for 20 fictional German financial
institutions and publishes them to the appropriate DORA Kafka topic at a
configurable rate. Severity is pre-assigned by the generator (~80 % minor,
~15 % major, ~5 % critical) and impact metrics are constrained so that
DORAClassifier (Phase 3) will agree with the pre-assigned tier.

Usage:
    python ingestion/simulator/incident_generator.py
    python ingestion/simulator/incident_generator.py --rate 5 --broker localhost:9092
"""

import argparse
import json
import random
import time
from datetime import datetime, timedelta, timezone
from typing import Optional

from confluent_kafka import Producer
from faker import Faker

from ingestion.simulator.schema import IncidentEvent

_faker = Faker("de_DE")

# ---------------------------------------------------------------------------
# Reference data
# ---------------------------------------------------------------------------

# (institution_id, institution_type)
INSTITUTIONS: list[tuple[str, str]] = [
    ("BANK_DE_001", "bank"),              # Deutsche Retail Bank AG
    ("BANK_DE_002", "bank"),              # Bayerische Volksbank AG
    ("BANK_DE_003", "bank"),              # Rheinische Sparkasse eG
    ("BANK_DE_004", "bank"),              # Nord Investitionsbank AG
    ("BANK_DE_005", "bank"),              # Frankfurter Hypothekenbank AG
    ("BANK_DE_006", "bank"),              # Berliner Handelsbank GmbH
    ("INS_DE_001",  "insurer"),           # Allianz Versicherungs-AG
    ("INS_DE_002",  "insurer"),           # Münchener Rückversicherungs-AG
    ("INS_DE_003",  "insurer"),           # Hannoversche Lebensversicherung AG
    ("INS_DE_004",  "insurer"),           # Dresdner Sachversicherung GmbH
    ("INS_DE_005",  "insurer"),           # Württembergische Krankenversicherung AG
    ("PAY_DE_001",  "payment_provider"),  # PayDe GmbH
    ("PAY_DE_002",  "payment_provider"),  # TransferNord AG
    ("PAY_DE_003",  "payment_provider"),  # EuroZahlung GmbH
    ("PAY_DE_004",  "payment_provider"),  # FinTrans Deutschland AG
    ("PAY_DE_005",  "payment_provider"),  # Sofortüberweisung Holding GmbH
    ("AM_DE_001",   "asset_manager"),     # Deutsche Asset Management GmbH
    ("AM_DE_002",   "asset_manager"),     # Frankfurter Fondsgesellschaft AG
    ("AM_DE_003",   "asset_manager"),     # Bayern Kapital GmbH
    ("AM_DE_004",   "asset_manager"),     # Rhein-Main Vermögensverwaltung AG
]

ALL_SYSTEMS: list[str] = [
    "core_banking", "payment_gateway", "trading_platform", "risk_engine",
    "customer_portal", "mobile_app", "authentication_service", "data_warehouse",
    "regulatory_reporting", "clearing_system", "card_processing", "iban_validator",
    "api_gateway", "identity_provider", "fraud_detection",
]

THIRD_PARTY_PROVIDERS: list[str] = [
    "AWS", "Azure", "Google Cloud", "SAP", "Salesforce",
    "IBM", "Oracle", "Finastra", "Temenos", "Murex",
]

TOPIC_MAP: dict[str, str] = {
    "critical": "dora.incidents.critical",
    "major":    "dora.incidents.major",
    "minor":    "dora.incidents.minor",
}

# 80 % minor, 15 % major, 5 % critical
_SEVERITIES  = ["minor", "major", "critical"]
_SEV_WEIGHTS = [0.80,    0.15,    0.05]

_NON_CYBER_TYPES = [
    "system_outage", "data_breach", "third_party_failure",
    "transaction_failure", "authentication_failure",
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _detection_time() -> datetime:
    """Return a random UTC datetime within the last 24 hours."""
    return _faker.date_time_between(start_date="-24h", end_date="now", tzinfo=timezone.utc)


def _containment_time(detection: datetime, severity: str) -> Optional[datetime]:
    """Return a containment timestamp after detection, or None if the incident is ongoing.

    Containment probability is higher for minor incidents and lower for critical
    ones, reflecting real-world mean-time-to-resolve distributions.

    Args:
        detection: The detection timestamp to anchor the containment time.
        severity: Pre-assigned DORA severity tier.

    Returns:
        A datetime after detection, or None if the incident is uncontained.
    """
    contained_prob = {"minor": 0.85, "major": 0.50, "critical": 0.20}[severity]
    if random.random() > contained_prob:
        return None
    max_hours = {"minor": 2.0, "major": 8.0, "critical": 24.0}[severity]
    return detection + timedelta(hours=random.uniform(0.1, max_hours))


# ---------------------------------------------------------------------------
# Event generation — impact ranges are constrained per severity tier so that
# DORAClassifier (Phase 3) will agree with dora_severity set here.
#
# CRITICAL thresholds (BaFin Article 18 / CLAUDE.md):
#   pct >= 25 %
#   eur >= 1,000,000
#   cyber_attack AND pct >= 10 %
#   cross_border  AND pct >= 10 %
#
# MAJOR thresholds (not already CRITICAL):
#   pct in [10 %, 25 %)           AND is_cross_border=False AND not cyber_attack
#   eur in [100k, 1M)             AND pct < 10 %
#   third_party set + system_outage AND pct < 10 %, eur < 100k
# ---------------------------------------------------------------------------

def _random_event() -> IncidentEvent:
    """Build one IncidentEvent with a pre-assigned severity and consistent impact metrics.

    Severity is chosen by weighted random draw (80 % minor, 15 % major, 5 % critical).
    Each severity branch constrains clients_affected_pct, financial_impact_eur,
    incident_type, and ict_third_party_provider so the generated metrics place the
    event in exactly the pre-assigned DORA tier — preventing classifier disagreement
    when DORAClassifier runs in Phase 3.

    Returns:
        A fully populated IncidentEvent ready for Kafka serialisation.
    """
    severity = random.choices(_SEVERITIES, weights=_SEV_WEIGHTS, k=1)[0]
    inst_id, inst_type = random.choice(INSTITUTIONS)

    if severity == "critical":
        # Pick one critical trigger; generate metrics that satisfy it
        trigger = random.choice(["high_pct", "high_eur", "cyber", "cross_border"])

        if trigger == "high_pct":
            pct           = round(random.uniform(25.0, 80.0), 2)
            eur           = round(random.uniform(1_000, 800_000), 2)
            is_cross_border = random.random() < 0.35
            incident_type = random.choice(_NON_CYBER_TYPES)
        elif trigger == "high_eur":
            pct           = round(random.uniform(0.5, 15.0), 2)
            eur           = round(random.uniform(1_000_000, 10_000_000), 2)
            is_cross_border = random.random() < 0.35
            incident_type = random.choice(_NON_CYBER_TYPES)
        elif trigger == "cyber":
            # cyber_attack + pct >= 10 % → critical
            pct           = round(random.uniform(10.0, 45.0), 2)
            eur           = round(random.uniform(50_000, 600_000), 2)
            is_cross_border = random.random() < 0.40
            incident_type = "cyber_attack"
        else:  # cross_border + pct >= 10 % → critical
            pct           = round(random.uniform(10.0, 35.0), 2)
            eur           = round(random.uniform(20_000, 400_000), 2)
            is_cross_border = True
            incident_type = random.choice(_NON_CYBER_TYPES)

        provider    = random.choice(THIRD_PARTY_PROVIDERS) if random.random() < 0.50 else None
        bafin_hours = 4

    elif severity == "major":
        trigger = random.choice(["pct", "eur", "third_party"])

        if trigger == "pct":
            # pct in [10, 25) — keep is_cross_border=False and non-cyber to avoid critical
            pct           = round(random.uniform(10.0, 24.9), 2)
            eur           = round(random.uniform(500, 95_000), 2)
            is_cross_border = False
            incident_type = random.choice(_NON_CYBER_TYPES)
            provider      = random.choice(THIRD_PARTY_PROVIDERS) if random.random() < 0.25 else None
        elif trigger == "eur":
            # eur in [100k, 1M) with pct < 10 %
            pct           = round(random.uniform(0.1, 9.9), 2)
            eur           = round(random.uniform(100_000, 999_000), 2)
            is_cross_border = False
            incident_type = random.choice(_NON_CYBER_TYPES)
            provider      = random.choice(THIRD_PARTY_PROVIDERS) if random.random() < 0.25 else None
        else:  # third_party + system_outage trigger (both required by classifier)
            pct           = round(random.uniform(0.1, 9.9), 2)
            eur           = round(random.uniform(500, 95_000), 2)
            is_cross_border = False
            incident_type = "system_outage"
            provider      = random.choice(THIRD_PARTY_PROVIDERS)

        bafin_hours = 72

    else:  # minor — stays below every threshold
        pct           = round(random.uniform(0.0, 9.9), 2)
        eur           = round(random.uniform(100, 95_000), 2)
        # pct < 10 % so cross_border trigger cannot fire even if True
        is_cross_border = random.random() < 0.05
        incident_type = random.choice(_NON_CYBER_TYPES)
        # Avoid third_party + system_outage which would fire the MAJOR trigger
        provider = (
            random.choice(THIRD_PARTY_PROVIDERS)
            if incident_type != "system_outage" and random.random() < 0.10
            else None
        )
        bafin_hours = None

    detection   = _detection_time()
    containment = _containment_time(detection, severity)
    systems     = random.sample(ALL_SYSTEMS, k=random.randint(1, 4))

    return IncidentEvent(
        institution_id=inst_id,
        institution_type=inst_type,
        incident_type=incident_type,
        affected_systems=systems,
        clients_affected_pct=pct,
        financial_impact_eur=eur,
        detection_timestamp=detection,
        containment_timestamp=containment,
        ict_third_party_provider=provider,
        is_cross_border=is_cross_border,
        dora_severity=severity,
        bafin_notification_required=(severity != "minor"),
        bafin_notification_deadline_hours=bafin_hours,
    )


# ---------------------------------------------------------------------------
# Kafka delivery
# ---------------------------------------------------------------------------

def _delivery_callback(err, msg) -> None:
    """Log Kafka delivery failures; successes are silent to keep stdout clean.

    Called asynchronously by confluent-kafka after the broker acknowledges
    (or rejects) each produced message.

    Args:
        err: KafkaError describing the failure, or None on success.
        msg: The Message object that was produced (or failed to be produced).
    """
    if err is not None:
        print(
            f"[DELIVERY ERROR] topic={msg.topic()} "
            f"partition={msg.partition()} error={err}",
            flush=True,
        )


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

def run(broker: str = "localhost:9092", rate: float = 2.0) -> None:
    """Produce synthetic DORA incident events to Kafka at the given rate.

    Runs indefinitely until interrupted with Ctrl-C. Each event is routed to
    the Kafka topic that matches its pre-assigned DORA severity tier.

    Args:
        broker: Kafka broker address in host:port format.
        rate: Events to produce per second. Fractional values are supported
              (e.g. 0.5 = one event every 2 seconds).
    """
    producer   = Producer({"bootstrap.servers": broker, "acks": "all"})
    sleep_secs = 1.0 / max(rate, 0.001)

    print(f"DORA incident simulator starting  broker={broker!r}  rate={rate}/s")
    print("-" * 72)

    try:
        while True:
            event   = _random_event()
            topic   = TOPIC_MAP[event.dora_severity]
            payload = json.dumps(event.to_kafka_message()).encode("utf-8")

            producer.produce(
                topic=topic,
                key=event.institution_id.encode("utf-8"),
                value=payload,
                on_delivery=_delivery_callback,
            )
            producer.poll(0)  # trigger delivery callbacks without blocking

            ts = event.timestamp.strftime("%Y-%m-%dT%H:%M:%SZ")
            print(
                f"{ts} | {event.institution_id:<12} | "
                f"{event.incident_type:<26} | {event.dora_severity.upper()}",
                flush=True,
            )

            time.sleep(sleep_secs)

    except KeyboardInterrupt:
        print("\nFlushing remaining messages ...", flush=True)
    finally:
        producer.flush(timeout=10)
        print("Simulator stopped.", flush=True)


def main() -> None:
    """Parse CLI arguments and start the incident producer loop.

    Reads --broker and --rate from the command line, then delegates to run().
    """
    parser = argparse.ArgumentParser(
        description="DORA ICT incident simulator — publishes synthetic events to Kafka."
    )
    parser.add_argument(
        "--broker",
        default="localhost:9092",
        help="Kafka broker address (default: localhost:9092)",
    )
    parser.add_argument(
        "--rate",
        type=float,
        default=2.0,
        help="Events per second (default: 2.0 — one event every 0.5 s)",
    )
    args = parser.parse_args()
    run(broker=args.broker, rate=args.rate)


if __name__ == "__main__":
    main()

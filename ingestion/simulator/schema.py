"""Pydantic v2 schema for DORA ICT incident events.

IncidentEvent is the single source of truth for field names and types.
All other modules that process incidents must import from this file.
"""

from datetime import datetime, timezone
from typing import Literal, Optional
from uuid import UUID, uuid4

from pydantic import BaseModel, Field


class IncidentEvent(BaseModel):
    """Canonical schema for a single ICT operational incident flowing through the pipeline."""

    # --- Identity & timing ---
    incident_id: UUID = Field(default_factory=uuid4)
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    # --- Institution ---
    institution_id: str
    institution_type: Literal["bank", "insurer", "payment_provider", "asset_manager"]

    # --- Incident classification ---
    incident_type: Literal[
        "system_outage",
        "data_breach",
        "third_party_failure",
        "cyber_attack",
        "transaction_failure",
        "authentication_failure",
    ]
    affected_systems: list[str]

    # --- Impact metrics ---
    clients_affected_pct: float  # 0.0 – 100.0
    financial_impact_eur: float  # estimated loss in EUR

    # --- Timeline ---
    detection_timestamp: datetime
    containment_timestamp: Optional[datetime] = None

    # --- Context ---
    ict_third_party_provider: Optional[str] = None
    is_cross_border: bool

    # --- DORA classification (filled by DORAClassifier, not the simulator) ---
    dora_severity: Optional[Literal["critical", "major", "minor"]] = None
    bafin_notification_required: Optional[bool] = None
    bafin_notification_deadline_hours: Optional[int] = None

    def to_kafka_message(self) -> dict:
        """Serialise the event to a JSON-serialisable dict for Kafka production.

        UUIDs and datetimes are converted to strings so the result can be
        passed directly to json.dumps() without a custom encoder.

        Returns:
            A dict with all fields converted to JSON-safe primitives.
        """
        raw = self.model_dump()
        raw["incident_id"] = str(raw["incident_id"])
        for key in ("timestamp", "detection_timestamp", "containment_timestamp"):
            if raw[key] is not None:
                raw[key] = raw[key].isoformat()
        return raw

    @classmethod
    def from_kafka_message(cls, data: dict) -> "IncidentEvent":
        """Deserialise a dict (from Kafka) back into an IncidentEvent instance.

        Accepts the dict produced by to_kafka_message() and reconstructs
        the model, letting Pydantic coerce ISO-8601 strings to datetime and
        string UUIDs back to UUID objects.

        Args:
            data: A dict previously produced by to_kafka_message().

        Returns:
            A fully validated IncidentEvent instance.
        """
        return cls.model_validate(data)

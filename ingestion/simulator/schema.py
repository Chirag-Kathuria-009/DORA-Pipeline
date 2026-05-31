"""Pydantic schema for DORA ICT incident events.

IncidentEvent is the single source of truth for field names and types.
All other modules that process incidents must import from this file.
Fields are defined in Phase 2 — this is a placeholder stub.
"""

from pydantic import BaseModel


class IncidentEvent(BaseModel):
    """Represents a single ICT operational incident event.

    This model is the canonical schema for all incident data flowing
    through the pipeline — from Kafka ingestion to Iceberg storage.
    Field definitions are added in Phase 2, Task 2.1.
    """

    pass

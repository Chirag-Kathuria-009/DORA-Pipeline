"""Superset runtime configuration — loaded via SUPERSET_CONFIG_PATH.

This file serves two purposes:
  1. Module-level variables are read by Superset's Flask app on startup
     (database URI, secret key, feature flags).
  2. bootstrap_superset() is called in Phase 7 to register dbt mart
     datasets and create the DORA compliance dashboards via the REST API.
"""

import os


# ── Flask / SQLAlchemy ────────────────────────────────────────────────────────
# Superset stores its own metadata (charts, dashboards, users) in SQLite.
# SQLite is sufficient for local dev; apache/superset:latest does not bundle
# psycopg2, so PostgreSQL would require a custom image (see decisions.md).
# The file is written to the named volume `superset-data` for persistence.
SQLALCHEMY_DATABASE_URI = "sqlite:////app/superset_home/superset.db"

SECRET_KEY = os.environ["SUPERSET_SECRET_KEY"]

# Disable CSRF for local development — enable for any internet-facing deployment
WTF_CSRF_ENABLED = False

FEATURE_FLAGS = {
    "ENABLE_TEMPLATE_PROCESSING": True,
}


# ── Phase 7: Dashboard bootstrap ──────────────────────────────────────────────

def bootstrap_superset(superset_url: str = "http://localhost:8088") -> None:
    """Connect Superset to PostgreSQL dbt marts and create DORA dashboards.

    Authenticates against the Superset REST API, registers the `dora`
    PostgreSQL database as a data source, creates datasets from the three
    mart models (mart_bafin_report, mart_vendor_risk, mart_sla_breach),
    and imports pre-built dashboard definitions.

    Args:
        superset_url: Base URL of the running Superset instance.
    """
    raise NotImplementedError("Implemented in Phase 7")


if __name__ == "__main__":
    bootstrap_superset()

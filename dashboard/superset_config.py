"""Superset runtime configuration — loaded via SUPERSET_CONFIG_PATH.

This file serves two purposes:
  1. Module-level variables are read by Superset's Flask app on startup
     (database URI, secret key, feature flags).
  2. bootstrap_superset() is called in Phase 7 to register dbt mart
     datasets and create the DORA compliance dashboards via the REST API.
"""

import json
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
#
# Everything below runs ONLY when bootstrap_superset() is called (CLI / __main__).
# It is never executed when Superset imports this file as its runtime config, so it
# has no effect on Superset startup. `requests` is imported lazily for the same reason.
# Targets Superset 6.x ECharts viz types and references only real mart columns.

DB_CONNECTION_NAME = "DORA Pipeline DB"
DASHBOARD_TITLE = "DORA Regulatory Compliance Dashboard"
MART_TABLES = ("mart_bafin_report", "mart_vendor_risk", "mart_sla_breach")


def _pg_sqlalchemy_uri() -> str:
    """Build the in-network PostgreSQL URI Superset uses to reach the dbt marts.

    Host/port are the Docker-internal values (postgres:5432) because the Superset
    server — not this script — opens the connection. Credentials come from env.

    Returns:
        A postgresql+psycopg2 SQLAlchemy URI string.
    """
    user = os.environ.get("POSTGRES_USER", "dora")
    password = os.environ.get("POSTGRES_PASSWORD", "dora")
    db = os.environ.get("POSTGRES_DB", "dora")
    return f"postgresql+psycopg2://{user}:{password}@postgres:5432/{db}"


def _authenticate(session, base_url: str) -> None:
    """Attach an auth (and CSRF) header to the session for Superset API calls.

    Uses SUPERSET_API_TOKEN as a bearer token if set; otherwise logs in with
    SUPERSET_ADMIN_USER / SUPERSET_ADMIN_PASSWORD to obtain an access token.

    Args:
        session: A requests.Session to mutate in place.
        base_url: Superset base URL (e.g. http://localhost:8088).
    """
    token = os.environ.get("SUPERSET_API_TOKEN")
    if not token:
        resp = session.post(
            f"{base_url}/api/v1/security/login",
            json={
                "username": os.environ.get("SUPERSET_ADMIN_USER", "admin"),
                "password": os.environ.get("SUPERSET_ADMIN_PASSWORD", "admin"),
                "provider": "db",
                "refresh": True,
            },
        )
        resp.raise_for_status()
        token = resp.json()["access_token"]
    session.headers.update({"Authorization": f"Bearer {token}"})
    # CSRF token: harmless if WTF_CSRF is disabled (as above), required if enabled.
    try:
        r = session.get(f"{base_url}/api/v1/security/csrf_token/")
        csrf = r.json().get("result")
        if csrf:
            session.headers.update({"X-CSRFToken": csrf, "Referer": base_url})
    except Exception:  # noqa: BLE001 — CSRF fetch is best-effort
        pass


def _find_id(session, base_url: str, resource: str, name_key: str, name: str):
    """Return the id of an existing Superset object with the given name, or None.

    Fetches the first page of the resource list and matches name_key client-side
    (avoids Rison-encoding a server-side filter). Makes creation idempotent.

    Args:
        session: Authenticated requests.Session.
        base_url: Superset base URL.
        resource: API resource segment (e.g. "database", "dataset", "chart").
        name_key: Field to match on (e.g. "database_name", "table_name").
        name: Value to match.

    Returns:
        The integer id if found, else None.
    """
    resp = session.get(f"{base_url}/api/v1/{resource}/?q=(page_size:100)")
    resp.raise_for_status()
    for item in resp.json().get("result", []):
        if item.get(name_key) == name:
            return item["id"]
    return None


def _create(session, base_url: str, resource: str, payload: dict, name_key: str, name: str) -> int:
    """Create a Superset object idempotently and return its id.

    If an object with the same name already exists it is reused (logged as a skip);
    otherwise it is created via POST.

    Args:
        session: Authenticated requests.Session.
        base_url: Superset base URL.
        resource: API resource segment.
        payload: JSON body for the POST.
        name_key: Field used to detect an existing object.
        name: Name value for the existence check + logging.

    Returns:
        The id of the existing or newly created object.
    """
    existing = _find_id(session, base_url, resource, name_key, name)
    if existing is not None:
        print(f"  [SKIP]    {resource}: '{name}' already exists (id={existing})")
        return existing
    resp = session.post(f"{base_url}/api/v1/{resource}/", json=payload)
    if not resp.ok:
        raise RuntimeError(f"create {resource} '{name}' failed: {resp.status_code} {resp.text}")
    new_id = resp.json()["id"]
    print(f"  [CREATED] {resource}: '{name}' (id={new_id})")
    return new_id


def _metric(aggregate: str, column_name: str, label: str | None = None) -> dict:
    """Build a Superset SIMPLE adhoc metric dict.

    Args:
        aggregate: SQL aggregate (e.g. "AVG", "SUM").
        column_name: Column to aggregate.
        label: Optional display label (defaults to AGG(column)).

    Returns:
        An adhoc-metric dict accepted by Superset chart params.
    """
    return {
        "expressionType": "SIMPLE",
        "column": {"column_name": column_name},
        "aggregate": aggregate,
        "label": label or f"{aggregate}({column_name})",
    }


def _sql_metric(expression: str, label: str) -> dict:
    """Build a Superset SQL adhoc metric dict (e.g. COUNT(*)).

    Args:
        expression: Raw SQL aggregate expression.
        label: Display label.

    Returns:
        An adhoc-metric dict accepted by Superset chart params.
    """
    return {"expressionType": "SQL", "sqlExpression": expression, "label": label}


def _chart_definitions(datasets: dict) -> list:
    """Return the four DORA chart specs (slice_name, viz_type, dataset id, params).

    Args:
        datasets: Mapping of mart table name -> dataset id.

    Returns:
        A list of dicts, each with keys: slice_name, viz_type, dataset_id, params.
    """
    bafin = datasets["mart_bafin_report"]
    vendor = datasets["mart_vendor_risk"]
    sla = datasets["mart_sla_breach"]

    return [
        {
            # a. bar — x: institution_id, y: avg compliance_rate_pct, series: compliance_status
            "slice_name": "BaFin Compliance Rate by Institution",
            "viz_type": "echarts_timeseries_bar",
            "dataset_id": bafin,
            "params": {
                "datasource": f"{bafin}__table",
                "viz_type": "echarts_timeseries_bar",
                "x_axis": "institution_id",
                "metrics": [_metric("AVG", "compliance_rate_pct")],
                "groupby": ["compliance_status"],
                "row_limit": 10000,
            },
        },
        {
            # b. treemap — size: institutions_exposed, color/grouping: concentration_risk_tier
            "slice_name": "ICT Vendor Concentration Risk",
            "viz_type": "treemap_v2",
            "dataset_id": vendor,
            "params": {
                "datasource": f"{vendor}__table",
                "viz_type": "treemap_v2",
                "metric": _metric("SUM", "institutions_exposed"),
                "groupby": ["concentration_risk_tier", "ict_third_party_provider"],
                "row_limit": 1000,
            },
        },
        {
            # c. line — x: detection_timestamp (daily), y: count of sla_breached = true
            "slice_name": "SLA Breach Timeline",
            "viz_type": "echarts_timeseries_line",
            "dataset_id": sla,
            "params": {
                "datasource": f"{sla}__table",
                "viz_type": "echarts_timeseries_line",
                "x_axis": "detection_timestamp",
                "time_grain_sqla": "P1D",
                "metrics": [_sql_metric("COUNT(*)", "breach_count")],
                "adhoc_filters": [
                    {"expressionType": "SQL", "sqlExpression": "sla_breached = true", "clause": "WHERE"}
                ],
                "row_limit": 10000,
            },
        },
        {
            # d. stacked bar — x: reporting_period, y: incident counts, series: severity tier
            "slice_name": "Incident Volume by Severity",
            "viz_type": "echarts_timeseries_bar",
            "dataset_id": bafin,
            "params": {
                "datasource": f"{bafin}__table",
                "viz_type": "echarts_timeseries_bar",
                "x_axis": "reporting_period",
                "metrics": [
                    _metric("SUM", "incident_count_critical"),
                    _metric("SUM", "incident_count_major"),
                    _metric("SUM", "incident_count_minor"),
                ],
                "stack": "Stack",
                "row_limit": 10000,
            },
        },
    ]


def _dashboard_position(charts: list) -> dict:
    """Build a 2x2 dashboard position_json laying out the four charts.

    Args:
        charts: List of (chart_id, slice_name) tuples in display order.

    Returns:
        A position_json dict for PUT /api/v1/dashboard/{id}.
    """
    position = {
        "DASHBOARD_VERSION_KEY": "v2",
        "ROOT_ID": {"type": "ROOT", "id": "ROOT_ID", "children": ["GRID_ID"]},
        "GRID_ID": {"type": "GRID", "id": "GRID_ID", "children": [], "parents": ["ROOT_ID"]},
        "HEADER_ID": {"type": "HEADER", "id": "HEADER_ID", "meta": {"text": DASHBOARD_TITLE}},
    }
    # two charts per row, each half of the 12-column grid (width 6)
    for row_idx in range(0, len(charts), 2):
        row_id = f"ROW-{row_idx // 2}"
        row_children = []
        for chart_id, slice_name in charts[row_idx:row_idx + 2]:
            comp_id = f"CHART-{chart_id}"
            position[comp_id] = {
                "type": "CHART", "id": comp_id, "children": [],
                "meta": {"chartId": chart_id, "width": 6, "height": 50, "sliceName": slice_name},
                "parents": ["ROOT_ID", "GRID_ID", row_id],
            }
            row_children.append(comp_id)
        position[row_id] = {
            "type": "ROW", "id": row_id, "children": row_children,
            "meta": {"background": "BACKGROUND_TRANSPARENT"},
            "parents": ["ROOT_ID", "GRID_ID"],
        }
        position["GRID_ID"]["children"].append(row_id)
    return position


def _dashboard_metadata(charts: list) -> dict:
    """Build a minimal valid dashboard json_metadata.

    A dashboard created via the API has json_metadata=null by default; Superset's
    frontend then does JSON.parse(null) and reads properties off it, crashing the
    dashboard render ("failing to load"). Providing a minimal object fixes that.

    Args:
        charts: List of (chart_id, slice_name) tuples on the dashboard.

    Returns:
        A json_metadata dict for PUT /api/v1/dashboard/{id}.
    """
    chart_ids = [chart_id for chart_id, _ in charts]
    return {
        "color_scheme": "supersetColors",
        "cross_filters_enabled": True,
        "chart_configuration": {},
        "global_chart_configuration": {
            "scope": {"rootPath": ["ROOT_ID"], "excluded": []},
            "chartsInScope": chart_ids,
        },
        "default_filters": "{}",
        "filter_scopes": {},
        "expanded_slices": {},
        "refresh_frequency": 0,
        "timed_refresh_immune_slices": [],
        "label_colors": {},
        "shared_label_colors": {},
        "native_filter_configuration": [],
    }


def bootstrap_superset(superset_url: str = "http://localhost:8088") -> None:
    """Connect Superset to the PostgreSQL dbt marts and build the DORA dashboard.

    Idempotent: re-running reuses any objects that already exist. Creates the
    "DORA Pipeline DB" database connection, datasets for the three mart tables,
    four charts, and the "DORA Regulatory Compliance Dashboard" containing them,
    then prints the URL of each chart and of the final dashboard.

    Auth uses SUPERSET_API_TOKEN if set, else SUPERSET_ADMIN_USER/PASSWORD.

    Args:
        superset_url: Base URL of the running Superset instance.
    """
    import requests

    base_url = superset_url.rstrip("/")
    session = requests.Session()
    _authenticate(session, base_url)

    print("DORA Superset bootstrap")
    print("=" * 50)

    # 1. PostgreSQL database connection
    db_id = _create(
        session, base_url, "database",
        {"database_name": DB_CONNECTION_NAME, "sqlalchemy_uri": _pg_sqlalchemy_uri()},
        "database_name", DB_CONNECTION_NAME,
    )

    # 2. Datasets — one per mart table
    datasets = {}
    for table in MART_TABLES:
        datasets[table] = _create(
            session, base_url, "dataset",
            {"database": db_id, "schema": "public", "table_name": table},
            "table_name", table,
        )

    # 3. Dashboard (created first so charts can be linked to it on creation)
    dashboard_id = _create(
        session, base_url, "dashboard",
        {"dashboard_title": DASHBOARD_TITLE, "published": True},
        "dashboard_title", DASHBOARD_TITLE,
    )

    # 4. Charts
    created = []
    for spec in _chart_definitions(datasets):
        chart_id = _create(
            session, base_url, "chart",
            {
                "slice_name": spec["slice_name"],
                "viz_type": spec["viz_type"],
                "datasource_id": spec["dataset_id"],
                "datasource_type": "table",
                "params": json.dumps(spec["params"]),
                "dashboards": [dashboard_id],
            },
            "slice_name", spec["slice_name"],
        )
        created.append((chart_id, spec["slice_name"]))

    # 5. Lay the four charts out on the dashboard (2x2 grid) + set json_metadata
    #    (a null json_metadata makes the Superset frontend crash on render).
    resp = session.put(
        f"{base_url}/api/v1/dashboard/{dashboard_id}",
        json={
            "position_json": json.dumps(_dashboard_position(created)),
            "json_metadata": json.dumps(_dashboard_metadata(created)),
        },
    )
    if not resp.ok:
        raise RuntimeError(f"dashboard layout update failed: {resp.status_code} {resp.text}")

    print("-" * 50)
    for chart_id, name in created:
        print(f"  chart:     {base_url}/explore/?slice_id={chart_id}   ({name})")
    print(f"  dashboard: {base_url}/superset/dashboard/{dashboard_id}/")
    print("Bootstrap complete.")


if __name__ == "__main__":
    bootstrap_superset(os.environ.get("SUPERSET_URL", "http://localhost:8088"))

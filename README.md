# DORA ICT Incident Intelligence Pipeline

> A production-grade, end-to-end data engineering pipeline that ingests, classifies,
> and reports ICT operational incidents in real time — built to the compliance
> requirements of the EU Digital Operational Resilience Act (DORA), Article 18.

---

## What is DORA?

The **Digital Operational Resilience Act (DORA)** is an EU regulation (effective January 2025)
that requires financial institutions operating in Germany and the EU to detect, classify,
and report ICT (Information and Communication Technology) incidents to their national regulator
(BaFin in Germany) within strict time windows.

Three severity tiers, each with a reporting deadline:

| Severity | Trigger condition | BaFin deadline |
|---|---|---|
| **CRITICAL** | ≥25% clients affected, or ≥€1M financial impact, or cyber attack with ≥10% clients | **4 hours** |
| **MAJOR** | ≥10% clients affected, or ≥€100K impact, or third-party outage | **72 hours** |
| **MINOR** | Everything else | Internal log only |

Missing these deadlines is a regulatory violation. This pipeline automates the detection,
classification, and reporting so that no incident slips through.

---

## What This Project Builds

A **fully local, containerised data pipeline** that simulates the ICT incident lifecycle
from raw event generation through to a compliance dashboard — using the same tools and
patterns you would use in a production financial institution.

### Data Flow

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        DORA Pipeline — Data Flow                        │
└─────────────────────────────────────────────────────────────────────────┘

  [Incident Simulator]
  (Python / Pydantic)
        │  synthetic ICT incidents
        ▼
  [Apache Kafka]  ──── dora.incidents.enriched
  4 topics:            dora.incidents.critical
  critical/major/      dora.incidents.major
  minor/enriched       dora.incidents.minor
        │
        ▼
  [PySpark Structured Streaming]
  + DORA Classifier (rules engine)
  + Enrichment (vendor metadata)
        │  classified + enriched events
        ▼
  [Apache Iceberg tables]  ←──── stored in ────►  [MinIO / dora-lakehouse]
  (HadoopCatalog)                                   S3-compatible object store
        │
        ▼
  [dbt Core]
  staging → intermediate → marts
  + Great Expectations quality checks
        │  clean, tested analytical tables
        ▼
  [PostgreSQL]
  mart_bafin_report
  mart_vendor_risk
  mart_sla_breach
        │
        ▼
  [Apache Superset]
  Compliance dashboards
  BaFin report export

  ─────────────────────────────────────────────────
  [Apache Airflow]  ←── orchestrates dbt runs, quality checks, alerting
```

---

## Tech Stack

| Layer | Technology | Why this choice |
|---|---|---|
| **Ingestion** | Apache Kafka (Confluent 7.5) | Industry-standard event streaming; dot-separated topic naming matches DORA severity tiers |
| **Schema** | Pydantic v2 | Runtime-validated `IncidentEvent` model — single source of truth for all field names |
| **Stream Processing** | PySpark Structured Streaming | Python-native, same API as batch; simpler than Flink for this use case |
| **Table Format** | Apache Iceberg | Schema evolution, time-travel, partitioning by severity + date; better local MinIO support than Delta Lake |
| **Object Storage** | MinIO | Free, S3-compatible, same `boto3` API as real AWS — swap endpoint URL to go to production |
| **Catalog** | Iceberg HadoopCatalog | No extra catalog service needed; works directly against MinIO |
| **Transformation** | dbt Core | SQL-based lineage from raw → staging → intermediate → marts; tested models |
| **Data Quality** | Great Expectations | Validates that no incident arrives with null severity or out-of-range fields |
| **Orchestration** | Apache Airflow 2.8 | Schedules dbt runs and quality checks; LocalExecutor keeps it single-node |
| **Database** | PostgreSQL 15 | Shared by Airflow (metadata) and dbt (mart target); avoids a second DB service |
| **Dashboard** | Apache Superset | Self-hosted BI layer; connects directly to PostgreSQL marts |
| **Infrastructure** | Docker Compose | Zero cloud cost during development; fully reproducible on any machine |

---

## Project Phases

| Phase | Description | Status |
|---|---|---|
| **0** | Folder structure scaffold | ✅ Complete |
| **1** | Infrastructure — Docker stack, Kafka topics, MinIO setup | ✅ Complete |
| **2** | Simulator & Schema — `IncidentEvent` model, Kafka producer | ✅ Complete |
| **3** | DORA Classifier — BaFin Article 18 rules engine + unit tests | 🔄 In progress |
| **4** | Streaming Job — PySpark consumer, Iceberg writer, enrichment | ⏳ Upcoming |
| **5** | dbt & Data Quality — staging/intermediate/mart models, GE suite | ⏳ Upcoming |
| **6** | Airflow Orchestration — pipeline DAG, dbt DAG | ⏳ Upcoming |
| **7** | Dashboard — Superset compliance dashboards | ⏳ Upcoming |
| **8** | Packaging — requirements.txt, CI, final docs | ⏳ Upcoming |

---

## Prerequisites

- **Docker Desktop** (v4.x+) — all services run in containers
- **Python 3.11** — for host-side scripts (simulator, setup scripts)
- **Git**

Python packages required so far:

```bash
pip install confluent-kafka boto3
```

Additional packages are added per phase (`pydantic`, `pyspark`, `pyiceberg`, `dbt-postgres`, `apache-airflow`).

---

## Quick Start

### 1 — Clone and configure

```bash
git clone https://github.com/Chirag-Kathuria-009/dora-incident-pipeline.git
cd dora-incident-pipeline

cp .env.example .env
```

Open `.env` and fill in the two generated secrets:

```bash
# Generate Airflow Fernet key (required — Airflow will not start without it)
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"

# Generate random secret keys for Airflow webserver and Superset
python -c "import secrets; print(secrets.token_hex(32))"
```

Paste the outputs into `AIRFLOW_FERNET_KEY`, `AIRFLOW_SECRET_KEY`, and `SUPERSET_SECRET_KEY`
in your `.env`. All other values work as-is for local development.

### 2 — Start the stack

```bash
docker compose up -d

# Wait until these three report (healthy):
docker compose ps
```

Expected healthy services: `dora-kafka`, `dora-postgres`, `dora-minio`

### 3 — Bootstrap Kafka topics and MinIO storage

```bash
# Creates 4 Kafka topics with correct partition counts and retention policies
python ingestion/kafka/topics_setup.py

# Creates dora-lakehouse bucket and folder prefixes in MinIO
python storage/s3_config.py
```

Both scripts are **idempotent** — safe to run multiple times.

### 4 — Verify everything is working

| Service | URL | Credentials |
|---|---|---|
| Kafka UI | http://localhost:8080 | — |
| MinIO Console | http://localhost:9001 | minioadmin / minioadmin |
| Airflow | http://localhost:8082 | admin / admin |
| Superset | http://localhost:8088 | admin / admin |

---

## Project Structure

```
dora-incident-pipeline/
│
├── docker-compose.yml              # Full 7-service local stack
├── .env.example                    # All required environment variables (copy to .env)
│
├── ingestion/
│   ├── simulator/
│   │   ├── schema.py               # IncidentEvent Pydantic model — source of truth for all field names
│   │   └── incident_generator.py   # Synthetic event producer → Kafka
│   └── kafka/
│       └── topics_setup.py         # Creates 4 DORA Kafka topics (idempotent)
│
├── processing/
│   ├── dora_classifier.py          # BaFin Article 18 rules engine → CRITICAL / MAJOR / MINOR
│   ├── streaming_job.py            # PySpark Structured Streaming consumer + Iceberg writer
│   └── enrichment.py               # Adds vendor metadata and human-readable severity labels
│
├── storage/
│   ├── s3_config.py                # Reusable boto3 MinIO client + bucket/folder bootstrap
│   └── iceberg_tables.py           # PyIceberg table definitions (HadoopCatalog on MinIO)
│
├── transform/
│   ├── dbt_project/
│   │   ├── models/staging/         # stg_incidents — cast and rename raw Iceberg columns
│   │   ├── models/intermediate/    # int_dora_classified — add BaFin notification deadlines
│   │   └── models/marts/           # mart_bafin_report · mart_vendor_risk · mart_sla_breach
│   └── great_expectations/
│       └── expectations/           # Data quality suite — validates incident field constraints
│
├── orchestration/
│   └── dags/
│       ├── dora_pipeline_dag.py    # Main 7-task Airflow DAG
│       └── dbt_run_dag.py          # dbt transformation DAG (triggered after streaming lands)
│
├── dashboard/
│   └── superset_config.py          # Superset Flask runtime config + Phase 7 dashboard bootstrap
│
└── tests/
    ├── test_classifier.py          # 8 unit tests covering every DORA classification boundary
    ├── test_generator.py           # Incident generator and schema validation tests
    └── test_dbt_models.py          # dbt model integration tests against test PostgreSQL schema
```

---

## MinIO Storage Layout

Bucket `dora-lakehouse` is the single storage layer for both raw events and Iceberg tables:

```
dora-lakehouse/
├── raw/incidents/       # JSON landing zone — raw Kafka events before Iceberg ingest
├── iceberg/incidents/   # Iceberg data + metadata files for the incidents table
├── iceberg/vendors/     # Iceberg data + metadata files for the vendor reference table
└── iceberg/audit_log/   # Iceberg audit trail
```

---

## Kafka Topics

| Topic | Partitions | Retention | Purpose |
|---|---|---|---|
| `dora.incidents.critical` | 3 | 7 days | CRITICAL classified events awaiting BaFin notification |
| `dora.incidents.major` | 3 | 7 days | MAJOR classified events |
| `dora.incidents.minor` | 2 | 3 days | MINOR events (internal log only) |
| `dora.incidents.enriched` | 3 | 30 days | All events post-enrichment (full audit trail) |

---

## DORA Classification Rules (BaFin Article 18)

The rules engine in `processing/dora_classifier.py` evaluates these conditions in priority order:

**CRITICAL** — notify BaFin within 4 hours if any condition is true:
- `clients_affected_pct >= 25%`
- `financial_impact_eur >= €1,000,000`
- `cyber_attack = true` AND `clients_affected_pct >= 10%`
- `cross_border = true` AND `clients_affected_pct >= 10%`

**MAJOR** — notify BaFin within 72 hours (only if not already CRITICAL):
- `clients_affected_pct >= 10%`
- `financial_impact_eur >= €100,000`
- `third_party_provider` is set AND `incident_type = "system_outage"`

**MINOR** — internal log only, no BaFin notification required.

---

## Key Design Decisions

See [decisions.md](decisions.md) for the full dated log. Highlights:

- **Iceberg over Delta Lake** — PyIceberg has better local MinIO / HadoopCatalog support
- **PySpark over Flink** — simpler Python integration; adequate throughput for DORA event scale
- **PostgreSQL shared** — used by both Airflow (metadata) and dbt (mart target) to avoid a second DB
- **Superset uses SQLite** — `apache/superset:latest` does not bundle psycopg2; SQLite is sufficient for local dashboard metadata
- **100% Docker Compose** — zero cloud cost, fully reproducible on any reviewer's machine

---

## Contributing / Extending

This project is built phase by phase with a clear quality gate at each phase:

```bash
# Phase 3 gate — all 8 classifier unit tests must pass
pytest tests/test_classifier.py -v

# Phase 5 gate — dbt models must pass all schema tests
dbt test --select staging
dbt run --select marts

# Phase 6 gate — Airflow DAG structure tests
pytest tests/test_dag.py -v

# Full suite
pytest tests/ -v
```

---

## License

MIT

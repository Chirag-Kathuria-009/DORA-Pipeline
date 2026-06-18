# Architecture — DORA ICT Incident Intelligence Pipeline

End-to-end view of how data flows through the pipeline and how the components connect.

## Data flow

```mermaid
flowchart TD
  GEN["incident_generator.py (host)"] -->|JSON| K1[("Kafka: critical/major/minor")]
  K1 --> SJ["streaming_job.py PySpark (host)"]
  SJ --> CLS["DORAClassifier.classify()"]
  SJ -->|foreachBatch| IC[("Iceberg incidents_classified")]
  SJ --> AL[("Iceberg audit_log")]
  SJ --> KE[("Kafka enriched")]
  IC -. catalog pointer .-> CAT[("dora_catalog.db SQLite host")]
  IC -. data/metadata .-> MIN[("MinIO dora-lakehouse")]

  subgraph AF["Airflow DAG every 15m — DockerOperator tasks"]
    direction TB
    T1["1 check_kafka_health"] --> T2["2 sync_iceberg_to_postgres"]
    T2 --> T3["3 run_great_expectations"]
    T3 --> T4["4 run_dbt_staging"]
    T4 --> T5["5 run_dbt_marts (int -> marts)"]
    T5 --> T6["6 check_compliance_alerts"]
    T6 --> T7["7 update_pipeline_metadata"]
  end

  IC --> T2
  CAT --> T2
  T2 --> PG[("Postgres dora.incidents_classified")]
  PG --> T3
  PG --> T4
  SEED[("seed ict_vendors")] --> T4
  T4 --> STG[("stg_* views")]
  STG --> T5
  T5 --> MARTS[("marts: bafin / vendor / sla")]
  MARTS --> T6
  T7 --> RUNS[("public.pipeline_runs")]
  MARTS --> SUP["Superset dashboards"]
```

## Stages

1. **Ingestion (host):** `incident_generator.py` produces synthetic `IncidentEvent` JSON to the
   `dora.incidents.{critical,major,minor}` Kafka topics.
2. **Stream processing (host):** `streaming_job.py` (PySpark) reads the topics, applies
   `DORAClassifier.classify()`, stamps audit metadata, and writes via `foreachBatch` to Iceberg
   (`incidents_classified` + `audit_log`) and the `dora.incidents.enriched` topic.
3. **Lakehouse storage:** Apache Iceberg — table data/metadata in **MinIO**
   (`s3://dora-lakehouse/iceberg`); the catalog pointer is a local **SQLite** file
   (`dora_catalog.db`). This shared SQLite catalog is why the Airflow runner bind-mounts the host repo.
4. **Orchestration (Airflow, every 15 min):** each of the 7 tasks runs in the
   `dora/pipeline-runner` container (DockerOperator) — sync Iceberg→Postgres, Great Expectations,
   dbt staging → intermediate → marts, compliance alerting, and run-metadata logging.
5. **Serving:** PostgreSQL marts (`mart_bafin_report`, `mart_vendor_risk`, `mart_sla_breach`)
   feed the **Superset** "DORA Regulatory Compliance Dashboard".

## Why DockerOperator (Option B)

Airflow 2.8 pins `SQLAlchemy<2.0`, but PyIceberg's SqlCatalog needs `>=2.0`, so the heavy
pipeline tools (pyiceberg/pandas/dbt/Great Expectations/confluent-kafka) cannot live in Airflow's
own environment. Every DAG task therefore runs a command inside a separate `dora/pipeline-runner`
image — which also maps 1:1 to a production KubernetesPodOperator.

## Component status

| Stage | Component | Status |
|---|---|---|
| Ingestion | `incident_generator.py` (host) | ✅ |
| Stream + classify | `streaming_job.py` + `DORAClassifier` (host) | ✅ |
| Storage | Iceberg on MinIO + SQLite catalog | ✅ |
| Transform | dbt staging → intermediate → marts | ✅ |
| Data quality | Great Expectations suite | ✅ |
| Orchestration | Airflow DAG via DockerOperator | ✅ |
| Serving | Superset compliance dashboard | ✅ |

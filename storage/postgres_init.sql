-- postgres_init.sql
-- Runs automatically on first PostgreSQL container start via docker-entrypoint-initdb.d.
-- The primary database (POSTGRES_DB=dora) is created by the postgres image itself.
-- Superset now uses SQLite (see decisions.md) — no extra database needed here.

-- Placeholder for future schema seeds (e.g. dbt sources, test fixtures)
SELECT 1;

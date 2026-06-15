-- assert_no_null_severity.sql
-- dbt singular test: asserts that no incident row has a NULL dora_severity value.
-- Returns the violating rows; dbt fails the test if any rows are returned.

select
    incident_id,
    dora_severity
from {{ ref('stg_incidents') }}
where dora_severity is null

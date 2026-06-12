-- mart_sla_breach — BaFin notification SLA tracking.
-- Grain: one row per REPORTABLE incident (critical/major; minor has no deadline).
--
-- Thin projection over int_dora_classified: deadline_timestamp, containment_hours,
-- sla_breached, and hours_overdue are all computed in the intermediate layer.

with incidents as (

    select * from {{ ref('int_dora_classified') }}
    where bafin_notification_deadline_hours is not null   -- reportable only

)

select
    incident_id,
    institution_id,
    dora_severity,
    detection_timestamp,
    bafin_notification_deadline_hours,
    deadline_timestamp,
    containment_hours as actual_containment_hours,
    sla_breached,
    hours_overdue
from incidents

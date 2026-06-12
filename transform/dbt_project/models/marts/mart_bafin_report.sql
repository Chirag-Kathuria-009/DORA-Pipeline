-- mart_bafin_report — simulated mandatory BaFin incident-register submission.
-- Grain: one row per (reporting month, institution).
--
-- Pure aggregation over int_dora_classified — the reporting_period, reportability,
-- containment, and within-deadline flags are all computed in the intermediate layer.

with incidents as (

    select * from {{ ref('int_dora_classified') }}

),

aggregated as (

    select
        reporting_period,
        institution_id,
        institution_type,

        count(*)                                            as incident_count_total,
        count(*) filter (where dora_severity = 'critical')  as incident_count_critical,
        count(*) filter (where dora_severity = 'major')     as incident_count_major,
        count(*) filter (where dora_severity = 'minor')     as incident_count_minor,

        count(*) filter (where bafin_notification_required) as incidents_reported_to_bafin,

        round(
            avg(containment_hours) filter (where containment_hours is not null)::numeric, 2
        )                                                   as avg_time_to_contain_hours,

        -- denominator: all reportable incidents; numerator: those within deadline
        count(*) filter (where bafin_notification_required) as reportable_total,
        count(*) filter (where is_within_deadline)          as reportable_within_deadline

    from incidents
    group by 1, 2, 3

),

final as (

    select
        reporting_period,
        institution_id,
        institution_type,
        incident_count_total,
        incident_count_critical,
        incident_count_major,
        incident_count_minor,
        incidents_reported_to_bafin,
        avg_time_to_contain_hours,

        -- NULL when nothing was reportable that month (no BaFin obligation).
        round(
            100.0 * reportable_within_deadline / nullif(reportable_total, 0),
            2
        ) as compliance_rate_pct

    from aggregated

)

select
    *,
    -- Illustrative thresholds. No reportable incidents (NULL rate) => vacuously COMPLIANT.
    case
        when compliance_rate_pct is null  then 'COMPLIANT'
        when compliance_rate_pct >= 95    then 'COMPLIANT'
        when compliance_rate_pct >= 80    then 'AT_RISK'
        else 'NON_COMPLIANT'
    end as compliance_status
from final

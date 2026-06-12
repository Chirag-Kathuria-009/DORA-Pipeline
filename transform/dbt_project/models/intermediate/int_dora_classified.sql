-- int_dora_classified — enriched, one row per incident.
--
-- stg_incidents joined with the vendor reference dimension, plus the SHARED BaFin
-- deadline / containment / SLA fields consumed by all three marts. Centralising the
-- deadline math here keeps the marts thin and removes the duplication that would
-- otherwise live in both mart_bafin_report and mart_sla_breach.
--
-- Materialised as a view (see dbt_project.yml: intermediate +materialized: view).

with incidents as (

    select * from {{ ref('stg_incidents') }}

),

vendors as (

    select * from {{ ref('stg_ict_vendors') }}

),

joined as (

    select
        -- keys & identity
        i.incident_sk,
        i.incident_id,

        -- timing
        i.incident_timestamp,
        i.detection_timestamp,
        i.containment_timestamp,
        to_char(date_trunc('month', i.incident_timestamp), 'YYYY-MM') as reporting_period,

        -- institution
        i.institution_id,
        i.institution_type,

        -- incident attributes
        i.incident_type,
        i.affected_systems,
        i.clients_affected_pct,
        i.financial_impact_eur,
        i.is_cross_border,

        -- DORA classification
        i.dora_severity,
        i.bafin_notification_required,
        i.bafin_notification_deadline_hours,
        i.classification_reason,

        -- third-party vendor (LEFT JOIN: attrs NULL when provider absent from seed)
        i.ict_third_party_provider,
        v.vendor_type,
        v.eu_headquartered,
        coalesce(v.dora_designated_critical, false) as dora_designated_critical,
        v.concentration_risk_score                  as vendor_concentration_risk_score,
        coalesce(v.is_hyperscaler, false)           as is_hyperscaler,

        -- derived: containment time in hours (NULL if still uncontained)
        case when i.containment_timestamp is not null then
            round(extract(epoch from (i.containment_timestamp - i.detection_timestamp)) / 3600.0, 2)
        end as containment_hours,

        -- derived: BaFin deadline timestamp (NULL for minor / non-reportable)
        case when i.bafin_notification_deadline_hours is not null then
            i.detection_timestamp + (i.bafin_notification_deadline_hours * interval '1 hour')
        end as deadline_timestamp

    from incidents i
    left join vendors v
        on i.ict_third_party_provider = v.vendor_name

),

final as (

    select
        *,

        -- reportable incident contained within its deadline window
        (
            bafin_notification_required
            and containment_timestamp is not null
            and containment_hours <= bafin_notification_deadline_hours
        ) as is_within_deadline,

        -- SLA breach (reportable only): contained late, OR uncontained past deadline
        case
            when bafin_notification_deadline_hours is null then null
            when containment_timestamp is not null
                then containment_hours > bafin_notification_deadline_hours
            else current_timestamp > deadline_timestamp
        end as sla_breached,

        -- hours past deadline when breached; NULL otherwise
        case
            when bafin_notification_deadline_hours is null then null
            when containment_timestamp is not null
                 and containment_hours > bafin_notification_deadline_hours
                then round(containment_hours - bafin_notification_deadline_hours, 2)
            when containment_timestamp is null
                 and current_timestamp > deadline_timestamp
                then round(extract(epoch from (current_timestamp - deadline_timestamp)) / 3600.0, 2)
        end as hours_overdue

    from joined

)

select * from final

-- mart_vendor_risk — third-party ICT concentration-risk register (DORA Article 28).
-- Grain: one row per ICT third-party provider that appears in incident history.
--
-- Aggregation over int_dora_classified, which already carries the vendor attributes
-- (joined from the seed) on each incident row. Providers missing from the seed
-- (currently "Google Cloud", "Murex") still appear, with NULL vendor attrs and
-- dora_designated_critical defaulted to false in the intermediate layer.

with incidents as (

    select * from {{ ref('int_dora_classified') }}
    where ict_third_party_provider is not null

),

by_provider as (

    select
        ict_third_party_provider,
        -- vendor attributes are functionally determined by the provider, so they
        -- are grouping keys (one constant value per provider)
        vendor_type,
        eu_headquartered,
        dora_designated_critical,

        count(*) filter (
            where incident_timestamp >= current_date - interval '90 days'
        )                                          as incident_count_last_90d,
        round(avg(financial_impact_eur)::numeric, 2) as avg_financial_impact_eur,
        count(distinct institution_id)             as institutions_exposed

    from incidents
    group by 1, 2, 3, 4

)

select
    *,
    -- HIGH if widely depended upon (>=5 institutions) or DORA-designated critical;
    -- otherwise graded by how many institutions are exposed.
    case
        when institutions_exposed >= 5 or dora_designated_critical then 'HIGH'
        when institutions_exposed >= 2                             then 'MEDIUM'
        else 'LOW'
    end as concentration_risk_tier
from by_provider

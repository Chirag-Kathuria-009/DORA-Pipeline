-- stg_ict_vendors — staging layer over the ict_vendors seed.
--
-- Cleans the reference vendor list (trims stray whitespace) and adds an
-- is_hyperscaler flag for the three public-cloud hyperscalers. Used by
-- stg_incidents' relationships test and the downstream mart_vendor_risk model.

with source as (

    select * from {{ ref('ict_vendors') }}

),

cleaned as (

    select
        trim(vendor_name)                          as vendor_name,
        trim(vendor_type)                          as vendor_type,
        cast(eu_headquartered as boolean)          as eu_headquartered,
        cast(dora_designated_critical as boolean)  as dora_designated_critical,
        cast(concentration_risk_score as integer)  as concentration_risk_score,

        -- Hyperscalers: the global public-cloud providers. Both seed spellings of
        -- Google's cloud ('GCP' and 'Google Cloud') are included.
        case
            when trim(vendor_name) in ('AWS', 'Azure', 'GCP', 'Google Cloud') then true
            else false
        end                                        as is_hyperscaler

    from source

)

select * from cleaned

-- stg_incidents — staging layer over dora.incidents_classified.
--
-- Source is the Iceberg table dora.incidents_classified, landed into Postgres by
-- the Iceberg->Postgres sync (see source 'dora' in schema.yml). This model casts
-- every column to a stable type, exposes tz-aware timestamps, adds a surrogate
-- key, and drops unusable (null-key / unclassified) rows.
--
-- NOTE: the raw "timestamp" column is renamed to incident_timestamp — `timestamp`
-- is a reserved type keyword in Postgres and breaks generated test/SELECT SQL.

with source as (

    select * from {{ source('dora', 'incidents_classified') }}

),

renamed as (

    select
        -- surrogate key (deterministic hash of the natural key)
        {{ dbt_utils.generate_surrogate_key(['incident_id']) }} as incident_sk,

        -- identity & timing (tz-aware)
        cast(incident_id as varchar)                       as incident_id,
        cast("timestamp" as timestamptz)                   as incident_timestamp,
        cast(detection_timestamp as timestamptz)           as detection_timestamp,
        cast(containment_timestamp as timestamptz)         as containment_timestamp,

        -- institution
        cast(institution_id as varchar)                    as institution_id,
        cast(institution_type as varchar)                  as institution_type,

        -- incident classification
        cast(incident_type as varchar)                     as incident_type,
        affected_systems                                   as affected_systems,

        -- impact metrics
        cast(clients_affected_pct as double precision)     as clients_affected_pct,
        cast(financial_impact_eur as double precision)     as financial_impact_eur,

        -- third-party / context
        cast(ict_third_party_provider as varchar)          as ict_third_party_provider,
        cast(is_cross_border as boolean)                   as is_cross_border,

        -- DORA classification (filled by the streaming classifier)
        cast(dora_severity as varchar)                     as dora_severity,
        cast(bafin_notification_required as boolean)       as bafin_notification_required,
        cast(bafin_notification_deadline_hours as integer) as bafin_notification_deadline_hours,
        cast(classification_reason as varchar)             as classification_reason

    from source

)

select * from renamed
-- Filter out test/null records: rows missing a natural key, a timestamp, or a
-- severity are unusable downstream (there is no explicit test-record flag in the
-- IncidentEvent schema, so null-key filtering is the available signal).
where incident_id is not null
  and incident_timestamp is not null
  and dora_severity is not null

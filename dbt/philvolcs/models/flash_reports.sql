-- Final flash-report table in BigQuery (n8n-rogverse.philvolcs.flash_reports).
-- Runs only on the bigquery target; depends on the raw LLM output table the
-- DAG inserts into, not on the Postgres staging models. Deduplicates to one
-- row per event, keeping the most recently generated report.
with ranked as (
    select
        event_time,
        latitude,
        longitude,
        depth_km,
        magnitude,
        location,
        headline,
        flash_report_message,
        urgency,
        source_model,
        generated_at,
        row_number() over (
            -- BigQuery rejects FLOAT64 window keys; event_time is unique per
            -- event, so lat/lon are not needed to dedup.
            partition by event_time
            order by generated_at desc
        ) as rn
    from {{ source('bq_raw', 'raw_llm_flash_reports') }}
)

select * except (rn)
from ranked
where rn = 1

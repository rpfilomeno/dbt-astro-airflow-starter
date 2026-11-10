-- The single latest event, ready for the flash-report LLM task (Postgres target).
with ranked as (
    select
        event_time,
        latitude,
        longitude,
        depth_km,
        magnitude,
        location,
        bulletin_url,
        row_number() over (order by event_time desc) as rn
    from {{ ref('stg_earthquakes') }}
)

select
    event_time,
    latitude,
    longitude,
    depth_km,
    magnitude,
    location,
    bulletin_url
from ranked
where rn = 1

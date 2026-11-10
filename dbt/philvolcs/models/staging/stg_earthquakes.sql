-- Clean and cast the raw scraped PHIVOLCS rows. Published event times are
-- Philippine Standard Time; the result is re-anchored to Asia/Manila and
-- yielded as a UTC instant. Date and time are parsed separately: a single
-- to_timestamp with a literal between date and time eats one character of the
-- hour (Postgres HH12 quirk), silently corrupting 12/11-hour times and
-- rejecting every 10:xx row.
with parsed as (
    select
        (
            to_date(split_part(event_time_text, ' - ', 1), 'DD Month YYYY')
            + to_timestamp(split_part(event_time_text, ' - ', 2), 'HH12:MI AM')::time
        ) at time zone 'Asia/Manila' as event_time,
        nullif(trim(latitude_text), '')::numeric as latitude,
        nullif(trim(longitude_text), '')::numeric as longitude,
        nullif(trim(depth_text), '')::int as depth_km,
        nullif(trim(magnitude_text), '')::numeric as magnitude,
        nullif(trim(location), '') as location,
        bulletin_url,
        scraped_at
    from {{ source('pg_raw', 'raw_phivolcs_earthquakes') }}
)

select *
from parsed
where event_time is not null
  and latitude is not null
  and longitude is not null
  and depth_km is not null
  and magnitude is not null

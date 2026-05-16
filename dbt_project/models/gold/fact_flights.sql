with flights as (
    select *
    from {{ ref('stg_flights') }}
)

select
    flight_id as flight_key,

    cast(to_char(flight_date, 'YYYYMMDD') as integer) as date_key,
    md5(airline) as airline_key,
    md5(dep_airport) as dep_airport_key,
    md5(arr_airport) as arr_airport_key,
    md5(tail_number) as aircraft_key,

    dep_time_label,
    dep_delay,
    dep_delay_tag,
    dep_delay_type,
    arr_delay,
    arr_delay_type,
    flight_duration,
    distance_type,

    delay_carrier,
    delay_weather,
    delay_nas,
    delay_security,
    delay_last_aircraft,

    _ingested_at,
    _source_file
from flights

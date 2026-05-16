with source as (
    select *
    from {{ source('bronze', 'raw_flights') }}
),

cleaned as (
    select
        md5(
            concat_ws(
                '|',
                flight_date,
                airline,
                tail_number,
                dep_airport,
                arr_airport,
                dep_time_label,
                dep_delay,
                arr_delay,
                flight_duration,
                dep_delay_tag,
                coalesce(_source_file, '')
            )
        ) as flight_id,

        nullif(trim(flight_date), '')::date as flight_date,
        day_of_week::integer as day_of_week,

        nullif(trim(airline), '') as airline,
        nullif(trim(tail_number), '') as tail_number,
        nullif(trim(manufacturer), '') as manufacturer,
        nullif(trim(model), '') as model,
        aircraft_age::integer as aircraft_age,

        nullif(trim(dep_airport), '') as dep_airport,
        nullif(trim(dep_city), '') as dep_city,
        nullif(trim(dep_time_label), '') as dep_time_label,
        dep_delay::integer as dep_delay,
        dep_delay_tag::integer as dep_delay_tag,
        nullif(trim(dep_delay_type), '') as dep_delay_type,

        nullif(trim(arr_airport), '') as arr_airport,
        nullif(trim(arr_city), '') as arr_city,
        arr_delay::integer as arr_delay,
        nullif(trim(arr_delay_type), '') as arr_delay_type,

        flight_duration::integer as flight_duration,
        nullif(trim(distance_type), '') as distance_type,

        delay_carrier::integer as delay_carrier,
        delay_weather::integer as delay_weather,
        delay_nas::integer as delay_nas,
        delay_security::integer as delay_security,
        delay_last_aircraft::integer as delay_last_aircraft,

        _ingested_at,
        _source_file
    from source
)

select *
from cleaned
where flight_date is not null

with source as (
    select *
    from {{ source('bronze', 'raw_flights') }}
),

deduped as (
    select *
    from (
        select *,
            row_number() over (
                partition by
                    flight_date, airline, tail_number, dep_airport, arr_airport,
                    dep_time_label, dep_delay, arr_delay, flight_duration,
                    dep_delay_tag, distance_type, delay_carrier, delay_weather,
                    delay_nas, delay_security, delay_last_aircraft, dep_delay_type,
                    coalesce(_source_file, '')
                order by _ingested_at
            ) as rn
        from source
    ) t
    where rn = 1
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
                distance_type,
                delay_carrier,
                delay_weather,
                delay_nas,
                delay_security,
                delay_last_aircraft,
                dep_delay_type,
                coalesce(_source_file, '')
            )
        ) as flight_id,

        nullif(trim(flight_date), '')::date as flight_date,
        nullif(trim(day_of_week), '')::integer as day_of_week,

        nullif(trim(airline), '') as airline,
        nullif(trim(tail_number), '') as tail_number,
        nullif(trim(manufacturer), '') as manufacturer,
        nullif(trim(model), '') as model,
        nullif(trim(aircraft_age), '')::integer as aircraft_age,

        nullif(trim(dep_airport), '') as dep_airport,
        nullif(trim(dep_city), '') as dep_city,
        nullif(trim(dep_time_label), '') as dep_time_label,
        nullif(trim(dep_delay), '')::integer as dep_delay,
        nullif(trim(dep_delay_tag), '')::integer as dep_delay_tag,
        nullif(trim(dep_delay_type), '') as dep_delay_type,

        nullif(trim(arr_airport), '') as arr_airport,
        nullif(trim(arr_city), '') as arr_city,
        nullif(trim(arr_delay), '')::integer as arr_delay,
        nullif(trim(arr_delay_type), '') as arr_delay_type,

        nullif(trim(flight_duration), '')::integer as flight_duration,
        nullif(trim(distance_type), '') as distance_type,

        nullif(trim(delay_carrier), '')::integer as delay_carrier,
        nullif(trim(delay_weather), '')::integer as delay_weather,
        nullif(trim(delay_nas), '')::integer as delay_nas,
        nullif(trim(delay_security), '')::integer as delay_security,
        nullif(trim(delay_last_aircraft), '')::integer as delay_last_aircraft,

        _ingested_at,
        _source_file
    from deduped
)

select *
from cleaned
where flight_date is not null

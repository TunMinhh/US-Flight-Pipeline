select distinct 
    cast(to_char(flight_date, 'YYYYMMDD') as integer) as date_key,
    flight_date,
    day_of_week,
    extract(month from flight_date)::integer as month,
    extract(quarter from flight_date)::integer as quarter,
    extract(year from flight_date)::integer as year
from {{ ref('stg_flights') }}
where flight_date is not null


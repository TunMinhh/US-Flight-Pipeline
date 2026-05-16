with airports as (
    select
        dep_airport as airport_code,
        dep_city as city_name
    from {{ ref('stg_flights')}}
    where dep_airport is not null

    union 

    select 
        arr_airport as airport_code,
        arr_city as city_name
    from {{ ref('stg_flights')}}
    where arr_airport is not null
)

select 
    md5(airport_code) as airport_key,
    airport_code,
    city_name
from airports


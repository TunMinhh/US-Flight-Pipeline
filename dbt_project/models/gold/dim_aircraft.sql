with aircrafts as (
    select
        tail_number,
        min(manufacturer) as manufacturer,
        min(model) as model,
        max(aircraft_age) as aircraft_age
    from {{ ref('stg_flights') }}
    where tail_number is not null
    group by tail_number
)

select
    md5(tail_number) as aircraft_key,
    tail_number,
    manufacturer,
    model,
    aircraft_age
from aircrafts

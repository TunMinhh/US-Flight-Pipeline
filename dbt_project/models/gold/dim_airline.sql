select distinct 
    md5(airline) as airline_key,
    airline as airline_name
from {{ ref('stg_flights') }}
where airline is not null
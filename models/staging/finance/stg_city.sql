select
    city_id,
    city as city_name,
    state_id
from {{ source('raw', 'CITY') }}
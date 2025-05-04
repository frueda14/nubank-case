select
    state as state_code,
    country_id,
    state_id
from {{ source('raw', 'STATE') }}
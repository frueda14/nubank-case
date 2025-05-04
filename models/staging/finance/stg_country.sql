select
    country_id,
    country as country_name
from {{ source('raw', 'COUNTRY') }}
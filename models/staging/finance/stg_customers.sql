select
    customer_id::number  as customer_id,
    first_name,
    last_name,
    customer_city::number as city_id,
    cpf::number          as cpf,
    country_name         as country_name
from {{ source('raw', 'CUSTOMERS') }}

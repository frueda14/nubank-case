select
    c.customer_id,
    c.first_name,
    c.last_name,
    ci.city_name,
    s.state_code,
    co.country_name
from {{ ref('stg_customers') }} as c
left join {{ ref('stg_city') }}  as ci 
    on c.city_id = ci.city_id

left join {{ ref('stg_state') }} as s
    on ci.state_id = s.state_id

left join {{ ref('stg_country') }} as co
    on s.country_id = co.country_id

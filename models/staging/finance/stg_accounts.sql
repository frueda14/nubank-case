{{ config(materialized='view') }}

select
    account_id::number               as account_id,
    customer_id::number              as customer_id,
    created_at::timestamp_ntz        as created_at,
    status                           as status,
    account_branch::number           as branch,
    account_check_digit::number      as check_digit,
    account_number::number           as account_number
from {{ source('raw', 'ACCOUNTS') }}
where account_id is not null

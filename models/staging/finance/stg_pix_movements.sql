with src as (
    select * from {{ source('raw', 'PIX_MOVEMENTS') }}
)
select
    id::number           as pix_id,
    account_id::number   as account_id,
    pix_amount::number(18,2) as amount,
    to_timestamp_ntz(pix_requested_at / 1000)  as requested_ts,
    try_to_timestamp_ntz(pix_completed_at)     as completed_ts,
    status,
    in_or_out            as direction          -- 'pix_in' | 'pix_out'
from src
where status = 'completed'

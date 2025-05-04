select
    id::number                        as transfer_id,
    account_id::number               as account_id,
    amount::number(18,2)             as amount,
    to_timestamp_ntz(transaction_requested_at / 1000) as requested_ts,
    try_to_timestamp_ntz(transaction_completed_at)    as completed_ts,
    status
from {{ source('raw', 'TRANSFER_INS') }}
where status = 'completed'
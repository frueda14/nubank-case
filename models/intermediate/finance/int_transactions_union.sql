{{ config(materialized='table') }}

with pix as (
    select
        pix_id                 as txn_id,
        account_id,
        amount * case when direction = 'pix_out' then -1 else 1 end as signed_amount,
        completed_ts           as txn_ts,
        'PIX'                  as source
    from {{ ref('stg_pix_movements') }}
),

tx_in as (
    select
        transfer_id            as txn_id,
        account_id,
        amount                 as signed_amount,   -- input ➕
        completed_ts           as txn_ts,
        'TRANSFER_IN'          as source
    from {{ ref('stg_transfer_ins') }}
),

tx_out as (
    select
        transfer_id,
        account_id,
        -amount                as signed_amount,   -- out ➖
        completed_ts,
        'TRANSFER_OUT'
    from {{ ref('stg_transfer_outs') }}
)

select * from pix
union all
select * from tx_in
union all
select * from tx_out
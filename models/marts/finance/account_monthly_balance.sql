{{ 
    config(materialized='incremental',
           unique_key='account_id||month',
           incremental_strategy='merge'
          ) 
}}

with movements as (
    select
        account_id,
        date_trunc('month', txn_ts) as month,
        sum(signed_amount)          as delta_month
    from {{ ref('int_transactions_union') }}
    where txn_ts is not null
    group by 1, 2
),

running as (
    select
        account_id,
        month,
        sum(delta_month) over (
            partition by account_id
            order by month
            rows between unbounded preceding and current row
        ) as closing_balance
    from movements
)

select * from running
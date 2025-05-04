select
    time_id::number                     as time_id,
    action_timestamp                    as ts,
    week_id::number                     as week_id,
    month_id::number                    as month_id,
    year_id::number                     as year_id,
    weekday_id::number                  as weekday_id
from {{ source('raw', 'D_TIME') }}

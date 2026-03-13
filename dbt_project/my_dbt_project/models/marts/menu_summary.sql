select
    event_date,
    count(*) as record_count,
    avg(amount) as avg_amount,
    sum(amount) as total_amount
from {{ ref('stg_menu') }}
group by event_date
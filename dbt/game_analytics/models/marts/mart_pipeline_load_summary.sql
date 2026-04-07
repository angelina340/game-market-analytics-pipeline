select
    date_trunc('day', loaded_at) as load_date,
    source,
    target_table,
    count(*) as load_attempts,
    sum(case when status = 'loaded' then 1 else 0 end) as successful_loads,
    sum(case when status = 'skipped_existing' then 1 else 0 end) as skipped_existing_loads,
    sum(rows_loaded) as total_rows_loaded,
    max(loaded_at) as latest_loaded_at
from {{ ref('stg_raw_load_audit') }}
group by 1, 2, 3
order by load_date desc, source, target_table

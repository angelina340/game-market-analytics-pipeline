select
    platform_name,
    count(distinct rawg_game_id) as game_count,
    avg(case when released_at is not null then 1 else 0 end) as pct_with_platform_release_date,
    max(extracted_at_utc) as latest_extract_at_utc
from {{ ref('stg_rawg_game_platforms') }}
group by 1
order by game_count desc, platform_name

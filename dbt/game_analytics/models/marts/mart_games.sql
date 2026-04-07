{{ config(
    materialized='incremental',
    unique_key='rawg_game_id',
    incremental_strategy='merge'
) }}

with rawg_games as (
    select *
    from {{ ref('stg_rawg_games') }}
),

steam_games as (
    select *
    from {{ ref('stg_steam_featured_games') }}
),

steam_name_rollup as (
    select
        lower(trim(game_name)) as game_name_key,
        max(steam_app_id) as steam_app_id,
        max(category_name) as steam_category_name,
        max(is_discounted) as is_discounted_on_steam,
        max(discount_percent) as steam_discount_percent,
        max(final_price_usd) as steam_final_price_usd,
        max(windows_available) as steam_windows_available,
        max(mac_available) as steam_mac_available,
        max(linux_available) as steam_linux_available
    from steam_games
    where game_name is not null
    group by 1
),

genre_rollup as (
    select
        rawg_game_id,
        listagg(distinct genre_name, ', ') within group (order by genre_name) as genres
    from {{ ref('stg_rawg_game_genres') }}
    group by 1
),

platform_rollup as (
    select
        rawg_game_id,
        count(distinct platform_name) as platform_count,
        listagg(distinct platform_name, ', ') within group (order by platform_name) as platforms
    from {{ ref('stg_rawg_game_platforms') }}
    group by 1
)

select
    g.rawg_game_id,
    g.slug,
    g.game_name,
    g.released_date,
    g.is_tba,
    g.rating,
    g.ratings_count,
    g.reviews_count,
    g.metacritic_score,
    g.added_count,
    g.playtime_hours,
    coalesce(gr.genres, 'Unknown') as genres,
    coalesce(pr.platform_count, 0) as platform_count,
    pr.platforms,
    s.steam_app_id,
    s.steam_category_name,
    s.is_discounted_on_steam,
    s.steam_discount_percent,
    s.steam_final_price_usd,
    s.steam_windows_available,
    s.steam_mac_available,
    s.steam_linux_available,
    g.extracted_at_utc
from rawg_games as g
left join steam_name_rollup as s
    on lower(trim(g.game_name)) = s.game_name_key
left join genre_rollup as gr
    on g.rawg_game_id = gr.rawg_game_id
left join platform_rollup as pr
    on g.rawg_game_id = pr.rawg_game_id

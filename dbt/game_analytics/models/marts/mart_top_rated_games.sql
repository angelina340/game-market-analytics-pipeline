select
    rawg_game_id,
    game_name,
    released_date,
    rating,
    ratings_count,
    reviews_count,
    metacritic_score,
    genres,
    platform_count,
    platforms,
    steam_app_id,
    is_discounted_on_steam,
    steam_discount_percent,
    steam_final_price_usd
from {{ ref('mart_games') }}
where rating is not null
qualify row_number() over (
    order by rating desc, ratings_count desc, reviews_count desc, game_name
) <= 25

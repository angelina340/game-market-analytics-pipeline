with source_data as (
    select
        extracted_at,
        payload
    from {{ source('raw', 'RAWG_GAMES_RAW') }}
),

latest_extract as (
    select max(extracted_at) as extracted_at
    from source_data
),

flattened_games as (
    select
        source_data.extracted_at,
        game.value as game_json
    from source_data
    inner join latest_extract
        on source_data.extracted_at = latest_extract.extracted_at,
    lateral flatten(input => payload:payload:results) as game
)

select
    game_json:id::number as rawg_game_id,
    game_json:slug::string as slug,
    game_json:name::string as game_name,
    game_json:released::date as released_date,
    game_json:tba::boolean as is_tba,
    game_json:rating::float as rating,
    game_json:rating_top::number as rating_top,
    game_json:ratings_count::number as ratings_count,
    game_json:reviews_count::number as reviews_count,
    game_json:reviews_text_count::number as reviews_text_count,
    game_json:added::number as added_count,
    game_json:playtime::number as playtime_hours,
    game_json:suggestions_count::number as suggestions_count,
    game_json:metacritic::number as metacritic_score,
    game_json:background_image::string as background_image_url,
    game_json:updated::timestamp_ntz as source_updated_at,
    extracted_at as extracted_at_utc
from flattened_games
qualify row_number() over (
    partition by game_json:id::number
    order by extracted_at desc
) = 1

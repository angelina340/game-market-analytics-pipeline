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
),

flattened_platforms as (
    select
        game_json:id::number as rawg_game_id,
        game_json:name::string as game_name,
        extracted_at,
        platform.value as platform_json
    from flattened_games,
    lateral flatten(input => game_json:platforms) as platform
)

select
    rawg_game_id,
    game_name,
    platform_json:platform:id::number as platform_id,
    platform_json:platform:name::string as platform_name,
    platform_json:platform:slug::string as platform_slug,
    platform_json:released_at::date as released_at,
    extracted_at as extracted_at_utc
from flattened_platforms
qualify row_number() over (
    partition by rawg_game_id, platform_json:platform:id::number
    order by extracted_at desc
) = 1

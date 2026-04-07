create or replace file format JSON_RAW_FORMAT
type = json
strip_outer_array = false;

create table if not exists GAME_ANALYTICS.RAW.RAWG_GAMES_RAW (
    source varchar,
    s3_key varchar,
    extracted_at timestamp_ntz,
    payload variant,
    loaded_at timestamp_ntz default current_timestamp()
);

create table if not exists GAME_ANALYTICS.RAW.STEAM_GAMES_RAW (
    source varchar,
    s3_key varchar,
    extracted_at timestamp_ntz,
    payload variant,
    loaded_at timestamp_ntz default current_timestamp()
);

create table if not exists GAME_ANALYTICS.RAW.RAW_LOAD_AUDIT (
    source varchar,
    s3_key varchar,
    target_table varchar,
    rows_loaded number,
    status varchar,
    loaded_at timestamp_ntz default current_timestamp()
);

-- Replace the placeholder credentials before running manually in Snowflake.
create or replace stage GAME_ANALYTICS.RAW.GAME_ANALYTICS_RAW_STAGE
url = 's3://YOUR_BUCKET_NAME/raw/'
credentials = (
    aws_key_id = 'YOUR_AWS_ACCESS_KEY_ID'
    aws_secret_key = 'YOUR_AWS_SECRET_ACCESS_KEY'
)
file_format = GAME_ANALYTICS.RAW.JSON_RAW_FORMAT;

copy into GAME_ANALYTICS.RAW.RAWG_GAMES_RAW (source, s3_key, extracted_at, payload)
from (
    select
        'rawg' as source,
        metadata$filename as s3_key,
        to_timestamp_ntz($1:extracted_at_utc::string) as extracted_at,
        $1 as payload
    from @GAME_ANALYTICS.RAW.GAME_ANALYTICS_RAW_STAGE/source=rawg/
)
file_format = (format_name = GAME_ANALYTICS.RAW.JSON_RAW_FORMAT)
on_error = 'ABORT_STATEMENT';

copy into GAME_ANALYTICS.RAW.STEAM_GAMES_RAW (source, s3_key, extracted_at, payload)
from (
    select
        'steam' as source,
        metadata$filename as s3_key,
        to_timestamp_ntz($1:extracted_at_utc::string) as extracted_at,
        $1 as payload
    from @GAME_ANALYTICS.RAW.GAME_ANALYTICS_RAW_STAGE/source=steam/
)
file_format = (format_name = GAME_ANALYTICS.RAW.JSON_RAW_FORMAT)
on_error = 'ABORT_STATEMENT';

with source_data as (
    select
        extracted_at,
        payload
    from {{ source('raw', 'STEAM_GAMES_RAW') }}
),

latest_extract as (
    select max(extracted_at) as extracted_at
    from source_data
),

flattened_items as (
    select
        source_data.extracted_at,
        item.value as item_json
    from source_data
    inner join latest_extract
        on source_data.extracted_at = latest_extract.extracted_at,
    lateral flatten(input => payload:payload:featured_categories) as item
)

select
    item_json:id::number as steam_app_id,
    item_json:name::string as game_name,
    item_json:type::number as item_type,
    item_json:category_key::string as category_key,
    item_json:category_name::string as category_name,
    item_json:discounted::boolean as is_discounted,
    item_json:discount_percent::number as discount_percent,
    item_json:original_price::number / 100.0 as original_price_usd,
    item_json:final_price::number / 100.0 as final_price_usd,
    item_json:currency::string as currency,
    item_json:windows_available::boolean as windows_available,
    item_json:mac_available::boolean as mac_available,
    item_json:linux_available::boolean as linux_available,
    item_json:controller_support::string as controller_support,
    item_json:header_image::string as header_image_url,
    item_json:url::string as steam_url,
    to_timestamp_ntz(item_json:discount_expiration::number) as discount_expires_at,
    extracted_at as extracted_at_utc
from flattened_items
qualify row_number() over (
    partition by coalesce(item_json:id::number, -1), item_json:category_key::string, item_json:name::string
    order by extracted_at desc
) = 1

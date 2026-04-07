select
    source,
    s3_key,
    target_table,
    rows_loaded,
    status,
    loaded_at
from {{ source('raw', 'RAW_LOAD_AUDIT') }}

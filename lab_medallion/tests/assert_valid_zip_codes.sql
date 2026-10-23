select
    user_id,
    state,
    zip_code
from {{ ref('stg_users') }}
where zip_code is null
   or not regexp_full_match(zip_code, '[0-9]{5}')
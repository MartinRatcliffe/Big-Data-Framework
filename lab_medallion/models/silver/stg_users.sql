with source as (
    select * from {{ source('bronze', 'users') }}
),

typed as (
    select
        cast(uuid as uuid) as user_id,
        trim(username) as username,
        lower(trim(username)) as username_normalized,
        trim(name) as name,
        upper(trim(sex)) as sex,
        lower(trim(mail)) as email,
        cast(birthdate as date) as birthdate,
        -- The address spans 2 lines: the street, then the city, the state and the zip code
        split_part(address, chr(10), 1) as street,
        split_part(address, chr(10), 2) as address_line_2
    from source
)

select
    u.user_id,
    u.username,
    u.username_normalized,
    u.name,
    u.sex,
    u.email,
    case
        when o.first_order is not null
         and u.birthdate > o.first_order::date
        then null
        else u.birthdate
    end as birthdate,
    not (
        o.first_order is not null
        and u.birthdate > o.first_order::date
    ) as birthdate_is_valid,
    u.street,
    nullif(regexp_extract(u.address_line_2, '^(.+), [A-Z]{2} \d{5}$', 1), '') as city,
    regexp_extract(u.address_line_2, '([A-Z]{2}) (\d{5})$', 1) as state,
    regexp_extract(u.address_line_2, '([A-Z]{2}) (\d{5})$', 2) as zip_code
from typed u
left join (
    select
        cast(user_uuid as uuid) as user_id,
        min(cast(date as timestamptz)) as first_order
    from {{ source('bronze', 'orders') }}
    group by user_id
) o on u.user_id = o.user_id

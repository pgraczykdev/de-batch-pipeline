SELECT
    user_id,
    first_name,
    last_name,
    age,
    gender,
    city,
    state,
    country
FROM {{ source('staging_dummyjson', 'users') }}
SELECT
    date_id,
    date,
    day,
    month,
    year,
    quarter
FROM {{ source('staging_dummyjson', 'dates') }}
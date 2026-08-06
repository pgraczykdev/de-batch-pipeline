SELECT
    date_id,
    TO_DATE(TO_TIMESTAMP(date / 1000)) AS date,
    day,
    month,
    year,
    quarter
FROM {{ source('staging_dummyjson', 'dates') }}
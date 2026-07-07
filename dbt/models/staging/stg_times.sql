SELECT
    time_id,
    day_name,
    day_number_in_week,
    day_number_in_month,
    calendar_week_number,
    calendar_month_number,
    calendar_month_desc,
    calendar_month_name,
    calendar_quarter_desc,
    calendar_quarter_number,
    calendar_year
FROM {{ source('staging', 'times') }}
{{ config(materialized='table') }}

SELECT 
    d.date,
    d.year,
    d.month,
    COUNT(o.cart_id) as total_orders,
    SUM(o.total) AS total_revenue
FROM 
    {{ ref('fct_orders') }} o
LEFT OUTER JOIN {{ ref('dim_dates') }} d ON o.date_id = d.date_id
GROUP BY d.date, d.year, d.month
ORDER BY d.date
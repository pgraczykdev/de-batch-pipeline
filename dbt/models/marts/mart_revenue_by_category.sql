{{ config(materialized='table') }}

SELECT 
    p.category,
    COUNT(*) as total_orders,
    SUM(o.total) AS total_revenue,
    AVG((1 - o.discounted_total / o.total) * 100) AS avg_discount_pct
FROM 
    {{ ref('fct_orders')}} o
INNER JOIN {{ ref('dim_products')}} p ON o.product_id = p.product_id
GROUP BY p.category 
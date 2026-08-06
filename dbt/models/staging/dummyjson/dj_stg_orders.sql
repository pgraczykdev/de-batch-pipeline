SELECT
    cart_id,
    user_id,
    product_id,
    TO_DATE(TO_TIMESTAMP(order_date / 1000)) AS order_date,
    quantity,
    price,
    total,
    discounted_total
FROM {{ source('staging_dummyjson', 'orders') }}
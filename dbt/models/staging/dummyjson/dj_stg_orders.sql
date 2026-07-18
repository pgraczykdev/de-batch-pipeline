SELECT
    cart_id,
    user_id,
    product_id,
    order_date,
    quantity,
    price,
    total,
    discounted_total
FROM {{ source('staging_dummyjson', 'orders') }}
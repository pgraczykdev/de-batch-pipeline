SELECT
    product_id,
    product_name,
    category,
    price,
    brand,
    rating,
    stock
FROM {{ source('staging_dummyjson', 'products') }}
SELECT
    prod_id,
    prod_name,
    prod_desc,
    prod_subcategory,
    prod_category,
    prod_category_desc,
    prod_status,
    prod_list_price,
    prod_min_price
FROM {{ source('staging', 'products') }}
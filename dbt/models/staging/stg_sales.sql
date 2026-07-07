SELECT
    prod_id,
    cust_id,
    time_id,
    channel_id,
    quantity_sold,
    amount_sold
FROM {{ source('staging', 'sales') }}
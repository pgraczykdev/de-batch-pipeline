SELECT
    cust_id,
    cust_first_name,
    cust_last_name,
    cust_gender,
    cust_year_of_birth,
    cust_marital_status,
    cust_city,
    cust_state_province,
    country_id,
    cust_income_level,
    cust_credit_limit
FROM {{ source('staging', 'customers') }}
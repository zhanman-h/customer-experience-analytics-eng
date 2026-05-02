{{ config(materialized='view') }}

WITH raw_orders AS (
    SELECT *
    FROM {{ source("raw_data", "raw_orders") }}
), 

cleaned AS (
    SELECT 
        order_id,
        COALESCE(customer_id, 'CUST501') AS customer_id,
        customer_name,
        customer_email, 
        COALESCE(product_id, 'PROD201') AS product_id,
        product_category,
        CASE 
            WHEN unit_price > 0 THEN unit_price
            ELSE 20
        END AS unit_price,
        CASE 
            WHEN quantity > 0 THEN quantity
            ELSE 1
        END AS quantity,
        country,
        order_status,
        CAST(order_date AS date) AS order_date
    FROM raw_orders
    WHERE order_id IS NOT NULL 
        AND CAST(order_date AS date) <= current_date
),

deduplicated AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (
            PARTITION BY order_id ORDER BY order_date DESC
        ) AS row_number
    FROM cleaned
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['order_id', 'order_date']) }} as order_pk,
    order_id, 
    order_status,
    order_date,
    customer_id, 
    customer_name,
    customer_email,
    country, 
    product_id,
    product_category,
    unit_price,
    quantity,
    {{ get_order_amount('unit_price', 'quantity') }} as order_amount
FROM deduplicated
WHERE row_number = 1
 


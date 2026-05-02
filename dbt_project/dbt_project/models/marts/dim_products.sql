SELECT DISTINCT
    product_id,
    product_category,
    unit_price
FROM {{ ref('stg_orders') }}
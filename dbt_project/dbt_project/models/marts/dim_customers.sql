WITH customers AS (
    SELECT DISTINCT
        customer_id,
        customer_name,
        customer_email,
        country
    FROM {{ ref('stg_orders') }}
),

revenue_summary AS (
    SELECT * FROM {{ ref('int_customer_revenue_summary') }}
)

SELECT
    c.customer_id,
    c.customer_name,
    c.customer_email,
    c.country,
    r.total_lifetime_revenue,
    r.total_order_count,
    r.avg_order_value,
    r.days_since_last_purchase,
    CASE 
        WHEN r.total_lifetime_revenue > 500 THEN 'High Value'
        WHEN r.total_lifetime_revenue BETWEEN 100 AND 500 THEN 'Mid Value'
        ELSE 'Low Value'
    END AS customer_tier
FROM customers c
LEFT JOIN revenue_summary r ON c.customer_id = r.customer_id
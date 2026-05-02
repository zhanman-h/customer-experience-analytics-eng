WITH support_csat AS (
    SELECT * FROM {{ ref('int_support_csat_joined') }}
),

customers AS (
    SELECT * FROM {{ ref('dim_customers') }}
)

SELECT
    s.support_pk,
    s.chat_id,
    s.order_id,
    c.customer_id,
    c.customer_tier,
    c.country,
    s.wait_time_minutes,
    s.is_wait_sla_breach,
    s.sentiment_category,
    s.csat_score,
    c.total_lifetime_revenue AS customer_ltv
FROM support_csat s
LEFT JOIN {{ ref('stg_orders') }} o ON s.order_id = o.order_id
LEFT JOIN customers c ON o.customer_id = c.customer_id
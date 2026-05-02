WITH mart AS (
    SELECT customer_id, max(customer_ltv) AS mart_ltv
    FROM {{ ref('fct_customer_experience') }}
    GROUP BY 1
),

summary AS (
    SELECT customer_id, total_lifetime_revenue AS summary_ltv
    FROM {{ ref('int_customer_revenue_summary') }}
)

SELECT
    m.customer_id,
    m.mart_ltv,
    s.summary_ltv
FROM mart m
JOIN summary s ON m.customer_id = s.customer_id
WHERE m.mart_ltv != s.summary_ltv
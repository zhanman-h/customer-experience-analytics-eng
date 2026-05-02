WITH orders AS (
    SELECT * FROM {{ ref('stg_orders') }}
),

revenue_agg AS (
    SELECT
        customer_id,
        count(order_id) AS total_order_count,
        sum(order_amount) AS total_lifetime_revenue,
        max(order_date) AS most_recent_purchase_date,
        (current_date - max(order_date))::integer as days_since_last_purchase,        round(avg(order_amount), 2) AS avg_order_value
    FROM orders
    GROUP BY 1
)

SELECT * FROM revenue_agg
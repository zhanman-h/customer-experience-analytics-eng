WITH experience_data AS (
    SELECT * FROM {{ ref('fct_customer_experience') }}
)

SELECT
    is_wait_sla_breach,
    sentiment_category,
    count(support_pk) AS interaction_count,
    round(sum(customer_ltv)::numeric, 2) AS total_impacted_ltv,
    round(avg(wait_time_minutes)::numeric, 2) AS avg_wait_time
FROM experience_data
GROUP BY 1, 2
ORDER BY total_impacted_ltv DESC
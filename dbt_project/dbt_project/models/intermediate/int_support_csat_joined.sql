WITH support_performance AS (
    SELECT * FROM {{ ref('int_support_performance') }}
),

csat AS (
    SELECT * FROM {{ ref('stg_csat') }}
),

joined AS (
    SELECT
        s.support_pk,
        c.csat_pk,
        s.chat_id,
        s.order_id,
        s.wait_time_minutes,
        s.session_duration_minutes,
        s.is_wait_sla_breach,
        s.is_long_wait,
        c.csat_score,
        c.is_positive_feedback,
        CASE 
            WHEN c.csat_score >= 4 THEN 'Promoter'
            WHEN c.csat_score = 3 THEN 'Passive'
            WHEN c.csat_score < 3 THEN 'Detractor'
            ELSE 'No Feedback'
        END AS sentiment_category
    FROM support_performance s
    LEFT JOIN csat c ON s.chat_id = c.chat_id
)

SELECT * FROM joined
SELECT
    support_pk,
    chat_id,
    order_id,
    wait_time_minutes,
    session_duration_minutes,
    is_wait_sla_breach,
    csat_score,
    sentiment_category
FROM {{ ref('int_support_csat_joined') }}
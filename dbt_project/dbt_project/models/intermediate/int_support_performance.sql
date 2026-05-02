WITH support AS (
    SELECT * FROM {{ ref('stg_support') }}
),

performance_calc AS (
    SELECT
        support_pk,
        chat_id,
        order_id,
        channel,
        contact_reason,
        {{ get_time_diff('chat_created_at', 'chat_updated_at', 'minute') }} as session_duration_minutes,
        ROUND(wait_time_seconds / 60.0, 2) AS wait_time_minutes,
        CASE 
            WHEN wait_time_seconds > 180 THEN true
            ELSE false
        END AS is_wait_sla_breach,
        is_long_wait
    FROM support
)

SELECT * FROM performance_calc
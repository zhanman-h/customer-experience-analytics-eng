WITH support_data AS (
    SELECT 
        o.product_id,
        s.contact_reason,
        s.chat_id,
        s.session_duration_minutes
    FROM {{ ref('fct_support_interactions') }} s
    LEFT JOIN {{ ref('stg_orders') }} o on s.order_id = o.order_id
)

SELECT
    product_id,
    contact_reason,
    count(chat_id) AS total_chats,
    round(avg(session_duration_minutes)::numeric, 2) AS avg_resolution_time_min
FROM support_data
GROUP BY 1, 2
ORDER BY total_chats DESC, avg_resolution_time_min DESC
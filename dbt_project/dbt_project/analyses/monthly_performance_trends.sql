SELECT
    date_trunc('month', chat_created_at) AS report_month,
    round(avg(csat_score)::numeric, 2) AS avg_csat,
    count(CASE WHEN is_wait_sla_breach THEN 1 END)::float / count(*) AS sla_breach_rate,
    count(support_pk) AS total_chats
FROM {{ ref('fct_support_interactions') }}
GROUP BY 1
ORDER BY 1 DESC
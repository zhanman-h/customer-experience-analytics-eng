SELECT
    support_pk,
    session_duration_minutes
FROM {{ ref('fct_support_interactions') }}
WHERE session_duration_minutes < 0
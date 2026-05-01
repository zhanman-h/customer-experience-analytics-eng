{{ config(materialized='view') }}

WITH raw_csat AS (
    SELECT *
    FROM {{ source("raw_data", "raw_csat") }}
),

cleaned AS (
    SELECT 
        chat_id,
        CASE 
            WHEN csat_score BETWEEN 1 AND 5 THEN csat_score
            WHEN csat_score < 1 AND csat_score IS NOT NULL THEN 1
            WHEN csat_score > 5 THEN 5
            ELSE NULL 
        END AS csat_score,
        is_resolved,
        CASE 
            WHEN csat_score >= 4 THEN TRUE
            ELSE FALSE
        END AS is_positive_feedback
    FROM raw_csat
),

deduplicated AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (
            PARTITION BY chat_id ORDER BY csat_score DESC
        ) AS row_number
    FROM cleaned
)

SELECT 
    chat_id,
    csat_score,
    is_resolved,
    is_positive_feedback
FROM deduplicated
WHERE row_number = 1
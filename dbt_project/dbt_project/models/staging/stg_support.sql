{{ config(materialized='view') }}

WITH raw_support AS (
    SELECT *
    FROM {{ source("raw_data", "raw_support") }}
), 

cleaned AS (
    SELECT 
        chat_id,
        COALESCE(order_id, 'ORD999') AS order_id,
        channel,
        contact_reason,
        COALESCE(wait_time, 0) AS wait_time_seconds,
        CAST(chat_created_at AS timestamp) AS chat_created_at,
        CAST(chat_updated_at AS timestamp) AS chat_updated_at, 
        CASE 
            WHEN wait_time > 300 THEN TRUE
            ELSE FALSE
        END AS is_long_wait
    FROM raw_support
    WHERE chat_id IS NOT NULL
), 

deduplicated AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (
            PARTITION BY chat_id ORDER BY chat_updated_at DESC
        ) AS row_number
    FROM cleaned
)

SELECT
    chat_id,
    order_id,
    channel, 
    contact_reason,
    wait_time_seconds, 
    chat_created_at,
    chat_updated_at,
    is_long_wait
FROM deduplicated
WHERE row_number = 1

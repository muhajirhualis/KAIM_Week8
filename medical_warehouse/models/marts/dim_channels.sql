-- models/marts/dim_channels.sql
WITH channels AS (
    SELECT DISTINCT
        channel_name,
        channel_title,
        MIN(message_date) AS first_post_date,
        MAX(message_date) AS last_post_date,
        COUNT(*) AS total_posts,
        AVG(views) AS avg_views
    FROM {{ ref('stg_telegram_messages') }}
    GROUP BY channel_name, channel_title
),
classified AS (
    SELECT *,
        CASE
            WHEN LOWER(channel_name) LIKE '%pharma%' THEN 'Pharmaceutical'
            WHEN LOWER(channel_name) LIKE '%cosmetic%' THEN 'Cosmetics'
            ELSE 'Medical'
        END AS channel_type
    FROM channels
)
SELECT
    ROW_NUMBER() OVER (ORDER BY channel_name) AS channel_key,
    channel_name,
    channel_title,
    channel_type,
    first_post_date,
    last_post_date,
    total_posts,
    avg_views
FROM classified
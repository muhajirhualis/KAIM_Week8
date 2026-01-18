-- models/marts/fct_messages.sql
SELECT
    s.message_id,
    d.channel_key,
    dt.date_key,
    s.message_text,
    s.message_length,
    s.views AS view_count,
    s.forwards AS forward_count,
    s.has_media AS has_image
FROM {{ ref('stg_telegram_messages') }} s
JOIN {{ ref('dim_channels') }} d ON s.channel_name = d.channel_name
JOIN {{ ref('dim_dates') }} dt ON s.message_date::DATE = dt.full_date
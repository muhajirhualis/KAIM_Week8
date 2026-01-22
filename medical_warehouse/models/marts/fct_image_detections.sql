-- This links YOLO results to star schema via message_id.
WITH yolo AS (
    SELECT
        message_id,
        image_category,
        max_confidence
    FROM enriched.yolo_detections
),

messages AS (
    SELECT
        message_id,
        channel_key,
        date_key
    FROM {{ ref('fct_messages') }}
)

SELECT
    y.message_id,
    m.channel_key,
    m.date_key,
    y.image_category AS detected_class,
    y.max_confidence AS confidence_score,
    y.image_category
FROM yolo y
JOIN messages m ON y.message_id = m.message_id
-- models/staging/stg_telegram_messages.sql
SELECT
    message_id::BIGINT,
    channel_name::TEXT,
    channel_title::TEXT,
    message_date::TIMESTAMP,
    message_text::TEXT,
    has_media::BOOLEAN,
    image_path::TEXT,
    views::INTEGER,
    forwards::INTEGER,
    LENGTH(message_text) AS message_length
FROM raw.telegram_messages
WHERE message_id IS NOT NULL
  AND message_date IS NOT NULL
  AND message_date <= NOW()  -- no future dates
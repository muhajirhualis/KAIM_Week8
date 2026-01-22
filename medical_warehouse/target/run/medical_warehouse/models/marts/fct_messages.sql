
  
    

  create  table "medical_telegram_db"."public"."fct_messages__dbt_tmp"
  
  
    as
  
  (
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
FROM "medical_telegram_db"."public"."stg_telegram_messages" s
JOIN "medical_telegram_db"."public"."dim_channels" d ON s.channel_name = d.channel_name
JOIN "medical_telegram_db"."public"."dim_dates" dt ON s.message_date::DATE = dt.full_date
  );
  
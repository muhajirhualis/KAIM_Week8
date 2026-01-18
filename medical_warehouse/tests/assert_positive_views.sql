SELECT *
FROM {{ ref('stg_telegram_messages') }}
WHERE views < 0
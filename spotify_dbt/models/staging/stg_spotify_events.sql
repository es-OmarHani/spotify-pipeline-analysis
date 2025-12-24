WITH source_data AS (

    SELECT
        event_id,
        user_id,
        song_id,
        artist_name,
        song_name,
        event_type,
        device_type,
        country,
        TRY_TO_TIMESTAMP_TZ(timestamp) AS event_ts
    FROM {{ source('bronze', 'spotify_bronze') }}

),

cleaned AS (

    SELECT
        event_id,
        user_id,
        song_id,
        artist_name,
        song_name,
        LOWER(event_type) AS event_type,
        LOWER(device_type) AS device_type,
        UPPER(country) AS country,
        event_ts
    FROM source_data
    WHERE event_id IS NOT NULL
      AND user_id IS NOT NULL
      AND song_id IS NOT NULL
      AND event_ts IS NOT NULL

)

SELECT *
FROM cleaned

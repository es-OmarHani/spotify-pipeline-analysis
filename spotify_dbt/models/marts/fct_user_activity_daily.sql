SELECT
    user_id,
    device_type,
    country,
    DATE_TRUNC('day', event_ts) AS activity_date,

    COUNT_IF(event_type = 'play') AS plays,
    COUNT_IF(event_type = 'skip') AS skips,
    COUNT_IF(event_type = 'add_to_playlist') AS playlist_adds

FROM {{ ref('stg_spotify_events') }}
GROUP BY
    user_id,
    device_type,
    country,
    DATE_TRUNC('day', event_ts)

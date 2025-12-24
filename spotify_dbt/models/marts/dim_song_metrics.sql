SELECT
    song_id,
    song_name,
    artist_name,

    COUNT_IF(event_type = 'play') AS total_plays,
    COUNT_IF(event_type = 'skip') AS total_skips,
    COUNT(DISTINCT user_id) AS unique_listeners

FROM {{ ref('stg_spotify_events') }}
GROUP BY
    song_id,
    song_name,
    artist_name
ORDER BY total_plays DESC

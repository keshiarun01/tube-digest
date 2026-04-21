{{
    config(
        materialized='view',
        schema='staging'
    )
}}

/*
    One row per video with summary stats.
    Used as a dimension table for joining onto chunks.
*/

SELECT
    video_id,
    ANY_VALUE(video_title)                  AS video_title,
    ANY_VALUE(channel)                      AS channel,
    ANY_VALUE(language)                     AS language,
    ANY_VALUE(is_auto_generated)            AS is_auto_generated,
    COUNT(*)                                AS segment_count,
    MIN(start_time_seconds)                 AS first_segment_start,
    MAX(end_time_seconds)                   AS video_duration_seconds,
    SUM(LENGTH(text))                       AS total_characters,
    SUM(ARRAY_SIZE(SPLIT(text, ' ')))       AS total_words,
    MAX(loaded_at)                          AS last_loaded_at

FROM {{ ref('stg_transcripts') }}
GROUP BY video_id
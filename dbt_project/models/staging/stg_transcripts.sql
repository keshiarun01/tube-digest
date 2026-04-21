{{
    config(
        materialized='view',
        schema='staging'
    )
}}

/*
    Staging model for YouTube transcripts.
    - Parses raw_metadata JSON string into VARIANT
    - Cleans text (trim whitespace, collapse internal spaces)
    - Casts types explicitly
    - Deduplicates on (video_id, segment_index)
*/

WITH source AS (

    SELECT *
    FROM {{ source('raw', 'transcripts') }}

),

deduplicated AS (

    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY video_id, segment_index
            ORDER BY loaded_at DESC
        ) AS row_num
    FROM source

),

cleaned AS (

    SELECT
        -- Identifiers
        video_id::VARCHAR(20)                           AS video_id,
        segment_index::INTEGER                          AS segment_index,

        -- Text content
        TRIM(REGEXP_REPLACE(text, '\\s+', ' '))         AS text,

        -- Timing
        start_time::FLOAT                               AS start_time_seconds,
        duration::FLOAT                                 AS duration_seconds,
        (start_time + duration)::FLOAT                  AS end_time_seconds,

        -- Video metadata
        video_title::VARCHAR                            AS video_title,
        channel::VARCHAR                                AS channel,
        language::VARCHAR(10)                           AS language,
        is_auto_generated::BOOLEAN                      AS is_auto_generated,

        -- VARIANT conversion happens here, not in RAW
        TRY_PARSE_JSON(raw_metadata)                    AS video_metadata,

        -- Audit
        loaded_at::TIMESTAMP_NTZ                        AS loaded_at

    FROM deduplicated
    WHERE row_num = 1
      AND text IS NOT NULL
      AND TRIM(text) != ''

)

SELECT * FROM cleaned
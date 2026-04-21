{{
    config(
        materialized='view',
        schema='staging'
    )
}}

/*
    Chunks transcript segments into LLM-ready blocks (~4K tokens each).

    Token estimation: ~4 characters per token (rough heuristic matching
    what the Python chunker uses). Chunks split at segment boundaries
    (never mid-sentence) and are grouped using a running cumulative sum
    that resets when the per-chunk token budget is exceeded.
*/

WITH segments AS (

    SELECT
        video_id,
        segment_index,
        text,
        start_time_seconds,
        end_time_seconds,
        duration_seconds,
        CEIL(LENGTH(text) / 4.0)::INTEGER    AS estimated_tokens
    FROM {{ ref('stg_transcripts') }}

),

running_tokens AS (

    SELECT
        *,
        SUM(estimated_tokens) OVER (
            PARTITION BY video_id
            ORDER BY segment_index
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS cumulative_tokens
    FROM segments

),

chunked AS (

    SELECT
        *,
        FLOOR(
            cumulative_tokens / {{ var('chunk_max_tokens') }}
        )::INTEGER AS chunk_index
    FROM running_tokens

),

aggregated_chunks AS (

    SELECT
        video_id,
        chunk_index,
        LISTAGG(text, ' ') WITHIN GROUP (ORDER BY segment_index)  AS chunk_text,
        MIN(segment_index)                                         AS segment_start,
        MAX(segment_index)                                         AS segment_end,
        MIN(start_time_seconds)                                    AS chunk_start_seconds,
        MAX(end_time_seconds)                                      AS chunk_end_seconds,
        COUNT(*)                                                   AS segment_count_in_chunk,
        SUM(estimated_tokens)                                      AS chunk_token_estimate
    FROM chunked
    GROUP BY video_id, chunk_index

)

SELECT
    video_id,
    chunk_index,
    chunk_text,
    segment_start,
    segment_end,
    chunk_start_seconds,
    chunk_end_seconds,
    (chunk_end_seconds - chunk_start_seconds) AS chunk_duration_seconds,
    segment_count_in_chunk,
    chunk_token_estimate,
    LENGTH(chunk_text)                         AS chunk_character_count
FROM aggregated_chunks
ORDER BY video_id, chunk_index
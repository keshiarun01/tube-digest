{{
    config(
        materialized='table',
        schema='analytics'
    )
}}

/*
    The final LLM-ready table.

    One row per (video_id, chunk_index) with all denormalized video
    metadata. This is what the OpenAI summarization layer (Phase 6)
    will read from to generate study guides and flashcards.
*/

SELECT
    -- Chunk identifiers
    c.video_id,
    c.chunk_index,
    c.video_id || '-' || LPAD(c.chunk_index::VARCHAR, 4, '0')    AS chunk_key,

    -- Chunk content
    c.chunk_text,
    c.chunk_token_estimate,
    c.chunk_character_count,
    c.segment_count_in_chunk,

    -- Chunk timing (for deep-linking back to video)
    c.chunk_start_seconds,
    c.chunk_end_seconds,
    c.chunk_duration_seconds,
    c.segment_start,
    c.segment_end,

    -- Denormalized video metadata
    v.video_title,
    v.channel,
    v.language,
    v.is_auto_generated,
    v.segment_count    AS video_segment_count,
    v.video_duration_seconds,
    v.total_words      AS video_total_words,

    -- Audit
    v.last_loaded_at,
    CURRENT_TIMESTAMP()                                           AS mart_built_at

FROM {{ ref('int_transcript_chunks') }} c
INNER JOIN {{ ref('int_video_metadata') }} v
    ON c.video_id = v.video_id
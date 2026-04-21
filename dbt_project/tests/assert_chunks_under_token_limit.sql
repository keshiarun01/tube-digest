-- Verify no chunk exceeds the configured max token limit by more than 20%.
-- The 20% slack accounts for edge cases where a single long segment
-- pushes a chunk over the boundary.

SELECT
    video_id,
    chunk_index,
    chunk_token_estimate,
    {{ var('chunk_max_tokens') }} AS configured_max
FROM {{ ref('int_transcript_chunks') }}
WHERE chunk_token_estimate > ({{ var('chunk_max_tokens') }} * 1.2)
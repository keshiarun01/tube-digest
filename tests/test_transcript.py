"""
tests/test_transcript.py

Unit tests for utils/transcript.py
Run: pytest tests/test_transcript.py -v
"""

import pytest
from unittest.mock import patch, MagicMock
from utils.transcript import (
    extract_video_id,
    chunk_transcript,
    estimate_token_count,
    transcript_result_to_dict,
    TranscriptSegment,
    TranscriptResult,
    VideoMetadata,
)


# ── extract_video_id ──────────────────────────────────────────────────────────

class TestExtractVideoId:

    def test_standard_watch_url(self):
        assert extract_video_id(
            "https://www.youtube.com/watch?v=dQw4w9WgXcQ"
        ) == "dQw4w9WgXcQ"

    def test_short_url(self):
        assert extract_video_id(
            "https://youtu.be/dQw4w9WgXcQ"
        ) == "dQw4w9WgXcQ"

    def test_shorts_url(self):
        assert extract_video_id(
            "https://www.youtube.com/shorts/dQw4w9WgXcQ"
        ) == "dQw4w9WgXcQ"

    def test_embed_url(self):
        assert extract_video_id(
            "https://www.youtube.com/embed/dQw4w9WgXcQ"
        ) == "dQw4w9WgXcQ"

    def test_raw_video_id(self):
        assert extract_video_id("dQw4w9WgXcQ") == "dQw4w9WgXcQ"

    def test_url_with_extra_params(self):
        assert extract_video_id(
            "https://www.youtube.com/watch?v=dQw4w9WgXcQ&t=42s&list=PL123"
        ) == "dQw4w9WgXcQ"

    def test_strips_whitespace(self):
        assert extract_video_id(
            "  https://youtu.be/dQw4w9WgXcQ  "
        ) == "dQw4w9WgXcQ"

    def test_invalid_url_raises(self):
        with pytest.raises(ValueError):
            extract_video_id("https://vimeo.com/123456")

    def test_empty_string_raises(self):
        with pytest.raises(ValueError):
            extract_video_id("")

    def test_none_raises(self):
        with pytest.raises((ValueError, AttributeError)):
            extract_video_id(None)


# ── estimate_token_count ──────────────────────────────────────────────────────

class TestEstimateTokenCount:

    def test_empty_string(self):
        assert estimate_token_count("") == 0

    def test_known_length(self):
        assert estimate_token_count("a" * 400) == 100

    def test_scales_linearly(self):
        assert estimate_token_count("x" * 800) == 200


# ── chunk_transcript ──────────────────────────────────────────────────────────

class TestChunkTranscript:

    def _make_segments(self, texts: list[str]) -> list[TranscriptSegment]:
        return [
            TranscriptSegment(
                text=t,
                start=float(i * 5),
                duration=5.0,
                segment_index=i,
            )
            for i, t in enumerate(texts)
        ]

    def test_empty_input_returns_empty(self):
        assert chunk_transcript([]) == []

    def test_small_input_makes_one_chunk(self):
        segs = self._make_segments(["Hello world"])
        chunks = chunk_transcript(segs, max_tokens=1000)
        assert len(chunks) == 1

    def test_chunk_has_all_required_fields(self):
        segs = self._make_segments(["Hello world"])
        chunk = chunk_transcript(segs)[0]
        for field in [
            "chunk_index", "text", "start_time",
            "end_time", "segment_start", "segment_end", "token_estimate"
        ]:
            assert field in chunk, f"Missing field: {field}"

    def test_chunk_index_is_sequential(self):
        segs = self._make_segments(["a" * 400] * 30)
        chunks = chunk_transcript(segs, max_tokens=400, overlap_tokens=0)
        for i, c in enumerate(chunks):
            assert c["chunk_index"] == i

    def test_no_chunk_exceeds_max_tokens_by_much(self):
        # Each segment is ~100 tokens; chunks should stay near the ceiling
        segs = self._make_segments(["a" * 400] * 20)
        chunks = chunk_transcript(segs, max_tokens=500, overlap_tokens=0)
        for c in chunks:
            # Allow a single segment of overage at most
            assert c["token_estimate"] <= 700

    def test_timestamps_preserved(self):
        segs = self._make_segments(["word " * 50, "word " * 50])
        chunks = chunk_transcript(segs, max_tokens=10000)
        assert chunks[0]["start_time"] == 0.0
        assert chunks[0]["end_time"] > 0.0

    def test_text_is_non_empty(self):
        segs = self._make_segments(["Hello there"] * 5)
        chunks = chunk_transcript(segs)
        for c in chunks:
            assert c["text"].strip() != ""


# ── transcript_result_to_dict ─────────────────────────────────────────────────

class TestTranscriptResultToDict:

    def _make_result(self) -> TranscriptResult:
        return TranscriptResult(
            video_id="abc123",
            metadata=VideoMetadata(
                video_id="abc123",
                title="Test Video",
                channel="Test Channel",
                thumbnail_url="https://img.youtube.com/vi/abc123/hqdefault.jpg",
                url="https://www.youtube.com/watch?v=abc123",
            ),
            segments=[
                TranscriptSegment(text="Hello", start=0.0, duration=2.0, segment_index=0)
            ],
            language="en",
            is_auto_generated=False,
            total_words=1,
            total_characters=5,
        )

    def test_returns_dict(self):
        result = self._make_result()
        assert isinstance(transcript_result_to_dict(result), dict)

    def test_top_level_keys_present(self):
        d = transcript_result_to_dict(self._make_result())
        for key in [
            "video_id", "metadata", "segments",
            "language", "is_auto_generated",
            "total_words", "total_characters"
        ]:
            assert key in d

    def test_json_serializable(self):
        import json
        d = transcript_result_to_dict(self._make_result())
        # Should not raise
        json.dumps(d)

    def test_video_id_matches(self):
        d = transcript_result_to_dict(self._make_result())
        assert d["video_id"] == "abc123"

    def test_segments_is_list_of_dicts(self):
        d = transcript_result_to_dict(self._make_result())
        assert isinstance(d["segments"], list)
        assert isinstance(d["segments"][0], dict)
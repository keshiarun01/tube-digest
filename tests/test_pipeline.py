"""
tests/test_pipeline.py

Unit tests for utils/pipeline.py (all external calls mocked).
Run: pytest tests/test_pipeline.py -v
"""

import pytest
from unittest.mock import patch, MagicMock


class TestRunFullPipeline:

    def test_skips_r2_upload_when_cached(self, monkeypatch):
        monkeypatch.setenv("SNOWFLAKE_ACCOUNT", "fake")
        monkeypatch.setenv("SNOWFLAKE_USER", "fake")
        monkeypatch.setenv("SNOWFLAKE_PASSWORD", "fake")

        with patch("utils.pipeline.extract_full_transcript") as mock_extract, \
             patch("utils.pipeline.check_video_cached", return_value=True), \
             patch("utils.pipeline.upload_raw_transcript") as mock_upload_t, \
             patch("utils.pipeline.upload_raw_chunks") as mock_upload_c, \
             patch("utils.pipeline.check_video_in_snowflake", return_value=True), \
             patch("utils.pipeline.load_transcript_from_r2"), \
             patch("utils.pipeline.run_dbt_build", return_value=(True, "OK")), \
             patch("utils.pipeline.execute_query", return_value=[{"CNT": 10}]), \
             patch("utils.pipeline.process_video_from_mart") as mock_llm:

            mock_result = MagicMock()
            mock_result.video_id = "abc123"
            mock_result.metadata.title = "Test Video"
            mock_result.segments = [MagicMock() for _ in range(5)]
            mock_extract.return_value = (mock_result, [{"chunk_text": "x"}])

            mock_llm.return_value = {
                "video_id": "abc123",
                "video_title": "Test Video",
                "study_guide": "# Guide",
                "flashcards": [],
                "practice_questions": [],
                "chunks": [],
                "total_cost_usd": 0.05,
                "usage_by_step": {},
            }

            from utils.pipeline import run_full_pipeline
            result = run_full_pipeline("https://youtube.com/watch?v=abc123")

            # R2 upload was skipped because cached
            mock_upload_t.assert_not_called()
            mock_upload_c.assert_not_called()
            assert result["cached"] is True

    def test_raises_when_mart_is_empty(self, monkeypatch):
        monkeypatch.setenv("SNOWFLAKE_ACCOUNT", "fake")
        monkeypatch.setenv("SNOWFLAKE_USER", "fake")
        monkeypatch.setenv("SNOWFLAKE_PASSWORD", "fake")

        with patch("utils.pipeline.extract_full_transcript") as mock_extract, \
             patch("utils.pipeline.check_video_cached", return_value=True), \
             patch("utils.pipeline.check_video_in_snowflake", return_value=True), \
             patch("utils.pipeline.run_dbt_build", return_value=(True, "OK")), \
             patch("utils.pipeline.execute_query", return_value=[{"CNT": 0}]):

            mock_result = MagicMock()
            mock_result.video_id = "abc123"
            mock_result.metadata.title = "Test Video"
            mock_result.segments = []
            mock_extract.return_value = (mock_result, [])

            from utils.pipeline import run_full_pipeline
            with pytest.raises(RuntimeError, match="not found in mart"):
                run_full_pipeline("https://youtube.com/watch?v=abc123")


class TestRunDbtBuild:

    def test_returns_failure_when_dbt_not_found(self):
        with patch("utils.pipeline.subprocess.run", side_effect=FileNotFoundError):
            from utils.pipeline import run_dbt_build
            success, output = run_dbt_build()
            assert success is False
            assert "not found" in output.lower()
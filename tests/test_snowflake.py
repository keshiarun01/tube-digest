"""
tests/test_snowflake.py

Unit tests for utils/snowflake_loader.py (all Snowflake calls mocked).
Run: pytest tests/test_snowflake.py -v
"""

import pytest
from unittest.mock import patch, MagicMock


@pytest.fixture
def snowflake_env(monkeypatch):
    """Set fake Snowflake env vars for tests."""
    monkeypatch.setenv("SNOWFLAKE_ACCOUNT", "fake_account")
    monkeypatch.setenv("SNOWFLAKE_USER", "fake_user")
    monkeypatch.setenv("SNOWFLAKE_PASSWORD", "fake_password")
    monkeypatch.setenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH")
    monkeypatch.setenv("SNOWFLAKE_DATABASE", "TUBE_DIGEST")
    monkeypatch.setenv("SNOWFLAKE_SCHEMA", "RAW")
    monkeypatch.setenv("SNOWFLAKE_ROLE", "SYSADMIN")


class TestGetConnection:

    def test_raises_on_missing_credentials(self, monkeypatch):
        monkeypatch.delenv("SNOWFLAKE_ACCOUNT", raising=False)
        monkeypatch.delenv("SNOWFLAKE_USER", raising=False)
        monkeypatch.delenv("SNOWFLAKE_PASSWORD", raising=False)
        from utils.snowflake_loader import get_snowflake_connection
        with pytest.raises(EnvironmentError):
            get_snowflake_connection()

    def test_calls_snowflake_connector_with_env(self, snowflake_env):
        with patch("utils.snowflake_loader.snowflake.connector.connect") as mock_connect:
            from utils.snowflake_loader import get_snowflake_connection
            get_snowflake_connection()
            mock_connect.assert_called_once()
            kwargs = mock_connect.call_args.kwargs
            assert kwargs["account"] == "fake_account"
            assert kwargs["user"] == "fake_user"
            assert kwargs["warehouse"] == "COMPUTE_WH"


class TestCheckVideoInSnowflake:

    def test_returns_true_when_rows_exist(self, snowflake_env):
        with patch("utils.snowflake_loader.get_snowflake_connection") as mock_conn_fn:
            mock_conn = MagicMock()
            mock_cur = MagicMock()
            mock_cur.fetchall.return_value = [{"CNT": 150}]
            mock_conn.cursor.return_value = mock_cur
            mock_conn_fn.return_value = mock_conn

            from utils.snowflake_loader import check_video_in_snowflake
            assert check_video_in_snowflake("abc123") is True

    def test_returns_false_when_no_rows(self, snowflake_env):
        with patch("utils.snowflake_loader.get_snowflake_connection") as mock_conn_fn:
            mock_conn = MagicMock()
            mock_cur = MagicMock()
            mock_cur.fetchall.return_value = [{"CNT": 0}]
            mock_conn.cursor.return_value = mock_cur
            mock_conn_fn.return_value = mock_conn

            from utils.snowflake_loader import check_video_in_snowflake
            assert check_video_in_snowflake("nonexistent") is False


class TestInsertTranscriptSegments:

    def test_empty_segments_returns_zero(self, snowflake_env):
        from utils.snowflake_loader import insert_transcript_segments
        result = insert_transcript_segments(
            video_id="abc",
            segments=[],
            metadata={"title": "X", "channel": "Y"},
            language="en",
            is_auto_generated=False,
        )
        assert result == 0

    def test_calls_executemany_with_correct_data(self, snowflake_env):
        with patch("utils.snowflake_loader.get_snowflake_connection") as mock_conn_fn:
            mock_conn = MagicMock()
            mock_cur = MagicMock()
            mock_conn.cursor.return_value = mock_cur
            mock_conn_fn.return_value = mock_conn

            from utils.snowflake_loader import insert_transcript_segments
            segments = [
                {"segment_index": 0, "text": "Hello", "start": 0.0, "duration": 2.0},
                {"segment_index": 1, "text": "World", "start": 2.0, "duration": 2.0},
            ]
            result = insert_transcript_segments(
                video_id="abc123",
                segments=segments,
                metadata={"title": "Test", "channel": "TestChan"},
                language="en",
                is_auto_generated=False,
            )
            assert result == 2
            mock_cur.executemany.assert_called_once()
            mock_conn.commit.assert_called_once()

    def test_batches_large_inserts(self, snowflake_env):
        with patch("utils.snowflake_loader.get_snowflake_connection") as mock_conn_fn:
            mock_conn = MagicMock()
            mock_cur = MagicMock()
            mock_conn.cursor.return_value = mock_cur
            mock_conn_fn.return_value = mock_conn

            from utils.snowflake_loader import insert_transcript_segments
            # 1200 segments with batch_size=500 should trigger 3 executemany calls
            segments = [
                {"segment_index": i, "text": f"seg {i}", "start": i * 1.0, "duration": 1.0}
                for i in range(1200)
            ]
            insert_transcript_segments(
                video_id="big_video",
                segments=segments,
                metadata={"title": "Big", "channel": "BigChan"},
                language="en",
                is_auto_generated=True,
                batch_size=500,
            )
            assert mock_cur.executemany.call_count == 3


class TestLoadFromR2:

    def test_skips_if_already_loaded(self, snowflake_env):
        with patch("utils.snowflake_loader.check_video_in_snowflake", return_value=True), \
             patch("utils.r2.download_json") as mock_download:

            from utils.snowflake_loader import load_transcript_from_r2
            result = load_transcript_from_r2("abc123", force_reload=False)
            assert result == 0
            mock_download.assert_not_called()

    def test_force_reload_deletes_and_reinserts(self, snowflake_env):
        with patch("utils.snowflake_loader.check_video_in_snowflake", return_value=True), \
             patch("utils.snowflake_loader.delete_video_from_snowflake") as mock_delete, \
             patch("utils.r2.download_json") as mock_download, \
             patch("utils.snowflake_loader.insert_transcript_segments", return_value=2):

            mock_download.return_value = {
                "segments": [{"segment_index": 0, "text": "a", "start": 0.0, "duration": 1.0}],
                "metadata": {"title": "X", "channel": "Y"},
                "language": "en",
                "is_auto_generated": False,
            }

            from utils.snowflake_loader import load_transcript_from_r2
            result = load_transcript_from_r2("abc123", force_reload=True)
            mock_delete.assert_called_once_with("abc123")
            assert result == 2
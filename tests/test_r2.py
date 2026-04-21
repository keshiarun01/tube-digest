"""
tests/test_r2.py

Unit tests for utils/r2.py (all boto3 calls mocked).
Run: pytest tests/test_r2.py -v
"""

import pytest
import json
from unittest.mock import patch, MagicMock
from botocore.exceptions import ClientError


# ── Env var helpers ───────────────────────────────────────────────────────────

@pytest.fixture
def r2_env(monkeypatch):
    """Set fake R2 env vars for tests."""
    monkeypatch.setenv("R2_ACCOUNT_ID", "fake_account_123")
    monkeypatch.setenv("R2_ACCESS_KEY_ID", "fake_access_key")
    monkeypatch.setenv("R2_SECRET_ACCESS_KEY", "fake_secret_key")
    monkeypatch.setenv("R2_BUCKET_NAME", "test-bucket")


# ── get_bucket_name ───────────────────────────────────────────────────────────

class TestGetBucketName:

    def test_returns_bucket_from_env(self, r2_env):
        from utils.r2 import get_bucket_name
        assert get_bucket_name() == "test-bucket"

    def test_raises_if_missing(self, monkeypatch):
        monkeypatch.delenv("R2_BUCKET_NAME", raising=False)
        from utils.r2 import get_bucket_name
        with pytest.raises(EnvironmentError):
            get_bucket_name()


# ── get_r2_client ─────────────────────────────────────────────────────────────

class TestGetR2Client:

    def test_uses_cloudflare_endpoint(self, r2_env):
        with patch("utils.r2.boto3.client") as mock_client:
            from utils.r2 import get_r2_client
            get_r2_client()
            call_kwargs = mock_client.call_args.kwargs
            assert "r2.cloudflarestorage.com" in call_kwargs["endpoint_url"]
            assert "fake_account_123" in call_kwargs["endpoint_url"]

    def test_raises_on_missing_credentials(self, monkeypatch):
        monkeypatch.delenv("R2_ACCOUNT_ID", raising=False)
        monkeypatch.delenv("R2_ACCESS_KEY_ID", raising=False)
        monkeypatch.delenv("R2_SECRET_ACCESS_KEY", raising=False)
        from utils.r2 import get_r2_client
        with pytest.raises(EnvironmentError):
            get_r2_client()


# ── upload_json ───────────────────────────────────────────────────────────────

class TestUploadJson:

    def test_uploads_with_correct_key(self, r2_env):
        with patch("utils.r2.boto3.client") as mock_client:
            mock_s3 = MagicMock()
            mock_client.return_value = mock_s3

            from utils.r2 import upload_json
            uri = upload_json({"hello": "world"}, "test/key.json")

            mock_s3.put_object.assert_called_once()
            call_kwargs = mock_s3.put_object.call_args.kwargs
            assert call_kwargs["Bucket"] == "test-bucket"
            assert call_kwargs["Key"] == "test/key.json"
            assert call_kwargs["ContentType"] == "application/json"
            assert "r2://test-bucket/test/key.json" == uri

    def test_serializes_dict_to_json(self, r2_env):
        with patch("utils.r2.boto3.client") as mock_client:
            mock_s3 = MagicMock()
            mock_client.return_value = mock_s3

            from utils.r2 import upload_json
            upload_json({"a": 1, "b": [1, 2, 3]}, "test.json")

            body = mock_s3.put_object.call_args.kwargs["Body"]
            parsed = json.loads(body.decode("utf-8"))
            assert parsed == {"a": 1, "b": [1, 2, 3]}


# ── object_exists ─────────────────────────────────────────────────────────────

class TestObjectExists:

    def test_returns_true_when_object_exists(self, r2_env):
        with patch("utils.r2.boto3.client") as mock_client:
            mock_s3 = MagicMock()
            mock_client.return_value = mock_s3
            mock_s3.head_object.return_value = {}

            from utils.r2 import object_exists
            assert object_exists("some/key") is True

    def test_returns_false_on_404(self, r2_env):
        with patch("utils.r2.boto3.client") as mock_client:
            mock_s3 = MagicMock()
            mock_client.return_value = mock_s3
            mock_s3.head_object.side_effect = ClientError(
                {"Error": {"Code": "404", "Message": "Not Found"}},
                "HeadObject",
            )

            from utils.r2 import object_exists
            assert object_exists("missing/key") is False


# ── TubeDigest-specific helpers ───────────────────────────────────────────────

class TestRawTranscriptHelpers:

    def test_upload_raw_transcript_uses_correct_key(self, r2_env):
        with patch("utils.r2.boto3.client") as mock_client:
            mock_s3 = MagicMock()
            mock_client.return_value = mock_s3

            from utils.r2 import upload_raw_transcript
            uri = upload_raw_transcript("abc123", {"video_id": "abc123"})

            call_kwargs = mock_s3.put_object.call_args.kwargs
            assert call_kwargs["Key"] == "raw/abc123/transcript.json"
            assert "raw/abc123/transcript.json" in uri

    def test_upload_raw_chunks_wraps_data(self, r2_env):
        with patch("utils.r2.boto3.client") as mock_client:
            mock_s3 = MagicMock()
            mock_client.return_value = mock_s3

            from utils.r2 import upload_raw_chunks
            chunks = [{"chunk_index": 0, "text": "hi"}, {"chunk_index": 1, "text": "bye"}]
            upload_raw_chunks("abc123", chunks)

            body = mock_s3.put_object.call_args.kwargs["Body"]
            parsed = json.loads(body.decode("utf-8"))
            assert parsed["video_id"] == "abc123"
            assert parsed["chunk_count"] == 2
            assert len(parsed["chunks"]) == 2

    def test_check_video_cached_calls_head_object(self, r2_env):
        with patch("utils.r2.boto3.client") as mock_client:
            mock_s3 = MagicMock()
            mock_client.return_value = mock_s3
            mock_s3.head_object.return_value = {}

            from utils.r2 import check_video_cached
            assert check_video_cached("abc123") is True
            call_kwargs = mock_s3.head_object.call_args.kwargs
            assert call_kwargs["Key"] == "raw/abc123/transcript.json"
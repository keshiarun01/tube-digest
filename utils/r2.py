"""
utils/r2.py

Cloudflare R2 storage helpers for TubeDigest.
R2 is S3-compatible, so we use boto3 with a custom endpoint_url.

Bucket layout:
    raw/{video_id}/transcript.json        full transcript + metadata
    raw/{video_id}/chunks.json            chunked for LLM processing
    processed/{video_id}/summary.md       generated study guide
    processed/{video_id}/flashcards.json  generated flashcards
    processed/{video_id}/practice.json    generated practice questions
"""

import os
import json
import logging
from datetime import datetime, timezone
from typing import Optional

import boto3
from botocore.exceptions import ClientError, NoCredentialsError
from botocore.config import Config

logger = logging.getLogger(__name__)


# ── Client Factory ────────────────────────────────────────────────────────────

def get_r2_client():
    """
    Create and return a boto3 S3 client configured for Cloudflare R2.

    Reads from env vars:
        R2_ACCOUNT_ID, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY

    Returns:
        boto3 S3 client pointed at the Cloudflare R2 endpoint.

    Raises:
        EnvironmentError: If any required credential is missing.
    """
    account_id = os.getenv("R2_ACCOUNT_ID")
    access_key = os.getenv("R2_ACCESS_KEY_ID")
    secret_key = os.getenv("R2_SECRET_ACCESS_KEY")

    if not all([account_id, access_key, secret_key]):
        raise EnvironmentError(
            "R2 credentials missing. Set R2_ACCOUNT_ID, "
            "R2_ACCESS_KEY_ID, and R2_SECRET_ACCESS_KEY in your .env file."
        )

    endpoint_url = os.getenv("END_POINT_URL") or f"https://{account_id}.r2.cloudflarestorage.com"

    # Cloudflare R2 requires specific signature version
    client = boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name="auto",  # R2 uses 'auto' as the region
        config=Config(signature_version="s3v4"),
    )
    return client


def get_bucket_name() -> str:
    """
    Return the R2 bucket name from the environment.

    Returns:
        Bucket name string.

    Raises:
        EnvironmentError: If R2_BUCKET_NAME is not set.
    """
    bucket = os.getenv("R2_BUCKET_NAME")
    if not bucket:
        raise EnvironmentError(
            "R2_BUCKET_NAME not set in environment variables."
        )
    return bucket


# ── Core Upload / Download ────────────────────────────────────────────────────

def upload_json(data: dict, key: str, metadata: dict = None) -> str:
    """
    Upload a Python dict as a JSON file to R2.

    Args:
        data: Dictionary to serialize.
        key: Full object key (e.g. 'raw/abc123/transcript.json').
        metadata: Optional R2 object metadata tags.

    Returns:
        Full URI in the form 'r2://bucket/key'.

    Raises:
        ClientError: If upload fails.
    """
    client = get_r2_client()
    bucket = get_bucket_name()

    body = json.dumps(data, indent=2, ensure_ascii=False, default=str).encode("utf-8")

    extra_metadata = {
        "uploaded_at": datetime.now(timezone.utc).isoformat(),
        **(metadata or {}),
    }

    try:
        client.put_object(
            Bucket=bucket,
            Key=key,
            Body=body,
            ContentType="application/json",
            Metadata=extra_metadata,
        )
        uri = f"r2://{bucket}/{key}"
        logger.info(f"Uploaded JSON → {uri} ({len(body):,} bytes)")
        return uri
    except ClientError as e:
        logger.error(f"R2 upload failed for key '{key}': {e}")
        raise


def upload_text(content: str, key: str, content_type: str = "text/plain") -> str:
    """
    Upload a text string (Markdown, CSV, plaintext) to R2.

    Args:
        content: Text content to upload.
        key: Full object key.
        content_type: MIME type (default: text/plain).

    Returns:
        R2 URI string.
    """
    client = get_r2_client()
    bucket = get_bucket_name()

    try:
        client.put_object(
            Bucket=bucket,
            Key=key,
            Body=content.encode("utf-8"),
            ContentType=content_type,
            Metadata={"uploaded_at": datetime.now(timezone.utc).isoformat()},
        )
        uri = f"r2://{bucket}/{key}"
        logger.info(f"Uploaded text → {uri} ({len(content):,} chars)")
        return uri
    except ClientError as e:
        logger.error(f"R2 upload failed for key '{key}': {e}")
        raise


def download_json(key: str) -> dict:
    """
    Download and parse a JSON file from R2.

    Args:
        key: Full object key.

    Returns:
        Parsed dict.

    Raises:
        FileNotFoundError: If the object does not exist.
        ClientError: On other R2 errors.
    """
    client = get_r2_client()
    bucket = get_bucket_name()

    try:
        response = client.get_object(Bucket=bucket, Key=key)
        content = response["Body"].read().decode("utf-8")
        logger.info(f"Downloaded JSON ← r2://{bucket}/{key}")
        return json.loads(content)
    except ClientError as e:
        if e.response["Error"]["Code"] in ("NoSuchKey", "404"):
            raise FileNotFoundError(f"R2 object not found: {key}")
        raise


def object_exists(key: str) -> bool:
    """
    Check if an R2 object exists without downloading it.

    Args:
        key: Full object key.

    Returns:
        True if object exists, False otherwise.
    """
    client = get_r2_client()
    bucket = get_bucket_name()

    try:
        client.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] in ("404", "NoSuchKey"):
            return False
        raise


def list_objects(prefix: str) -> list[str]:
    """
    List all object keys under a given prefix.

    Args:
        prefix: Key prefix (e.g. 'raw/' or 'processed/abc123/').

    Returns:
        List of full object keys.
    """
    client = get_r2_client()
    bucket = get_bucket_name()

    keys = []
    paginator = client.get_paginator("list_objects_v2")

    try:
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                keys.append(obj["Key"])
    except ClientError as e:
        logger.error(f"Error listing objects with prefix '{prefix}': {e}")
        raise

    logger.debug(f"Found {len(keys)} objects under prefix '{prefix}'")
    return keys


def delete_object(key: str) -> bool:
    """
    Delete a single object from R2.

    Args:
        key: Full object key.

    Returns:
        True if delete succeeded.
    """
    client = get_r2_client()
    bucket = get_bucket_name()

    try:
        client.delete_object(Bucket=bucket, Key=key)
        logger.info(f"Deleted r2://{bucket}/{key}")
        return True
    except ClientError as e:
        logger.error(f"Failed to delete '{key}': {e}")
        raise


# ── TubeDigest-Specific Helpers ───────────────────────────────────────────────

def upload_raw_transcript(video_id: str, transcript_data: dict) -> str:
    """
    Upload full raw transcript to raw/{video_id}/transcript.json.

    Args:
        video_id: YouTube video ID.
        transcript_data: Dict from transcript_result_to_dict().

    Returns:
        R2 URI.
    """
    key = f"raw/{video_id}/transcript.json"
    return upload_json(
        transcript_data,
        key,
        metadata={"video_id": video_id, "stage": "raw"},
    )


def upload_raw_chunks(video_id: str, chunks: list[dict]) -> str:
    """
    Upload chunked transcript to raw/{video_id}/chunks.json.

    Args:
        video_id: YouTube video ID.
        chunks: List of chunk dicts from chunk_transcript().

    Returns:
        R2 URI.
    """
    key = f"raw/{video_id}/chunks.json"
    payload = {
        "video_id": video_id,
        "chunk_count": len(chunks),
        "chunks": chunks,
    }
    return upload_json(
        payload,
        key,
        metadata={"video_id": video_id, "stage": "raw"},
    )


def download_raw_chunks(video_id: str) -> list[dict]:
    """
    Download chunks for a previously-processed video.

    Args:
        video_id: YouTube video ID.

    Returns:
        List of chunk dicts.
    """
    key = f"raw/{video_id}/chunks.json"
    data = download_json(key)
    return data.get("chunks", [])


def check_video_cached(video_id: str) -> bool:
    """
    Check if a video has already been extracted and stored.
    Used to skip re-processing on repeat requests.

    Args:
        video_id: YouTube video ID.

    Returns:
        True if raw/{video_id}/transcript.json exists.
    """
    return object_exists(f"raw/{video_id}/transcript.json")


def verify_r2_connection() -> bool:
    """
    Verify R2 credentials and bucket access at startup.

    Returns:
        True if connection works.

    Raises:
        Exception with descriptive message on failure.
    """
    try:
        client = get_r2_client()
        bucket = get_bucket_name()
        client.head_bucket(Bucket=bucket)
        logger.info(f"R2 connection OK — bucket '{bucket}' is accessible")
        return True
    except NoCredentialsError:
        raise Exception(
            "R2 credentials are invalid. Check your .env file."
        )
    except ClientError as e:
        code = e.response["Error"]["Code"]
        if code in ("404", "NoSuchBucket"):
            raise Exception(
                f"R2 bucket '{get_bucket_name()}' does not exist. "
                "Create it in the Cloudflare dashboard first."
            )
        if code in ("403", "AccessDenied"):
            raise Exception(
                f"Access denied to bucket '{get_bucket_name()}'. "
                "Check your API token permissions (needs Object Read & Write)."
            )
        raise Exception(f"R2 connection failed: {e}")
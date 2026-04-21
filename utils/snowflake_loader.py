"""
utils/snowflake_loader.py

Snowflake ingestion layer for TubeDigest.
Loads transcript data from Python dicts into RAW.TRANSCRIPTS.

For this free-tier project we use direct Python insertion via the
snowflake-connector rather than COPY INTO from an external stage —
this keeps setup simple and avoids needing a Snowflake storage
integration for Cloudflare R2 (which requires paid features).

Phase 5 (dbt) will read from RAW.TRANSCRIPTS and build the
staging → intermediate → marts layers.
"""

import os
import json
import logging
from contextlib import contextmanager
from typing import Optional

import snowflake.connector
from snowflake.connector import DictCursor
from snowflake.connector.errors import DatabaseError, ProgrammingError

logger = logging.getLogger(__name__)


# ── Connection Management ─────────────────────────────────────────────────────

def get_snowflake_connection():
    """
    Create a Snowflake connection using environment variables.

    Reads from: SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD,
                SNOWFLAKE_WAREHOUSE, SNOWFLAKE_DATABASE, SNOWFLAKE_SCHEMA,
                SNOWFLAKE_ROLE

    Returns:
        SnowflakeConnection instance.

    Raises:
        EnvironmentError: If required credentials are missing.
        DatabaseError: If authentication fails.
    """
    account = os.getenv("SNOWFLAKE_ACCOUNT")
    user = os.getenv("SNOWFLAKE_USER")
    password = os.getenv("SNOWFLAKE_PASSWORD")
    warehouse = os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH")
    database = os.getenv("SNOWFLAKE_DATABASE", "TUBE_DIGEST")
    schema = os.getenv("SNOWFLAKE_SCHEMA", "RAW")
    role = os.getenv("SNOWFLAKE_ROLE", "SYSADMIN")

    if not all([account, user, password]):
        raise EnvironmentError(
            "Snowflake credentials missing. Set SNOWFLAKE_ACCOUNT, "
            "SNOWFLAKE_USER, and SNOWFLAKE_PASSWORD in your .env file."
        )

    try:
        conn = snowflake.connector.connect(
            account=account,
            user=user,
            password=password,
            warehouse=warehouse,
            database=database,
            schema=schema,
            role=role,
            client_session_keep_alive=False,
            login_timeout=30,
        )
        logger.info(
            f"Connected to Snowflake — account: {account}, "
            f"warehouse: {warehouse}, db: {database}.{schema}"
        )
        return conn
    except DatabaseError as e:
        logger.error(f"Snowflake auth failed: {e}")
        raise


@contextmanager
def snowflake_session():
    """
    Context manager for Snowflake connections — ensures auto-close.

    Usage:
        with snowflake_session() as conn:
            cur = conn.cursor()
            cur.execute("SELECT 1")
    """
    conn = get_snowflake_connection()
    try:
        yield conn
    finally:
        conn.close()
        logger.debug("Snowflake connection closed")


# ── Query Execution ───────────────────────────────────────────────────────────

def execute_query(sql: str, params: tuple = None) -> list[dict]:
    """
    Execute a single SELECT query and return rows as dicts.

    Args:
        sql: SQL query string (use %s for parameter placeholders).
        params: Optional tuple of query parameters.

    Returns:
        List of row dicts.
    """
    with snowflake_session() as conn:
        cur = conn.cursor(DictCursor)
        try:
            cur.execute(sql, params) if params else cur.execute(sql)
            rows = cur.fetchall()
            logger.debug(f"Query returned {len(rows)} rows")
            return rows
        finally:
            cur.close()


def execute_non_query(sql: str, params: tuple = None) -> int:
    """
    Execute an INSERT/UPDATE/DELETE/DDL statement.

    Args:
        sql: SQL statement.
        params: Optional parameters.

    Returns:
        Number of rows affected.
    """
    with snowflake_session() as conn:
        cur = conn.cursor()
        try:
            cur.execute(sql, params) if params else cur.execute(sql)
            affected = cur.rowcount
            conn.commit()
            logger.debug(f"Non-query affected {affected} rows")
            return affected
        finally:
            cur.close()


# ── Transcript Loading ────────────────────────────────────────────────────────

def check_video_in_snowflake(video_id: str) -> bool:
    """
    Check if a video has already been loaded into RAW.TRANSCRIPTS.
    Used for idempotency — skips duplicate loads.

    Args:
        video_id: YouTube video ID.

    Returns:
        True if at least one row exists for this video_id.
    """
    sql = """
        SELECT COUNT(*) AS cnt
        FROM RAW.TRANSCRIPTS
        WHERE video_id = %s
    """
    rows = execute_query(sql, (video_id,))
    count = rows[0]["CNT"] if rows else 0
    exists = count > 0
    logger.info(f"Video '{video_id}' in Snowflake: {exists} ({count} segments)")
    return exists


def delete_video_from_snowflake(video_id: str) -> int:
    """
    Delete all rows for a given video_id. Useful for re-loading.

    Args:
        video_id: YouTube video ID.

    Returns:
        Number of rows deleted.
    """
    sql = "DELETE FROM RAW.TRANSCRIPTS WHERE video_id = %s"
    deleted = execute_non_query(sql, (video_id,))
    logger.info(f"Deleted {deleted} segments for video '{video_id}'")
    return deleted


def insert_transcript_segments(
    video_id: str,
    segments: list[dict],
    metadata: dict,
    language: str,
    is_auto_generated: bool,
    batch_size: int = 500,
) -> int:
    """
    Bulk insert transcript segments into RAW.TRANSCRIPTS.

    Uses plain VALUES clause (no PARSE_JSON) to avoid the known
    snowflake-connector-python executemany bug with SELECT-style inserts
    (GitHub issue #1770). The raw_metadata column stays as a plain JSON
    string in RAW; dbt staging parses it into VARIANT in Phase 5.

    Args:
        video_id: YouTube video ID.
        segments: List of segment dicts with keys: text, start, duration, segment_index.
        metadata: Video metadata dict with keys: title, channel.
        language: Transcript language code.
        is_auto_generated: Whether transcript is auto-generated.
        batch_size: Rows per batch (default 500).

    Returns:
        Total number of rows inserted.
    """
    if not segments:
        logger.warning(f"No segments to insert for video '{video_id}'")
        return 0

    video_title = metadata.get("title", "Unknown")
    channel = metadata.get("channel", "Unknown")
    raw_metadata_json = json.dumps(metadata)

    # Plain VALUES clause — executemany can rewrite this correctly
    sql = """
        INSERT INTO RAW.TRANSCRIPTS (
            video_id, segment_index, text, start_time, duration,
            video_title, channel, language, is_auto_generated, raw_metadata
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    rows_to_insert = [
        (
            video_id,
            seg["segment_index"],
            seg["text"],
            seg["start"],
            seg["duration"],
            video_title,
            channel,
            language,
            is_auto_generated,
            raw_metadata_json,
        )
        for seg in segments
    ]

    total_inserted = 0
    with snowflake_session() as conn:
        cur = conn.cursor()
        try:
            for i in range(0, len(rows_to_insert), batch_size):
                batch = rows_to_insert[i : i + batch_size]
                cur.executemany(sql, batch)
                total_inserted += len(batch)
                logger.info(
                    f"Inserted batch {i // batch_size + 1}: "
                    f"{total_inserted}/{len(rows_to_insert)} rows"
                )
            conn.commit()
        except ProgrammingError as e:
            conn.rollback()
            logger.error(f"Insert failed, rolled back: {e}")
            raise
        finally:
            cur.close()

    logger.info(f"Inserted {total_inserted} segments for video '{video_id}'")
    return total_inserted


def load_transcript_from_r2(video_id: str, force_reload: bool = False) -> int:
    """
    End-to-end loader: R2 → Snowflake for a single video.

    Downloads the raw transcript JSON from R2 and inserts segments
    into RAW.TRANSCRIPTS. Skips if already loaded (unless force_reload=True).

    Args:
        video_id: YouTube video ID.
        force_reload: If True, delete existing rows and reload.

    Returns:
        Number of rows inserted (0 if skipped).
    """
    # Imported here to avoid circular import at module load time
    from utils.r2 import download_json

    if check_video_in_snowflake(video_id):
        if not force_reload:
            logger.info(f"Video '{video_id}' already loaded. Skipping.")
            return 0
        logger.info(f"Force-reload: clearing existing data for '{video_id}'")
        delete_video_from_snowflake(video_id)

    logger.info(f"Downloading transcript from R2 for video '{video_id}'")
    transcript_data = download_json(f"raw/{video_id}/transcript.json")

    segments = transcript_data.get("segments", [])
    metadata = transcript_data.get("metadata", {})
    language = transcript_data.get("language", "en")
    is_auto = transcript_data.get("is_auto_generated", False)

    rows = insert_transcript_segments(
        video_id=video_id,
        segments=segments,
        metadata=metadata,
        language=language,
        is_auto_generated=is_auto,
    )
    return rows


# ── Diagnostics ───────────────────────────────────────────────────────────────

def verify_snowflake_connection() -> bool:
    """
    Verify Snowflake credentials, warehouse access, and target table exists.

    Returns:
        True if everything is OK.

    Raises:
        Exception with descriptive message on failure.
    """
    try:
        rows = execute_query("SELECT CURRENT_VERSION() AS version")
        version = rows[0]["VERSION"]
        logger.info(f"Snowflake connected — version: {version}")

        # Verify target table exists
        check_sql = """
            SELECT COUNT(*) AS cnt
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = 'RAW' AND TABLE_NAME = 'TRANSCRIPTS'
        """
        rows = execute_query(check_sql)
        if rows[0]["CNT"] == 0:
            raise Exception(
                "Table RAW.TRANSCRIPTS does not exist. "
                "Run the CREATE TABLE SQL from Phase 4 Step 3 first."
            )
        logger.info("Target table RAW.TRANSCRIPTS exists ✓")
        return True
    except DatabaseError as e:
        raise Exception(f"Snowflake connection failed: {e}")


def get_video_stats(video_id: str) -> dict:
    """
    Return summary stats for a loaded video.

    Args:
        video_id: YouTube video ID.

    Returns:
        Dict with segment_count, total_duration, video_title, channel.
    """
    sql = """
        SELECT
            COUNT(*)                  AS segment_count,
            MAX(start_time + duration) AS total_duration_seconds,
            ANY_VALUE(video_title)     AS video_title,
            ANY_VALUE(channel)         AS channel,
            ANY_VALUE(language)        AS language
        FROM RAW.TRANSCRIPTS
        WHERE video_id = %s
    """
    rows = execute_query(sql, (video_id,))
    return rows[0] if rows else {}
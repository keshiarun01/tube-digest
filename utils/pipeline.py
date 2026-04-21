"""
utils/pipeline.py

End-to-end orchestration: URL → R2 → Snowflake → dbt → OpenAI → results.

This is the single entry point the Streamlit UI calls.
"""

import logging
import subprocess
from pathlib import Path
from typing import Callable, Optional

from utils.transcript import extract_full_transcript, transcript_result_to_dict
from utils.r2 import (
    upload_raw_transcript,
    upload_raw_chunks,
    check_video_cached,
)
from utils.snowflake_loader import (
    load_transcript_from_r2,
    check_video_in_snowflake,
    execute_query,
)
from utils.summarizer import process_video_from_mart

logger = logging.getLogger(__name__)

DBT_PROJECT_DIR = Path(__file__).parent.parent / "dbt_project"


# ── Status Callback Helper ────────────────────────────────────────────────────

def _emit(status_cb: Optional[Callable], stage: str, detail: str = ""):
    """Emit a status update if a callback was provided (used by Streamlit)."""
    if status_cb:
        status_cb(stage, detail)
    logger.info(f"[{stage}] {detail}")


# ── dbt Runner ────────────────────────────────────────────────────────────────

def run_dbt_build() -> tuple[bool, str]:
    """
    Invoke `dbt build` as a subprocess from the dbt_project directory.
    """
    if not DBT_PROJECT_DIR.exists():
        return False, f"dbt_project directory not found at {DBT_PROJECT_DIR}"

    # Use the bundled profiles.yml so this works on Streamlit Cloud
    profiles_dir = DBT_PROJECT_DIR / "profiles"

    try:
        result = subprocess.run(
            [
                "dbt", "build",
                "--profiles-dir", str(profiles_dir),
                "--project-dir", str(DBT_PROJECT_DIR),
            ],
            capture_output=True,
            text=True,
            timeout=600,
        )
    except subprocess.TimeoutExpired:
        return False, "dbt build timed out after 10 minutes"
    except FileNotFoundError:
        return False, "dbt command not found. Is dbt-core installed and on PATH?"

    output = result.stdout + "\n" + result.stderr

    if result.returncode == 0:
        return True, output

    # Protobuf telemetry bug detection (from Phase 7 fix)
    protobuf_telemetry_bug = (
        "MessageToJson()" in output
        and "including_default_value_fields" in output
    )
    all_models_passed = (
        "Completed successfully" in output
        or ("PASS=" in output and "ERROR=0" in output)
    )
    if protobuf_telemetry_bug and all_models_passed:
        logger.warning("dbt hit protobuf bug but models passed — treating as success")
        return True, output

    return False, output

# ── Main Pipeline ─────────────────────────────────────────────────────────────

def run_full_pipeline(
    youtube_url: str,
    status_cb: Optional[Callable] = None,
    force_reload: bool = False,
    skip_dbt: bool = False,
) -> dict:
    """
    Run the end-to-end pipeline for a YouTube URL.

    Args:
        youtube_url: Any valid YouTube URL.
        status_cb: Optional callback (stage: str, detail: str) for progress updates.
        force_reload: If True, re-extract and re-load even if cached.
        skip_dbt: If True, skip dbt build (useful for dev iteration).

    Returns:
        Dict with: video_id, video_title, study_guide, flashcards,
                   practice_questions, chunks, total_cost_usd, cached.
    """
    cached = False

    # ── Stage 1: Extract ──
    _emit(status_cb, "extract", "Downloading YouTube transcript")
    result, chunks = extract_full_transcript(youtube_url)
    video_id = result.video_id
    video_title = result.metadata.title

    _emit(
        status_cb,
        "extract",
        f"Got {len(result.segments)} segments, {len(chunks)} chunks from '{video_title}'",
    )

    # ── Stage 2: Upload to R2 ──
    if not force_reload and check_video_cached(video_id):
        _emit(status_cb, "r2", "Already cached in R2 — skipping upload")
        cached = True
    else:
        _emit(status_cb, "r2", "Uploading raw transcript + chunks to Cloudflare R2")
        upload_raw_transcript(video_id, transcript_result_to_dict(result))
        upload_raw_chunks(video_id, chunks)
        _emit(status_cb, "r2", "Upload complete")

    # ── Stage 3: Load into Snowflake ──
    if not force_reload and check_video_in_snowflake(video_id):
        _emit(status_cb, "snowflake", "Already loaded in Snowflake — skipping")
    else:
        _emit(status_cb, "snowflake", "Loading transcript into RAW.TRANSCRIPTS")
        rows = load_transcript_from_r2(video_id, force_reload=force_reload)
        _emit(status_cb, "snowflake", f"Inserted {rows} rows")

    # ── Stage 4: Run dbt transforms ──
    if skip_dbt:
        _emit(status_cb, "dbt", "Skipped (skip_dbt=True)")
    else:
        _emit(status_cb, "dbt", "Running dbt build (staging → intermediate → marts)")
        success, output = run_dbt_build()
        if not success:
            raise RuntimeError(f"dbt build failed:\n{output[-2000:]}")
        _emit(status_cb, "dbt", "Build complete, mart populated")

    # ── Stage 5: Verify mart has data for this video ──
    mart_check = execute_query(
        "SELECT COUNT(*) AS cnt FROM ANALYTICS.mart_study_content WHERE video_id = %s",
        (video_id,),
    )
    mart_count = mart_check[0]["CNT"] if mart_check else 0
    if mart_count == 0:
        raise RuntimeError(
            f"Video '{video_id}' not found in mart_study_content after dbt build. "
            "Check dbt model filters or re-run with force_reload=True."
        )

    # ── Stage 6: Run LLM pipeline ──
    _emit(status_cb, "llm", "Generating study guide, flashcards, and practice questions")
    llm_result = process_video_from_mart(video_id)
    _emit(
        status_cb,
        "llm",
        f"Done — ${llm_result['total_cost_usd']:.3f} in OpenAI costs",
    )

    return {
        **llm_result,
        "cached": cached,
        "youtube_url": youtube_url,
    }
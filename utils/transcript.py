"""
utils/transcript.py

YouTube transcript extraction, metadata fetching, and chunking.
"""

import re
import json
import logging
import requests
from dataclasses import dataclass, asdict
from typing import Optional
# Replace with just:
from youtube_transcript_api import YouTubeTranscriptApi

logger = logging.getLogger(__name__)


# ── Data Classes ──────────────────────────────────────────────────────────────

@dataclass
class TranscriptSegment:
    """A single timestamped segment from a YouTube transcript."""
    text: str
    start: float
    duration: float
    segment_index: int


@dataclass
class VideoMetadata:
    """Metadata about a YouTube video."""
    video_id: str
    title: str
    channel: str
    thumbnail_url: str
    url: str


@dataclass
class TranscriptResult:
    """Full result of a transcript extraction job."""
    video_id: str
    metadata: VideoMetadata
    segments: list[TranscriptSegment]
    language: str
    is_auto_generated: bool
    total_words: int
    total_characters: int


# ── URL Parsing ───────────────────────────────────────────────────────────────

def extract_video_id(url: str) -> str:
    """
    Extract a YouTube video ID from any valid URL format.

    Supports:
      - https://www.youtube.com/watch?v=VIDEO_ID
      - https://youtu.be/VIDEO_ID
      - https://youtube.com/shorts/VIDEO_ID
      - https://www.youtube.com/embed/VIDEO_ID
      - Raw 11-character video ID

    Args:
        url: YouTube URL string or raw video ID.

    Returns:
        11-character video ID string.

    Raises:
        ValueError: If no valid ID can be extracted.
    """
    if not url or not isinstance(url, str):
        raise ValueError("URL must be a non-empty string.")

    url = url.strip()

    # Raw video ID
    if re.match(r'^[A-Za-z0-9_-]{11}$', url):
        return url

    patterns = [
        r'(?:v=)([A-Za-z0-9_-]{11})',
        r'(?:youtu\.be/)([A-Za-z0-9_-]{11})',
        r'(?:shorts/)([A-Za-z0-9_-]{11})',
        r'(?:embed/)([A-Za-z0-9_-]{11})',
    ]

    for pattern in patterns:
        match = re.search(pattern, url)
        if match:
            video_id = match.group(1)
            logger.debug(f"Extracted video ID: {video_id}")
            return video_id

    raise ValueError(
        f"Could not extract a YouTube video ID from: '{url}'\n"
        "Accepted formats: youtube.com/watch?v=ID, youtu.be/ID, "
        "youtube.com/shorts/ID, youtube.com/embed/ID"
    )


# ── Metadata Fetching ─────────────────────────────────────────────────────────

def fetch_video_metadata(video_id: str) -> VideoMetadata:
    """
    Fetch video title and channel using YouTube's oEmbed endpoint.
    No API key required.

    Args:
        video_id: YouTube video ID.

    Returns:
        VideoMetadata dataclass.

    Raises:
        ValueError: If the video is private or does not exist.
        requests.RequestException: On network errors.
    """
    video_url = f"https://www.youtube.com/watch?v={video_id}"
    oembed_url = f"https://www.youtube.com/oembed?url={video_url}&format=json"

    logger.info(f"Fetching metadata for: {video_id}")

    try:
        response = requests.get(oembed_url, timeout=10)
        response.raise_for_status()
        data = response.json()
    except requests.exceptions.HTTPError as e:
        if response.status_code == 404:
            raise ValueError(
                f"Video not found or is private: {video_id}"
            )
        raise requests.RequestException(f"HTTP error fetching metadata: {e}")
    except requests.exceptions.ConnectionError:
        raise requests.RequestException(
            "Network error: could not reach YouTube. Check your connection."
        )
    except requests.exceptions.Timeout:
        raise requests.RequestException(
            "Request timed out while fetching video metadata."
        )

    metadata = VideoMetadata(
        video_id=video_id,
        title=data.get("title", "Unknown Title"),
        channel=data.get("author_name", "Unknown Channel"),
        thumbnail_url=f"https://img.youtube.com/vi/{video_id}/hqdefault.jpg",
        url=video_url,
    )

    logger.info(f"Metadata: '{metadata.title}' by {metadata.channel}")
    return metadata


# ── Transcript Fetching ───────────────────────────────────────────────────────

def fetch_transcript(
    video_id: str,
    preferred_languages: list[str] = None,
) -> tuple[list[TranscriptSegment], str, bool]:
    """
    Fetch transcript segments with a language fallback chain:
      1. Manual transcript in preferred languages
      2. Auto-generated in preferred languages
      3. Any available transcript
    """
    if preferred_languages is None:
        preferred_languages = ["en", "en-US", "en-GB"]

    logger.info(f"Fetching transcript for: {video_id}")

    api = YouTubeTranscriptApi()

    try:
        transcript_list = api.list(video_id)
    except Exception as e:
        error_msg = str(e).lower()
        if "disabled" in error_msg:
            raise Exception(f"Transcripts are disabled for video '{video_id}'.")
        elif "unavailable" in error_msg or "private" in error_msg:
            raise Exception(f"Video '{video_id}' is unavailable or private.")
        raise

    transcript = None
    language_used = None
    is_auto = False

    # 1. Manual transcript in preferred languages
    try:
        transcript = transcript_list.find_manually_created_transcript(
            preferred_languages
        )
        language_used = transcript.language_code
        is_auto = False
        logger.info(f"Found manual transcript: {language_used}")
    except Exception:
        pass

    # 2. Auto-generated in preferred languages
    if not transcript:
        try:
            transcript = transcript_list.find_generated_transcript(
                preferred_languages
            )
            language_used = transcript.language_code
            is_auto = True
            logger.info(f"Found auto-generated transcript: {language_used}")
        except Exception:
            pass

    # 3. Any available transcript
    if not transcript:
        available = list(transcript_list)
        if not available:
            raise Exception(
                f"No transcript found for video '{video_id}' in any language."
            )
        transcript = available[0]
        language_used = transcript.language_code
        is_auto = True
        logger.warning(f"Falling back to: {language_used}")

    # Fetch the actual transcript data
    # v1.x returns a FetchedTranscript object — iterate it directly
    fetched = transcript.fetch()

    segments = []
    for i, snippet in enumerate(fetched):
        # In v1.x, each snippet is a FetchedTranscriptSnippet object
        # access attributes directly instead of .get()
        try:
            text = snippet.text.strip()
            start = snippet.start
            duration = snippet.duration
        except AttributeError:
            # Fallback for dict-style access (older sub-versions)
            text = snippet.get("text", "").strip()
            start = snippet.get("start", 0.0)
            duration = snippet.get("duration", 0.0)

        if text:
            segments.append(
                TranscriptSegment(
                    text=text,
                    start=start,
                    duration=duration,
                    segment_index=i,
                )
            )

    logger.info(f"Fetched {len(segments)} segments")
    return segments, language_used, is_auto


# ── Chunking ──────────────────────────────────────────────────────────────────

def estimate_token_count(text: str) -> int:
    """
    Estimate token count using the ~4 chars/token heuristic.

    Args:
        text: Input string.

    Returns:
        Estimated token count as integer.
    """
    return len(text) // 4


def chunk_transcript(
    segments: list[TranscriptSegment],
    max_tokens: int = 4000,
    overlap_tokens: int = 200,
) -> list[dict]:
    """
    Group transcript segments into overlapping chunks for LLM processing.

    Splits only at segment boundaries (never mid-sentence). Each chunk
    includes start/end timestamps for deep-linking back to the video.

    Args:
        segments: List of TranscriptSegment objects.
        max_tokens: Token ceiling per chunk (default 4000).
        overlap_tokens: Tokens carried over from the previous chunk
                        to preserve context at boundaries (default 200).

    Returns:
        List of chunk dicts with keys:
            chunk_index, text, start_time, end_time,
            segment_start, segment_end, token_estimate
    """
    if not segments:
        return []

    chunks = []
    current_segments: list[TranscriptSegment] = []
    current_tokens = 0

    for segment in segments:
        seg_tokens = estimate_token_count(segment.text)

        # Flush current chunk when adding this segment would exceed the limit
        if current_tokens + seg_tokens > max_tokens and current_segments:
            chunk_text = " ".join(s.text for s in current_segments)
            chunks.append({
                "chunk_index": len(chunks),
                "text": chunk_text,
                "start_time": current_segments[0].start,
                "end_time": (
                    current_segments[-1].start + current_segments[-1].duration
                ),
                "segment_start": current_segments[0].segment_index,
                "segment_end": current_segments[-1].segment_index,
                "token_estimate": estimate_token_count(chunk_text),
            })

            # Build overlap buffer from the tail of the current chunk
            overlap_buffer: list[TranscriptSegment] = []
            overlap_count = 0
            for s in reversed(current_segments):
                s_tokens = estimate_token_count(s.text)
                if overlap_count + s_tokens <= overlap_tokens:
                    overlap_buffer.insert(0, s)
                    overlap_count += s_tokens
                else:
                    break

            current_segments = overlap_buffer
            current_tokens = overlap_count

        current_segments.append(segment)
        current_tokens += seg_tokens

    # Final chunk
    if current_segments:
        chunk_text = " ".join(s.text for s in current_segments)
        chunks.append({
            "chunk_index": len(chunks),
            "text": chunk_text,
            "start_time": current_segments[0].start,
            "end_time": (
                current_segments[-1].start + current_segments[-1].duration
            ),
            "segment_start": current_segments[0].segment_index,
            "segment_end": current_segments[-1].segment_index,
            "token_estimate": estimate_token_count(chunk_text),
        })

    total_tokens = sum(c["token_estimate"] for c in chunks)
    logger.info(f"Created {len(chunks)} chunks (~{total_tokens:,} total tokens)")
    return chunks


# ── Main Orchestrator ─────────────────────────────────────────────────────────

def extract_full_transcript(
    url: str,
    preferred_languages: list[str] = None,
    chunk_max_tokens: int = 4000,
) -> tuple[TranscriptResult, list[dict]]:
    """
    Full extraction pipeline: URL → metadata + segments + chunks.

    This is the single entry point called by the Streamlit app,
    the R2 uploader, and the smoke test.

    Args:
        url: Any valid YouTube URL or raw video ID.
        preferred_languages: Language preference list.
        chunk_max_tokens: Token ceiling per chunk.

    Returns:
        Tuple of (TranscriptResult, chunks_list).
    """
    logger.info(f"Starting extraction for: {url}")

    video_id = extract_video_id(url)
    metadata = fetch_video_metadata(video_id)
    segments, language, is_auto = fetch_transcript(video_id, preferred_languages)

    full_text = " ".join(s.text for s in segments)

    result = TranscriptResult(
        video_id=video_id,
        metadata=metadata,
        segments=segments,
        language=language,
        is_auto_generated=is_auto,
        total_words=len(full_text.split()),
        total_characters=len(full_text),
    )

    chunks = chunk_transcript(segments, max_tokens=chunk_max_tokens)

    logger.info(
        f"Extraction complete — {result.total_words:,} words, "
        f"{len(segments)} segments, {len(chunks)} chunks"
    )
    return result, chunks


def transcript_result_to_dict(result: TranscriptResult) -> dict:
    """
    Serialize a TranscriptResult to a JSON-safe dictionary for R2 storage.

    Args:
        result: TranscriptResult dataclass instance.

    Returns:
        Fully serializable dict.
    """
    return {
        "video_id": result.video_id,
        "metadata": asdict(result.metadata),
        "segments": [asdict(s) for s in result.segments],
        "language": result.language,
        "is_auto_generated": result.is_auto_generated,
        "total_words": result.total_words,
        "total_characters": result.total_characters,
    }
"""
utils/summarizer.py

OpenAI-powered summarization, flashcard generation, and Q&A for TubeDigest.

Architecture:
  mart_study_content (Snowflake)
    → summarize_chunk()        [per-chunk, gpt-4o-mini, parallel]
    → synthesize_summaries()   [final pass, gpt-4o]
    → generate_flashcards()    [full text, gpt-4o, JSON mode]
    → generate_practice_qs()   [full text, gpt-4o, JSON mode]
    → ask_question()           [RAG-lite with TF-IDF over chunks]
"""

import os
import json
import logging
from pathlib import Path
from typing import Optional

import tiktoken
from openai import OpenAI
from openai import APIError, RateLimitError, APIConnectionError
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
)
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
import numpy as np

logger = logging.getLogger(__name__)

PROMPTS_DIR = Path(__file__).parent.parent / "prompts"


# ── Pricing (per 1M tokens, as of early 2026 — verify at openai.com/pricing) ──

PRICING = {
    "gpt-4o":       {"input": 2.50, "output": 10.00},
    "gpt-4o-mini":  {"input": 0.15, "output": 0.60},
}


# ── Client Factory ────────────────────────────────────────────────────────────

def get_openai_client() -> OpenAI:
    """
    Create an OpenAI client using OPENAI_API_KEY from environment.

    Returns:
        OpenAI client instance.

    Raises:
        EnvironmentError: If API key is not set.
    """
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise EnvironmentError(
            "OPENAI_API_KEY not set. Add it to your .env file."
        )
    return OpenAI(api_key=api_key)


def load_prompt(name: str) -> str:
    """
    Load a prompt template from the prompts/ directory.

    Args:
        name: Prompt filename without extension (e.g. 'summarize').

    Returns:
        Prompt content as string.

    Raises:
        FileNotFoundError: If prompt file doesn't exist.
    """
    path = PROMPTS_DIR / f"{name}.txt"
    if not path.exists():
        raise FileNotFoundError(f"Prompt not found: {path}")
    return path.read_text(encoding="utf-8").strip()


# ── Token Counting & Cost Estimation ──────────────────────────────────────────

def count_tokens(text: str, model: str = "gpt-4o") -> int:
    """
    Accurate token count using tiktoken.

    Args:
        text: Input string.
        model: Model name for tokenizer selection.

    Returns:
        Exact token count.
    """
    try:
        encoding = tiktoken.encoding_for_model(model)
    except KeyError:
        # Fallback for models not yet in tiktoken registry
        encoding = tiktoken.get_encoding("o200k_base")
    return len(encoding.encode(text))


def estimate_cost(
    input_tokens: int,
    output_tokens: int,
    model: str = "gpt-4o",
) -> float:
    """
    Estimate USD cost for a completion given token counts.

    Args:
        input_tokens: Prompt tokens.
        output_tokens: Completion tokens.
        model: Model name.

    Returns:
        Estimated cost in dollars.
    """
    if model not in PRICING:
        logger.warning(f"No pricing data for model '{model}', cost estimate = 0")
        return 0.0
    p = PRICING[model]
    return (input_tokens / 1_000_000) * p["input"] + (output_tokens / 1_000_000) * p["output"]


# ── Core LLM Call (with retry) ────────────────────────────────────────────────

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=2, min=2, max=30),
    retry=retry_if_exception_type((RateLimitError, APIConnectionError, APIError)),
    reraise=True,
)
def call_openai(
    system_prompt: str,
    user_content: str,
    model: str = None,
    temperature: float = 0.3,
    json_mode: bool = False,
    max_tokens: int = 4000,
) -> tuple[str, dict]:
    """
    Call OpenAI chat completions with exponential-backoff retry.

    Args:
        system_prompt: System-level instructions.
        user_content: User message content.
        model: Model name (defaults to OPENAI_MODEL env var or gpt-4o).
        temperature: Sampling temperature (0 = deterministic).
        json_mode: If True, force structured JSON output.
        max_tokens: Max tokens in completion.

    Returns:
        Tuple of (completion_text, usage_dict with tokens + cost).
    """
    if model is None:
        model = os.getenv("OPENAI_MODEL", "gpt-4o")

    client = get_openai_client()

    logger.info(
        f"Calling {model} — input ~{count_tokens(system_prompt + user_content, model):,} tokens"
    )

    kwargs = {
        "model": model,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_content},
        ],
        "temperature": temperature,
        "max_tokens": max_tokens,
    }
    if json_mode:
        kwargs["response_format"] = {"type": "json_object"}

    response = client.chat.completions.create(**kwargs)

    text = response.choices[0].message.content
    usage = response.usage

    cost = estimate_cost(usage.prompt_tokens, usage.completion_tokens, model)
    usage_dict = {
        "model": model,
        "input_tokens": usage.prompt_tokens,
        "output_tokens": usage.completion_tokens,
        "total_tokens": usage.total_tokens,
        "estimated_cost_usd": round(cost, 4),
    }
    logger.info(
        f"Completed: {usage.total_tokens:,} tokens, ~${cost:.4f}"
    )

    return text, usage_dict


# ── Chunk Summarization ───────────────────────────────────────────────────────

def summarize_chunk(chunk_text: str, model: str = None) -> tuple[str, dict]:
    """
    Generate structured Markdown study notes for a single transcript chunk.

    Uses the cheaper model (gpt-4o-mini) by default since per-chunk work
    is high-volume and the synthesize step will clean things up.

    Args:
        chunk_text: Transcript chunk text.
        model: Override model (defaults to OPENAI_MODEL_CHEAP).

    Returns:
        (markdown_notes, usage_dict)
    """
    if model is None:
        model = os.getenv("OPENAI_MODEL_CHEAP", "gpt-4o-mini")

    system_prompt = load_prompt("summarize")
    return call_openai(
        system_prompt=system_prompt,
        user_content=chunk_text,
        model=model,
        temperature=0.2,
        max_tokens=3000,
    )


def synthesize_summaries(
    chunk_summaries: list[str],
    video_title: str,
    model: str = None,
) -> tuple[str, dict]:
    """
    Merge per-chunk summaries into a single cohesive study guide.

    Args:
        chunk_summaries: List of Markdown summaries (output of summarize_chunk).
        video_title: Title of the video for the guide header.
        model: Override model (defaults to OPENAI_MODEL).

    Returns:
        (final_markdown_guide, usage_dict)
    """
    if model is None:
        model = os.getenv("OPENAI_MODEL", "gpt-4o")

    system_prompt = load_prompt("synthesize")

    user_content = f"Video title: {video_title}\n\n"
    user_content += "Per-chunk study notes to merge:\n\n"
    for i, summary in enumerate(chunk_summaries):
        user_content += f"\n\n--- CHUNK {i} ---\n\n{summary}"

    return call_openai(
        system_prompt=system_prompt,
        user_content=user_content,
        model=model,
        temperature=0.2,
        max_tokens=6000,
    )


# ── Flashcards ────────────────────────────────────────────────────────────────

def generate_flashcards(
    full_text: str,
    model: str = None,
) -> tuple[list[dict], dict]:
    """
    Generate flashcards from the full transcript text.

    Args:
        full_text: Joined transcript content.
        model: Override model (defaults to OPENAI_MODEL).

    Returns:
        (flashcards_list, usage_dict)
    """
    if model is None:
        model = os.getenv("OPENAI_MODEL", "gpt-4o")

    system_prompt = load_prompt("flashcards")

    # Force JSON mode by wrapping the prompt instruction
    wrapped_system = (
        system_prompt +
        '\n\nOutput a JSON object with a single key "flashcards" containing the array.'
    )

    text, usage = call_openai(
        system_prompt=wrapped_system,
        user_content=full_text,
        model=model,
        temperature=0.3,
        json_mode=True,
        max_tokens=4000,
    )

    cards = safe_parse_json(text, default_key="flashcards")
    logger.info(f"Generated {len(cards)} flashcards")
    return cards, usage


# ── Practice Questions ────────────────────────────────────────────────────────

def generate_practice_questions(
    full_text: str,
    model: str = None,
) -> tuple[list[dict], dict]:
    """
    Generate mixed MCQ + free-response practice questions.

    Args:
        full_text: Joined transcript content.
        model: Override model.

    Returns:
        (questions_list, usage_dict)
    """
    if model is None:
        model = os.getenv("OPENAI_MODEL", "gpt-4o")

    system_prompt = load_prompt("practice")
    wrapped_system = (
        system_prompt +
        '\n\nOutput a JSON object with a single key "questions" containing the array.'
    )

    text, usage = call_openai(
        system_prompt=wrapped_system,
        user_content=full_text,
        model=model,
        temperature=0.4,
        json_mode=True,
        max_tokens=4000,
    )

    questions = safe_parse_json(text, default_key="questions")
    logger.info(f"Generated {len(questions)} practice questions")
    return questions, usage


# ── RAG-Lite Q&A ──────────────────────────────────────────────────────────────

def ask_question(
    question: str,
    chunks: list[dict],
    top_k: int = 3,
    model: str = None,
) -> tuple[str, dict]:
    """
    Answer a user's question using TF-IDF retrieval over transcript chunks.

    Picks the top-k most relevant chunks by TF-IDF cosine similarity,
    stuffs them into the prompt as context, and asks the model to answer.

    Args:
        question: User's question.
        chunks: List of chunk dicts (from mart_study_content).
                Must have 'chunk_text' key.
        top_k: How many chunks to include as context.
        model: Override model.

    Returns:
        (answer_text, usage_dict)
    """
    if not chunks:
        return "No transcript data available to answer this question.", {}

    if model is None:
        model = os.getenv("OPENAI_MODEL", "gpt-4o")

    # Build TF-IDF index over chunks + the question
    chunk_texts = [c["chunk_text"] for c in chunks]
    corpus = chunk_texts + [question]

    vectorizer = TfidfVectorizer(
        stop_words="english",
        max_features=5000,
        ngram_range=(1, 2),
    )
    tfidf = vectorizer.fit_transform(corpus)

    question_vec = tfidf[-1]
    chunk_vecs = tfidf[:-1]

    similarities = cosine_similarity(question_vec, chunk_vecs).flatten()
    top_indices = np.argsort(similarities)[-top_k:][::-1]

    context_parts = []
    for idx in top_indices:
        sim_score = similarities[idx]
        if sim_score < 0.01:
            continue
        context_parts.append(
            f"[Chunk {chunks[idx].get('chunk_index', idx)} — "
            f"{chunks[idx].get('chunk_start_seconds', 0):.0f}s to "
            f"{chunks[idx].get('chunk_end_seconds', 0):.0f}s]\n"
            f"{chunks[idx]['chunk_text']}"
        )

    if not context_parts:
        return (
            "I couldn't find relevant content in the transcript for that question.",
            {},
        )

    context = "\n\n---\n\n".join(context_parts)

    system_prompt = (
        "You are a helpful tutor answering a student's question using only the provided "
        "transcript context. Quote specific phrases when relevant. If the answer isn't in "
        "the context, say so honestly instead of guessing."
    )

    user_content = (
        f"Transcript context:\n\n{context}\n\n"
        f"---\n\nStudent's question: {question}\n\n"
        f"Provide a clear, detailed answer grounded in the transcript."
    )

    return call_openai(
        system_prompt=system_prompt,
        user_content=user_content,
        model=model,
        temperature=0.2,
        max_tokens=1500,
    )


# ── Utilities ─────────────────────────────────────────────────────────────────

def safe_parse_json(text: str, default_key: str = None) -> list:
    """
    Parse JSON output from an LLM, handling common failure modes.

    Strips markdown fences, handles wrapped objects, returns empty list on failure.

    Args:
        text: Raw LLM output.
        default_key: If JSON is an object with this key, extract the array.

    Returns:
        Parsed list, or empty list on failure.
    """
    text = text.strip()

    # Strip ```json ... ``` fences if present
    if text.startswith("```"):
        lines = text.split("\n")
        text = "\n".join(lines[1:-1]) if len(lines) > 2 else text

    try:
        data = json.loads(text)
    except json.JSONDecodeError as e:
        logger.error(f"Failed to parse JSON from LLM output: {e}")
        logger.debug(f"Raw output: {text[:500]}")
        return []

    # Handle {"flashcards": [...]} wrapping
    if isinstance(data, dict) and default_key and default_key in data:
        return data[default_key]

    # Handle {"array": [...]} generic wrapping
    if isinstance(data, dict):
        for v in data.values():
            if isinstance(v, list):
                return v
        return []

    if isinstance(data, list):
        return data

    return []


# ── End-to-End Pipeline ───────────────────────────────────────────────────────

def process_video_from_mart(video_id: str) -> dict:
    """
    Full pipeline: read chunks from Snowflake mart, generate all LLM outputs.

    Args:
        video_id: YouTube video ID.

    Returns:
        Dict with keys: study_guide, flashcards, practice_questions,
                        total_cost_usd, usage_by_step.
    """
    from utils.snowflake_loader import execute_query

    logger.info(f"Starting LLM pipeline for video '{video_id}'")

    # 1. Load chunks from mart
    sql = """
        SELECT
            video_id, chunk_index, chunk_text, chunk_token_estimate,
            chunk_start_seconds, chunk_end_seconds,
            video_title, channel, language
        FROM ANALYTICS.mart_study_content
        WHERE video_id = %s
        ORDER BY chunk_index
    """
    chunks = execute_query(sql, (video_id,))

    if not chunks:
        raise ValueError(
            f"No chunks found in mart_study_content for video '{video_id}'. "
            "Run the Phase 4 loader and `dbt build` first."
        )

    # Normalize Snowflake UPPERCASE keys to lowercase for consistency
    chunks = [{k.lower(): v for k, v in c.items()} for c in chunks]

    video_title = chunks[0]["video_title"]
    logger.info(f"Loaded {len(chunks)} chunks for '{video_title}'")

    usage_by_step = {}
    total_cost = 0.0

    # 2. Summarize each chunk
    logger.info("Step 1/4: Summarizing chunks...")
    chunk_summaries = []
    chunk_usage = []
    for chunk in chunks:
        summary, usage = summarize_chunk(chunk["chunk_text"])
        chunk_summaries.append(summary)
        chunk_usage.append(usage)
        total_cost += usage["estimated_cost_usd"]
    usage_by_step["chunk_summaries"] = chunk_usage

    # 3. Synthesize into final study guide
    logger.info("Step 2/4: Synthesizing final study guide...")
    study_guide, synth_usage = synthesize_summaries(chunk_summaries, video_title)
    usage_by_step["synthesize"] = synth_usage
    total_cost += synth_usage["estimated_cost_usd"]

    # 4. Build full text for flashcards + practice
    full_text = "\n\n".join(c["chunk_text"] for c in chunks)

    # 5. Generate flashcards
    logger.info("Step 3/4: Generating flashcards...")
    flashcards, fc_usage = generate_flashcards(full_text)
    usage_by_step["flashcards"] = fc_usage
    total_cost += fc_usage["estimated_cost_usd"]

    # 6. Generate practice questions
    logger.info("Step 4/4: Generating practice questions...")
    practice, pq_usage = generate_practice_questions(full_text)
    usage_by_step["practice"] = pq_usage
    total_cost += pq_usage["estimated_cost_usd"]

    logger.info(f"Pipeline complete — total cost: ${total_cost:.4f}")

    return {
        "video_id": video_id,
        "video_title": video_title,
        "study_guide": study_guide,
        "flashcards": flashcards,
        "practice_questions": practice,
        "chunks": chunks,  # kept for Q&A step later
        "total_cost_usd": round(total_cost, 4),
        "usage_by_step": usage_by_step,
    }
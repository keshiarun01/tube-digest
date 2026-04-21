"""
TubeDigest — AI-powered YouTube study guide generator.

Main Streamlit application. Run with:
    streamlit run app.py
"""

import json
import logging
from pathlib import Path

import streamlit as st
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
# Quiet noisy libs
for lib in ["httpx", "snowflake.connector", "urllib3", "botocore"]:
    logging.getLogger(lib).setLevel(logging.WARNING)

from utils.pipeline import run_full_pipeline
from utils.summarizer import ask_question


# ── Page Setup ────────────────────────────────────────────────────────────────

st.set_page_config(
    page_title="TubeDigest",
    page_icon="🎬",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Load custom CSS
css_path = Path(__file__).parent / "assets" / "style.css"
if css_path.exists():
    st.markdown(f"<style>{css_path.read_text()}</style>", unsafe_allow_html=True)

# Fonts
st.markdown(
    '<link href="https://fonts.googleapis.com/css2?family=Outfit:wght@400;500;600;700;800&'
    'family=IBM+Plex+Sans:wght@400;500;600&family=IBM+Plex+Mono:wght@400;500&display=swap" '
    'rel="stylesheet">',
    unsafe_allow_html=True,
)


# ── Session State Defaults ────────────────────────────────────────────────────

def init_session_state():
    defaults = {
        "pipeline_result": None,
        "processing": False,
        "error": None,
        "chat_history": [],
        "stage_log": [],
    }
    for k, v in defaults.items():
        if k not in st.session_state:
            st.session_state[k] = v

init_session_state()


# ── Sidebar ───────────────────────────────────────────────────────────────────

with st.sidebar:
    st.markdown("## 🎬 TubeDigest")
    st.caption("Turn any YouTube video into an interactive study guide")
    st.divider()

    youtube_url = st.text_input(
        "YouTube URL",
        placeholder="https://www.youtube.com/watch?v=...",
        help="Paste any YouTube tutorial or lecture URL",
        key="url_input",
    )

    with st.expander("⚙️ Advanced settings"):
        force_reload = st.checkbox(
            "Force re-process (ignore cache)",
            value=False,
            help="Re-extract, re-upload, and re-run dbt even if already cached",
        )
        skip_dbt = st.checkbox(
            "Skip dbt build (dev only)",
            value=False,
            help="Useful if you already ran `dbt build` manually",
        )

    process_btn = st.button(
        "🚀 Generate Study Guide",
        type="primary",
        use_container_width=True,
        disabled=st.session_state.processing or not youtube_url,
    )

    st.divider()

    if st.session_state.pipeline_result:
        r = st.session_state.pipeline_result
        st.markdown("### Session")
        st.caption(f"**Video:** {r['video_title'][:50]}...")
        st.caption(f"**Chunks:** {len(r['chunks'])}")
        st.caption(f"**Flashcards:** {len(r['flashcards'])}")
        st.caption(f"**Practice Qs:** {len(r['practice_questions'])}")
        st.markdown(
            f'<span class="cost-pill">Cost: ${r["total_cost_usd"]:.3f}</span>',
            unsafe_allow_html=True,
        )
        if r.get("cached"):
            st.caption("♻️ Served from cache")


# ── Pipeline Execution ────────────────────────────────────────────────────────

if process_btn and youtube_url:
    st.session_state.processing = True
    st.session_state.error = None
    st.session_state.pipeline_result = None
    st.session_state.chat_history = []
    st.session_state.stage_log = []

    stage_labels = {
        "extract":   "📥 Extracting YouTube transcript",
        "r2":        "☁️  Uploading to Cloudflare R2",
        "snowflake": "❄️  Loading into Snowflake",
        "dbt":       "🔧 Running dbt transformations",
        "llm":       "🤖 Generating study content with OpenAI",
    }

    status = st.status("Starting pipeline...", expanded=True)

    def status_callback(stage: str, detail: str):
        label = stage_labels.get(stage, stage)
        status.update(label=f"{label} — {detail}")
        st.session_state.stage_log.append((stage, detail))
        status.write(f"**{stage}**: {detail}")

    try:
        result = run_full_pipeline(
            youtube_url=youtube_url,
            status_cb=status_callback,
            force_reload=force_reload,
            skip_dbt=skip_dbt,
        )
        st.session_state.pipeline_result = result
        status.update(label="✅ Complete", state="complete", expanded=False)
    except Exception as e:
        st.session_state.error = str(e)
        status.update(label=f"❌ Failed: {e}", state="error")
        logging.exception("Pipeline failed")
    finally:
        st.session_state.processing = False


# ── Main Content ──────────────────────────────────────────────────────────────

st.title("🎬 TubeDigest")
st.caption("From hours of video to an interview-ready study guide in minutes")

if st.session_state.error:
    st.error(f"Pipeline error: {st.session_state.error}")

if not st.session_state.pipeline_result and not st.session_state.processing:
    # Landing state
    col1, col2, col3 = st.columns(3)
    with col1:
        st.markdown("### 📥 Extract")
        st.caption(
            "Pulls any YouTube transcript — auto-captioned or manual. "
            "Works on tutorials, lectures, and talks."
        )
    with col2:
        st.markdown("### 🔧 Transform")
        st.caption(
            "Cloudflare R2 → Snowflake → dbt pipeline chunks and prepares "
            "the content for LLM processing."
        )
    with col3:
        st.markdown("### 🤖 Generate")
        st.caption(
            "OpenAI GPT-5.4 produces a structured study guide, "
            "flashcards, practice questions, and Q&A."
        )

    st.divider()
    st.info("👈 Paste a YouTube URL in the sidebar to get started.")

elif st.session_state.pipeline_result:
    result = st.session_state.pipeline_result

    # Header row: title + cost badge
    header_col1, header_col2 = st.columns([3, 1])
    with header_col1:
        st.markdown(f"### {result['video_title']}")
        st.caption(f"`{result['video_id']}`")
    with header_col2:
        st.metric("Generation cost", f"${result['total_cost_usd']:.3f}")

    # Four tabs
    tab_guide, tab_cards, tab_practice, tab_ask = st.tabs([
        "📖 Study Guide",
        f"🃏 Flashcards ({len(result['flashcards'])})",
        f"📝 Practice ({len(result['practice_questions'])})",
        "💬 Ask Anything",
    ])

    # ── Tab 1: Study Guide ──
    with tab_guide:
        c1, c2 = st.columns([3, 1])
        with c1:
            st.markdown(result["study_guide"])
        with c2:
            st.download_button(
                "⬇️ Download as Markdown",
                data=result["study_guide"],
                file_name=f"{result['video_id']}_study_guide.md",
                mime="text/markdown",
                use_container_width=True,
            )
            st.caption(f"**{len(result['study_guide']):,}** characters")

    # ── Tab 2: Flashcards ──
    with tab_cards:
        cards = result["flashcards"]
        if not cards:
            st.info("No flashcards were generated for this video.")
        else:
            # Filter controls
            filter_col1, filter_col2 = st.columns([1, 3])
            with filter_col1:
                difficulties = sorted({c.get("difficulty", "unknown") for c in cards})
                selected_diffs = st.multiselect(
                    "Difficulty",
                    difficulties,
                    default=difficulties,
                )
            with filter_col2:
                all_tags = sorted({t for c in cards for t in c.get("tags", [])})
                selected_tags = st.multiselect(
                    "Tags",
                    all_tags,
                    default=[],
                    placeholder="All tags",
                )

            filtered = [
                c for c in cards
                if c.get("difficulty") in selected_diffs
                and (not selected_tags or any(t in selected_tags for t in c.get("tags", [])))
            ]
            st.caption(f"Showing {len(filtered)} of {len(cards)} cards")

            for card in filtered:
                difficulty = card.get("difficulty", "intermediate")
                with st.expander(
                    f"**{card.get('concept', 'Concept')}** — {card.get('question', '')}"
                ):
                    st.markdown(
                        f'<div class="flashcard-answer">{card.get("answer", "")}</div>',
                        unsafe_allow_html=True,
                    )
                    meta_col1, meta_col2 = st.columns([1, 3])
                    with meta_col1:
                        st.markdown(
                            f'<span class="badge badge-{difficulty}">{difficulty}</span>',
                            unsafe_allow_html=True,
                        )
                    with meta_col2:
                        tags = card.get("tags", [])
                        if tags:
                            st.caption(" · ".join(f"#{t}" for t in tags))

            st.download_button(
                "⬇️ Download all flashcards (JSON)",
                data=json.dumps(cards, indent=2),
                file_name=f"{result['video_id']}_flashcards.json",
                mime="application/json",
            )

    # ── Tab 3: Practice Questions ──
    with tab_practice:
        questions = result["practice_questions"]
        if not questions:
            st.info("No practice questions were generated.")
        else:
            for i, q in enumerate(questions, 1):
                q_type = q.get("type", "free_response")
                difficulty = q.get("difficulty", "intermediate")
                concept = q.get("concept", "")

                with st.container(border=True):
                    q_col1, q_col2 = st.columns([4, 1])
                    with q_col1:
                        st.markdown(f"**Question {i}** — {concept}")
                    with q_col2:
                        st.markdown(
                            f'<span class="badge badge-{difficulty}">{difficulty}</span>',
                            unsafe_allow_html=True,
                        )

                    st.markdown(q.get("question", ""))

                    if q_type == "multiple_choice" and q.get("options"):
                        user_choice = st.radio(
                            "Your answer:",
                            q["options"],
                            key=f"q_{i}",
                            label_visibility="collapsed",
                        )
                        if st.button("Reveal answer", key=f"reveal_{i}"):
                            correct = q.get("correct_answer", "")
                            if user_choice and user_choice.startswith(correct):
                                st.success(f"✅ Correct! Answer: {correct}")
                            else:
                                st.error(f"❌ Correct answer: {correct}")
                            st.info(q.get("explanation", "No explanation provided."))
                    else:
                        with st.expander("Show model answer"):
                            st.markdown(f"**Answer:** {q.get('correct_answer', '')}")
                            st.markdown(f"**Explanation:** {q.get('explanation', '')}")

            st.download_button(
                "⬇️ Download practice questions (JSON)",
                data=json.dumps(questions, indent=2),
                file_name=f"{result['video_id']}_practice.json",
                mime="application/json",
            )

    # ── Tab 4: Ask Anything ──
    with tab_ask:
        st.caption(
            "Ask any question about the video. Answers are grounded in the transcript."
        )

        # Display chat history
        for msg in st.session_state.chat_history:
            with st.chat_message(msg["role"]):
                st.markdown(msg["content"])
                if msg.get("cost"):
                    st.caption(f"Cost: ${msg['cost']:.4f}")

        # Chat input
        if user_question := st.chat_input("Ask a question about this video..."):
            st.session_state.chat_history.append({
                "role": "user",
                "content": user_question,
            })
            with st.chat_message("user"):
                st.markdown(user_question)

            with st.chat_message("assistant"):
                with st.spinner("Thinking..."):
                    try:
                        answer, usage = ask_question(
                            user_question,
                            result["chunks"],
                            top_k=3,
                        )
                        st.markdown(answer)
                        cost = usage.get("estimated_cost_usd", 0)
                        if cost:
                            st.caption(f"Cost: ${cost:.4f}")
                        st.session_state.chat_history.append({
                            "role": "assistant",
                            "content": answer,
                            "cost": cost,
                        })
                    except Exception as e:
                        st.error(f"Q&A failed: {e}")

        if st.session_state.chat_history:
            if st.button("Clear chat history"):
                st.session_state.chat_history = []
                st.rerun()
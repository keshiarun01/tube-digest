# TubeDigest

AI-powered YouTube study guide generator. Paste a YouTube URL and get a complete study guide, flashcards, and practice questions.

## Tech Stack
- **Extraction:** `youtube-transcript-api`, `requests`, `boto3`
- **Storage:** AWS S3 (raw data lake), Snowflake (warehouse)
- **Transform:** dbt-core + dbt-snowflake
- **LLM:** Anthropic Claude API
- **UI:** Streamlit
- **Infra:** Docker + docker-compose

## Quickstart

```bash
# 1. Clone and enter project
cd tube-digest

# 2. Create venv and install
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# 3. Configure secrets
cp .env.example .env
# → fill in your real credentials

# 4. Run
streamlit run app.py
```

## Docker

```bash
docker compose up --build
```

## Tests

```bash
pytest tests/ -v
```

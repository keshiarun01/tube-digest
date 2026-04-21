"""
tests/test_summarizer.py

Unit tests for utils/summarizer.py (all OpenAI calls mocked).
Run: pytest tests/test_summarizer.py -v
"""

import pytest
from unittest.mock import patch, MagicMock


@pytest.fixture
def openai_env(monkeypatch):
    monkeypatch.setenv("OPENAI_API_KEY", "sk-fake-test-key")
    monkeypatch.setenv("OPENAI_MODEL", "gpt-4o")
    monkeypatch.setenv("OPENAI_MODEL_CHEAP", "gpt-4o-mini")


def make_mock_completion(content: str, input_tokens=100, output_tokens=50):
    """Build a mock OpenAI ChatCompletion response."""
    mock_response = MagicMock()
    mock_response.choices = [MagicMock()]
    mock_response.choices[0].message.content = content
    mock_response.usage.prompt_tokens = input_tokens
    mock_response.usage.completion_tokens = output_tokens
    mock_response.usage.total_tokens = input_tokens + output_tokens
    return mock_response


class TestClientFactory:

    def test_raises_without_api_key(self, monkeypatch):
        monkeypatch.delenv("OPENAI_API_KEY", raising=False)
        from utils.summarizer import get_openai_client
        with pytest.raises(EnvironmentError):
            get_openai_client()

    def test_returns_client_with_key(self, openai_env):
        from utils.summarizer import get_openai_client
        client = get_openai_client()
        assert client is not None


class TestTokenCounting:

    def test_count_tokens_non_empty(self):
        from utils.summarizer import count_tokens
        assert count_tokens("hello world") > 0

    def test_count_tokens_empty(self):
        from utils.summarizer import count_tokens
        assert count_tokens("") == 0

    def test_estimate_cost_gpt4o(self):
        from utils.summarizer import estimate_cost
        # 1M input + 1M output should equal $2.50 + $10.00 = $12.50
        cost = estimate_cost(1_000_000, 1_000_000, "gpt-4o")
        assert abs(cost - 12.50) < 0.01

    def test_estimate_cost_unknown_model(self):
        from utils.summarizer import estimate_cost
        assert estimate_cost(1000, 1000, "fake-model") == 0.0


class TestSafeParseJson:

    def test_plain_json_array(self):
        from utils.summarizer import safe_parse_json
        result = safe_parse_json('[{"id": "1"}, {"id": "2"}]')
        assert len(result) == 2

    def test_wrapped_object_with_key(self):
        from utils.summarizer import safe_parse_json
        result = safe_parse_json(
            '{"flashcards": [{"id": "1"}]}', default_key="flashcards"
        )
        assert len(result) == 1

    def test_strips_markdown_fences(self):
        from utils.summarizer import safe_parse_json
        result = safe_parse_json('```json\n[{"id": "1"}]\n```')
        assert len(result) == 1

    def test_malformed_json_returns_empty(self):
        from utils.summarizer import safe_parse_json
        result = safe_parse_json("not valid json at all")
        assert result == []


class TestSummarizeChunk:

    def test_calls_openai_with_mini_model_by_default(self, openai_env):
        with patch("utils.summarizer.OpenAI") as mock_openai_cls:
            mock_client = MagicMock()
            mock_openai_cls.return_value = mock_client
            mock_client.chat.completions.create.return_value = make_mock_completion(
                "## Test concept\n\nExplanation."
            )

            from utils.summarizer import summarize_chunk
            summary, usage = summarize_chunk("some transcript text")

            assert "Test concept" in summary
            call_kwargs = mock_client.chat.completions.create.call_args.kwargs
            assert call_kwargs["model"] == "gpt-4o-mini"
            assert usage["input_tokens"] == 100


class TestGenerateFlashcards:

    def test_parses_valid_json_response(self, openai_env):
        with patch("utils.summarizer.OpenAI") as mock_openai_cls:
            mock_client = MagicMock()
            mock_openai_cls.return_value = mock_client
            mock_client.chat.completions.create.return_value = make_mock_completion(
                '{"flashcards": [{"id": "card_001", "question": "Q", "answer": "A", '
                '"concept": "X", "difficulty": "beginner", "tags": ["sql"]}]}'
            )

            from utils.summarizer import generate_flashcards
            cards, usage = generate_flashcards("transcript")
            assert len(cards) == 1
            assert cards[0]["id"] == "card_001"


class TestAskQuestion:

    def test_returns_message_when_no_chunks(self, openai_env):
        from utils.summarizer import ask_question
        answer, usage = ask_question("What is SQL?", chunks=[])
        assert "No transcript data" in answer

    def test_retrieves_relevant_chunk(self, openai_env):
        with patch("utils.summarizer.OpenAI") as mock_openai_cls:
            mock_client = MagicMock()
            mock_openai_cls.return_value = mock_client
            mock_client.chat.completions.create.return_value = make_mock_completion(
                "SQL is a query language for relational databases."
            )

            chunks = [
                {"chunk_index": 0, "chunk_text": "Python is a general-purpose programming language.",
                 "chunk_start_seconds": 0, "chunk_end_seconds": 30},
                {"chunk_index": 1, "chunk_text": "SQL stands for Structured Query Language, used to query databases.",
                 "chunk_start_seconds": 30, "chunk_end_seconds": 60},
                {"chunk_index": 2, "chunk_text": "Machine learning requires statistics and calculus.",
                 "chunk_start_seconds": 60, "chunk_end_seconds": 90},
            ]

            from utils.summarizer import ask_question
            answer, usage = ask_question("What is SQL?", chunks, top_k=2)

            assert "SQL" in answer
            # Verify that chunk 1 (most relevant) was included in the prompt
            call_kwargs = mock_client.chat.completions.create.call_args.kwargs
            user_msg = call_kwargs["messages"][1]["content"]
            assert "Structured Query Language" in user_msg
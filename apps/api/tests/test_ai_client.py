"""Tests for the Anthropic client wrapper.

The wrapper is small but does several things that matter:
  * inserts cache_control on the system prompt when cache_system=True
  * records token usage on the singleton TokenLedger
  * strips ```json fences before json.loads
  * raises ValueError (not JSONDecodeError) on bad model JSON

We mock `anthropic.Anthropic` so these tests run with no network access
and no API key.
"""

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest


# Force settings.anthropic_api_key to a non-empty value for the duration
# of the module so AIClient.__post_init__ doesn't refuse to instantiate.
@pytest.fixture(autouse=True)
def _ensure_api_key(monkeypatch):
    monkeypatch.setenv("ANTHROPIC_API_KEY", "test-key")
    # The settings object reads at import time; patch its attribute directly.
    from src import config

    monkeypatch.setattr(config.settings, "anthropic_api_key", "test-key", raising=False)
    yield


def _stub_response(text: str, *, in_tok=10, out_tok=5):
    text_block = SimpleNamespace(type="text", text=text)
    usage = SimpleNamespace(
        input_tokens=in_tok,
        output_tokens=out_tok,
        cache_read_input_tokens=0,
        cache_creation_input_tokens=0,
    )
    return SimpleNamespace(content=[text_block], usage=usage)


class TestAIClientShape:
    @patch("src.ai.client.Anthropic")
    def test_complete_returns_text(self, MockClient):
        from src.ai.client import AIClient

        instance = MagicMock()
        MockClient.return_value = instance
        instance.messages.create.return_value = _stub_response("hello world")

        c = AIClient(model="claude-test", feature="unit")
        out = c.complete(system="sys", user="hi")
        assert out == "hello world"

    @patch("src.ai.client.Anthropic")
    def test_system_prompt_is_cached_by_default(self, MockClient):
        from src.ai.client import AIClient

        instance = MagicMock()
        MockClient.return_value = instance
        instance.messages.create.return_value = _stub_response("ok")

        c = AIClient(model="claude-test", feature="unit")
        c.complete(system="big stable prompt", user="q")

        kwargs = instance.messages.create.call_args.kwargs
        assert kwargs["system"] == [
            {"type": "text", "text": "big stable prompt", "cache_control": {"type": "ephemeral"}}
        ]

    @patch("src.ai.client.Anthropic")
    def test_can_disable_caching(self, MockClient):
        from src.ai.client import AIClient

        instance = MagicMock()
        MockClient.return_value = instance
        instance.messages.create.return_value = _stub_response("ok")

        c = AIClient(model="claude-test", feature="unit")
        c.complete(system="prompt", user="q", cache_system=False)

        kwargs = instance.messages.create.call_args.kwargs
        sys_blocks = kwargs["system"]
        assert sys_blocks == [{"type": "text", "text": "prompt"}]


class TestTokenLedger:
    @patch("src.ai.client.Anthropic")
    def test_records_usage_with_feature(self, MockClient):
        from src.ai.client import AIClient, get_ledger

        get_ledger().reset()

        instance = MagicMock()
        MockClient.return_value = instance
        instance.messages.create.return_value = _stub_response("ok", in_tok=42, out_tok=7)

        AIClient(model="m", feature="unit-test").complete(system="s", user="u")

        records = get_ledger().by_feature("unit-test")
        assert len(records) == 1
        assert records[0].input_tokens == 42
        assert records[0].output_tokens == 7
        assert records[0].model == "m"


class TestJsonParsing:
    @patch("src.ai.client.Anthropic")
    def test_plain_json(self, MockClient):
        from src.ai.client import AIClient

        instance = MagicMock()
        MockClient.return_value = instance
        instance.messages.create.return_value = _stub_response('{"x": 1}')

        c = AIClient(feature="unit")
        assert c.complete_json(system="s", user="u") == {"x": 1}

    @patch("src.ai.client.Anthropic")
    def test_strips_fenced_json(self, MockClient):
        from src.ai.client import AIClient

        instance = MagicMock()
        MockClient.return_value = instance
        fenced = '```json\n{"x": [1, 2]}\n```'
        instance.messages.create.return_value = _stub_response(fenced)

        c = AIClient(feature="unit")
        assert c.complete_json(system="s", user="u") == {"x": [1, 2]}

    @patch("src.ai.client.Anthropic")
    def test_strips_unlabeled_fence(self, MockClient):
        from src.ai.client import AIClient

        instance = MagicMock()
        MockClient.return_value = instance
        instance.messages.create.return_value = _stub_response('```\n{"a": true}\n```')
        c = AIClient(feature="unit")
        assert c.complete_json(system="s", user="u") == {"a": True}

    @patch("src.ai.client.Anthropic")
    def test_raises_value_error_on_bad_json(self, MockClient):
        from src.ai.client import AIClient

        instance = MagicMock()
        MockClient.return_value = instance
        instance.messages.create.return_value = _stub_response("not json")
        c = AIClient(feature="unit")
        with pytest.raises(ValueError):
            c.complete_json(system="s", user="u")


class TestModelSelection:
    @patch("src.ai.client.Anthropic")
    def test_fast_constructor_uses_fast_model(self, MockClient):
        from src.ai import client as c
        from src.ai.client import AIClient

        instance = MagicMock()
        MockClient.return_value = instance
        instance.messages.create.return_value = _stub_response("ok")

        ai = AIClient.fast(feature="unit")
        ai.complete(system="s", user="u")
        kwargs = instance.messages.create.call_args.kwargs
        assert kwargs["model"] == c.FAST_MODEL

    @patch("src.ai.client.Anthropic")
    def test_smart_constructor_uses_smart_model(self, MockClient):
        from src.ai import client as c
        from src.ai.client import AIClient

        instance = MagicMock()
        MockClient.return_value = instance
        instance.messages.create.return_value = _stub_response("ok")

        ai = AIClient.smart(feature="unit")
        ai.complete(system="s", user="u")
        kwargs = instance.messages.create.call_args.kwargs
        assert kwargs["model"] == c.SMART_MODEL


class TestApiKeyEnforcement:
    def test_missing_key_raises(self, monkeypatch):
        from src.ai.client import AIClient
        from src import config

        monkeypatch.setattr(config.settings, "anthropic_api_key", "", raising=False)
        with pytest.raises(RuntimeError, match="ANTHROPIC_API_KEY"):
            AIClient()

"""Tests for the token usage / cost summary endpoint."""

from fastapi import FastAPI
from fastapi.testclient import TestClient

import pytest

from src.ai.client import TokenUsage, get_ledger
from src.api.routes.usage import (
    estimate_cost,
    rates_for,
    summarize,
    FALLBACK_RATES,
)


@pytest.fixture(autouse=True)
def reset_ledger():
    get_ledger().reset()
    yield
    get_ledger().reset()


@pytest.fixture
def client():
    from src.api.routes.usage import router

    app = FastAPI()
    app.include_router(router)
    return TestClient(app)


# ─── Pricing table ───────────────────────────────────────────────────────────


class TestPricing:
    def test_haiku_prefix_match(self):
        r = rates_for("claude-haiku-4-5-20251001")
        assert r.input_per_million == 0.25 and r.output_per_million == 1.25

    def test_sonnet_prefix_match(self):
        r = rates_for("claude-sonnet-4-6")
        assert r.input_per_million == 3.0 and r.output_per_million == 15.0

    def test_opus_prefix_match(self):
        r = rates_for("claude-opus-4-7")
        assert r.input_per_million == 15.0 and r.output_per_million == 75.0

    def test_unknown_model_uses_fallback(self):
        assert rates_for("claude-future-9-0") is FALLBACK_RATES


class TestEstimateCost:
    def test_input_only(self):
        u = TokenUsage(model="claude-haiku-4-5", input_tokens=1_000_000, output_tokens=0)
        # Haiku input = $0.25/M => $0.25 for 1M tokens.
        assert estimate_cost(u) == 0.25

    def test_input_and_output(self):
        u = TokenUsage(model="claude-sonnet-4-6", input_tokens=1_000_000, output_tokens=1_000_000)
        # Sonnet: $3 input + $15 output = $18.
        assert estimate_cost(u) == 18.0

    def test_cache_read_is_cheaper_than_input(self):
        u = TokenUsage(
            model="claude-sonnet-4-6",
            input_tokens=0,
            output_tokens=0,
            cache_read_input_tokens=1_000_000,
        )
        # Cache read = $0.30 for sonnet (vs $3 base input).
        assert estimate_cost(u) == 0.30

    def test_cache_write_is_more_expensive_than_input(self):
        u = TokenUsage(
            model="claude-sonnet-4-6",
            input_tokens=0,
            output_tokens=0,
            cache_creation_input_tokens=1_000_000,
        )
        assert estimate_cost(u) == 3.75

    def test_zero_usage_zero_cost(self):
        u = TokenUsage(model="claude-haiku-4-5", input_tokens=0, output_tokens=0)
        assert estimate_cost(u) == 0.0

    def test_subtoken_counts_round(self):
        u = TokenUsage(model="claude-haiku-4-5", input_tokens=1, output_tokens=1)
        # 1*0.25/1M + 1*1.25/1M = 1.5e-6 -> rounded to 0.000002 (6 decimals).
        assert estimate_cost(u) == 0.000002


# ─── summarize() pure aggregation ────────────────────────────────────────────


class TestSummarize:
    def test_empty_records(self):
        s = summarize([])
        assert s.total_calls == 0
        assert s.total_estimated_cost_usd == 0.0
        assert s.by_feature == []
        assert s.by_model == []

    def test_groups_by_feature(self):
        records = [
            TokenUsage(
                model="claude-sonnet-4-6",
                input_tokens=100,
                output_tokens=50,
                feature="app_impact",
                latency_ms=200,
            ),
            TokenUsage(
                model="claude-sonnet-4-6",
                input_tokens=300,
                output_tokens=150,
                feature="app_impact",
                latency_ms=400,
            ),
            TokenUsage(
                model="claude-opus-4-7",
                input_tokens=500,
                output_tokens=250,
                feature="runbook",
                latency_ms=1000,
            ),
        ]
        s = summarize(records)
        features = {f.feature: f for f in s.by_feature}
        assert features["app_impact"].calls == 2
        assert features["app_impact"].input_tokens == 400
        assert features["app_impact"].output_tokens == 200
        assert features["app_impact"].avg_latency_ms == 300
        assert features["runbook"].calls == 1
        assert features["runbook"].input_tokens == 500

    def test_groups_by_model(self):
        records = [
            TokenUsage(model="claude-sonnet-4-6", input_tokens=10, output_tokens=5),
            TokenUsage(model="claude-opus-4-7", input_tokens=10, output_tokens=5),
            TokenUsage(model="claude-opus-4-7", input_tokens=20, output_tokens=10),
        ]
        s = summarize(records)
        models = {m.model: m for m in s.by_model}
        assert models["claude-opus-4-7"].calls == 2
        assert models["claude-opus-4-7"].input_tokens == 30
        assert models["claude-sonnet-4-6"].calls == 1

    def test_total_cost_sums_per_record_costs(self):
        records = [
            TokenUsage(model="claude-haiku-4-5", input_tokens=1_000_000, output_tokens=0),
            TokenUsage(model="claude-haiku-4-5", input_tokens=0, output_tokens=1_000_000),
        ]
        s = summarize(records)
        # 0.25 + 1.25 = 1.50
        assert s.total_estimated_cost_usd == 1.50

    def test_default_feature_label(self):
        records = [TokenUsage(model="claude-haiku-4-5", input_tokens=1, output_tokens=1)]
        s = summarize(records)
        assert s.by_feature[0].feature == "default"


# ─── Route ───────────────────────────────────────────────────────────────────


class TestUsageEndpoint:
    def test_empty_ledger_returns_zeros(self, client):
        resp = client.get("/api/v3/usage/summary")
        assert resp.status_code == 200
        body = resp.json()
        assert body["total_calls"] == 0
        assert body["total_estimated_cost_usd"] == 0.0
        assert body["by_feature"] == []

    def test_after_recording_aggregates_correctly(self, client):
        get_ledger().record(
            TokenUsage(
                model="claude-haiku-4-5",
                input_tokens=1_000_000,
                output_tokens=1_000_000,
                feature="app_impact",
                latency_ms=300,
            )
        )
        get_ledger().record(
            TokenUsage(
                model="claude-opus-4-7",
                input_tokens=100_000,
                output_tokens=50_000,
                feature="runbook",
                latency_ms=2000,
            )
        )

        resp = client.get("/api/v3/usage/summary")
        assert resp.status_code == 200
        body = resp.json()

        assert body["total_calls"] == 2
        assert body["total_input_tokens"] == 1_100_000
        assert body["total_output_tokens"] == 1_050_000
        # Haiku: 0.25 + 1.25 = 1.50; Opus: 0.1*15 + 0.05*75 = 1.5 + 3.75 = 5.25; total 6.75
        assert body["total_estimated_cost_usd"] == 6.75

        features = {f["feature"]: f for f in body["by_feature"]}
        assert features["app_impact"]["estimated_cost_usd"] == 1.50
        assert features["runbook"]["estimated_cost_usd"] == 5.25

    def test_response_keys_present(self, client):
        resp = client.get("/api/v3/usage/summary")
        body = resp.json()
        for key in (
            "total_calls",
            "total_input_tokens",
            "total_output_tokens",
            "total_cache_read_tokens",
            "total_cache_creation_tokens",
            "total_estimated_cost_usd",
            "by_feature",
            "by_model",
        ):
            assert key in body, f"missing {key}"

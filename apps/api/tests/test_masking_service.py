"""Unit tests for `src.services.masking_service`.

Pure-function tests — no DB, no Runner — covering each strategy's
correctness and the rule-compiler's error path.
"""

from __future__ import annotations

import hashlib
import hmac
from types import SimpleNamespace

import pytest

from src.services import masking_service


def _spec(qualified: str, columns: list[str]):
    return SimpleNamespace(
        source_table=SimpleNamespace(qualified=lambda q=qualified: q),
        columns=columns,
    )


# ─── validate_rules ──────────────────────────────────────────────────


def test_validate_accepts_minimal_shape():
    masking_service.validate_rules({"S.T": {"C": {"strategy": "null"}}})


@pytest.mark.parametrize(
    "bad",
    [
        [],                                      # not a dict
        {"S.T": "not-a-dict"},                  # column block not dict
        {"S.T": {"C": "not-a-rule"}},           # rule not dict
        {"S.T": {"C": {"strategy": "nope"}}},   # unknown strategy
        {"S.T": {"C": {"strategy": "partial", "keep_first": -1}}},
        {"S.T": {"C": {"strategy": "regex"}}},  # missing pattern
        {"S.T": {"C": {"strategy": "regex", "pattern": "(bad"}}},
        {"S.T": {"C": {"strategy": "hash", "length": 128}}},
    ],
)
def test_validate_rejects_bad_shapes(bad):
    with pytest.raises(ValueError):
        masking_service.validate_rules(bad)


# ─── per-strategy behavior ───────────────────────────────────────────


def test_null_strategy(monkeypatch):
    monkeypatch.setenv("HAFEN_MASKING_KEY", "k")
    tr = masking_service.build_row_transform(
        {"S.T": {"C": {"strategy": "null"}}}
    )
    out = tr([(1, "sensitive")], _spec("S.T", ["id", "C"]))
    assert out == [(1, None)]


def test_fixed_strategy_with_custom_value(monkeypatch):
    monkeypatch.setenv("HAFEN_MASKING_KEY", "k")
    tr = masking_service.build_row_transform(
        {"S.T": {"C": {"strategy": "fixed", "value": "XXX"}}}
    )
    out = tr([(1, "anything")], _spec("S.T", ["id", "C"]))
    assert out == [(1, "XXX")]


def test_fixed_strategy_default_redacted(monkeypatch):
    monkeypatch.setenv("HAFEN_MASKING_KEY", "k")
    tr = masking_service.build_row_transform(
        {"S.T": {"C": {"strategy": "fixed"}}}
    )
    out = tr([(1, "anything")], _spec("S.T", ["id", "C"]))
    assert out[0][1] == "[REDACTED]"


def test_hash_is_deterministic(monkeypatch):
    monkeypatch.setenv("HAFEN_MASKING_KEY", "secret-abc")
    tr = masking_service.build_row_transform(
        {"S.T": {"C": {"strategy": "hash"}}}
    )
    a = tr([(1, "alice@example.com")], _spec("S.T", ["id", "C"]))
    b = tr([(2, "alice@example.com")], _spec("S.T", ["id", "C"]))
    assert a[0][1] == b[0][1]


def test_hash_preserves_cross_table_fk_integrity(monkeypatch):
    """Same value in two different tables/columns → same hash output,
    so JOINs still work after masking."""
    monkeypatch.setenv("HAFEN_MASKING_KEY", "secret-abc")
    tr = masking_service.build_row_transform(
        {
            "S.USERS": {"EMAIL": {"strategy": "hash"}},
            "S.ORDERS": {"USER_EMAIL": {"strategy": "hash"}},
        }
    )
    u = tr([(1, "alice@example.com")], _spec("S.USERS", ["id", "EMAIL"]))
    o = tr([(9, "alice@example.com")], _spec("S.ORDERS", ["id", "USER_EMAIL"]))
    assert u[0][1] == o[0][1]


def test_hash_length_configurable(monkeypatch):
    monkeypatch.setenv("HAFEN_MASKING_KEY", "k")
    tr = masking_service.build_row_transform(
        {"S.T": {"C": {"strategy": "hash", "length": 8}}}
    )
    out = tr([(1, "x")], _spec("S.T", ["id", "C"]))
    assert len(out[0][1]) == 8


def test_hash_matches_external_hmac(monkeypatch):
    """Hash output is plain HMAC-SHA256 hex so subscribers can
    reproduce it without our code — verify the math directly."""
    monkeypatch.setenv("HAFEN_MASKING_KEY", "secret-abc")
    tr = masking_service.build_row_transform(
        {"S.T": {"C": {"strategy": "hash", "length": 64}}}
    )
    out = tr([(1, "hello")], _spec("S.T", ["id", "C"]))
    expected = hmac.new(b"secret-abc", b"hello", hashlib.sha256).hexdigest()
    assert out[0][1] == expected


def test_partial_keep_first_and_last(monkeypatch):
    monkeypatch.setenv("HAFEN_MASKING_KEY", "k")
    tr = masking_service.build_row_transform(
        {"S.T": {"C": {"strategy": "partial", "keep_first": 1, "keep_last": 4}}}
    )
    out = tr([(1, "123-45-6789")], _spec("S.T", ["id", "C"]))
    assert out[0][1] == "1******6789"


def test_partial_short_input_fully_masked(monkeypatch):
    """Inputs shorter than keep_first+keep_last get fully masked — never
    leak more than the operator asked to preserve."""
    monkeypatch.setenv("HAFEN_MASKING_KEY", "k")
    tr = masking_service.build_row_transform(
        {"S.T": {"C": {"strategy": "partial", "keep_first": 2, "keep_last": 2}}}
    )
    out = tr([(1, "abc")], _spec("S.T", ["id", "C"]))
    assert out[0][1] == "***"


def test_regex_replacement(monkeypatch):
    monkeypatch.setenv("HAFEN_MASKING_KEY", "k")
    tr = masking_service.build_row_transform(
        {
            "S.T": {
                "C": {
                    "strategy": "regex",
                    "pattern": r"\d{3}-\d{2}-\d{4}",
                    "replacement": "XXX-XX-XXXX",
                }
            }
        }
    )
    out = tr([(1, "ssn is 123-45-6789 here")], _spec("S.T", ["id", "C"]))
    assert out[0][1] == "ssn is XXX-XX-XXXX here"


def test_none_input_preserved(monkeypatch):
    """Each strategy except `fixed` must pass through None unchanged —
    masking should not replace NULLs with garbage."""
    monkeypatch.setenv("HAFEN_MASKING_KEY", "k")
    for strat in ("null", "hash", "partial", "regex"):
        rule = {"strategy": strat}
        if strat == "regex":
            rule["pattern"] = ".*"
        elif strat == "partial":
            rule["keep_first"] = 1
            rule["keep_last"] = 1
        tr = masking_service.build_row_transform({"S.T": {"C": rule}})
        out = tr([(1, None)], _spec("S.T", ["id", "C"]))
        assert out[0][1] is None, f"{strat} should preserve None"


# ─── transform-builder behavior ──────────────────────────────────────


def test_transform_skips_unmapped_tables(monkeypatch):
    """A spec whose source_table isn't in the rules dict passes
    through unchanged."""
    monkeypatch.setenv("HAFEN_MASKING_KEY", "k")
    tr = masking_service.build_row_transform(
        {"HR.EMPLOYEES": {"EMAIL": {"strategy": "null"}}}
    )
    # Different table; rule doesn't apply.
    out = tr([(1, "alice@ex.com")], _spec("HR.DEPARTMENTS", ["id", "NAME"]))
    assert out == [(1, "alice@ex.com")]


def test_transform_skips_missing_columns(monkeypatch):
    """Rule references a column that isn't in the spec → logs and
    skips, doesn't crash."""
    monkeypatch.setenv("HAFEN_MASKING_KEY", "k")
    tr = masking_service.build_row_transform(
        {"S.T": {"NONEXISTENT": {"strategy": "null"}}}
    )
    out = tr([(1, "kept")], _spec("S.T", ["id", "REAL_COL"]))
    assert out == [(1, "kept")]


def test_other_columns_passthrough(monkeypatch):
    monkeypatch.setenv("HAFEN_MASKING_KEY", "k")
    tr = masking_service.build_row_transform(
        {"S.T": {"EMAIL": {"strategy": "null"}}}
    )
    out = tr(
        [(1, "alice@ex.com", "Alice", 42)],
        _spec("S.T", ["id", "EMAIL", "NAME", "AGE"]),
    )
    assert out == [(1, None, "Alice", 42)]


# ─── HMAC key resolution ─────────────────────────────────────────────


def test_raises_if_no_key_set_and_hash_used(monkeypatch):
    monkeypatch.delenv("HAFEN_MASKING_KEY", raising=False)
    monkeypatch.delenv("HAFEN_ENCRYPTION_KEY", raising=False)
    with pytest.raises(RuntimeError, match="HAFEN_MASKING_KEY"):
        masking_service.build_row_transform(
            {"S.T": {"C": {"strategy": "hash"}}}
        )


def test_no_key_needed_if_no_hash(monkeypatch):
    """If no rule uses `hash`, we never need an HMAC key — build
    succeeds without one. Important so operators can use null/fixed
    masking without configuring a key."""
    monkeypatch.delenv("HAFEN_MASKING_KEY", raising=False)
    monkeypatch.delenv("HAFEN_ENCRYPTION_KEY", raising=False)
    tr = masking_service.build_row_transform(
        {"S.T": {"C": {"strategy": "null"}}}
    )
    out = tr([(1, "x")], _spec("S.T", ["id", "C"]))
    assert out == [(1, None)]


def test_falls_back_to_encryption_key(monkeypatch):
    monkeypatch.delenv("HAFEN_MASKING_KEY", raising=False)
    monkeypatch.setenv("HAFEN_ENCRYPTION_KEY", "fallback-key")
    tr = masking_service.build_row_transform(
        {"S.T": {"C": {"strategy": "hash"}}}
    )
    out = tr([(1, "x")], _spec("S.T", ["id", "C"]))
    expected = hmac.new(b"fallback-key", b"x", hashlib.sha256).hexdigest()[:32]
    assert out[0][1] == expected


# ─── (de)serialization helpers ───────────────────────────────────────


def test_load_empty_text_yields_empty_dict():
    assert masking_service.load_rules_from_text(None) == {}
    assert masking_service.load_rules_from_text("") == {}


def test_load_round_trip():
    rules = {"S.T": {"C": {"strategy": "null"}}}
    blob = masking_service.dump_rules_to_text(rules)
    assert masking_service.load_rules_from_text(blob) == rules


def test_load_rejects_bad_json():
    with pytest.raises(ValueError):
        masking_service.load_rules_from_text("{not json")

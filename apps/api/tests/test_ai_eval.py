"""Tests for the AI prompt eval harness.

Scorer rules are pure functions tested in isolation. The runner is
tested with a stub invoker so no LLM is called. Suite loading is tested
against the bundled corpus to lock in the JSONL format.
"""

import json

import pytest

from src.ai.eval import (
    EvalCase,
    EvalRunner,
    ScoreRule,
    load_suite,
)
from src.ai.eval.runner import format_report
from src.ai.eval.scorer import evaluate


def _rule(kind: str, config) -> ScoreRule:
    return ScoreRule(kind=kind, config=config)


# ─── Scorer rules ────────────────────────────────────────────────────────────


class TestMustContain:
    def test_passes_when_all_substrings_present_case_insensitive(self):
        r = evaluate(
            "Use COALESCE in PostgreSQL.", [_rule("must_contain", ["coalesce", "postgres"])]
        )
        assert r[0].passed

    def test_fails_with_missing_substrings_listed_in_detail(self):
        r = evaluate("Use COALESCE.", [_rule("must_contain", ["COALESCE", "MERGE"])])
        assert not r[0].passed
        assert "MERGE" in r[0].detail


class TestMustNotContain:
    def test_passes_when_forbidden_absent(self):
        r = evaluate(
            "Replace NVL with COALESCE.",
            [_rule("must_not_contain", ["might want to", "could potentially"])],
        )
        assert r[0].passed

    def test_fails_when_forbidden_present(self):
        r = evaluate(
            "You might want to consider...", [_rule("must_not_contain", ["might want to"])]
        )
        assert not r[0].passed
        assert "might want to" in r[0].detail


class TestJsonRules:
    def test_must_have_keys_passes(self):
        r = evaluate(
            '{"executive_summary": "x", "risk_narrative": "y"}',
            [_rule("json_must_have_keys", ["executive_summary", "risk_narrative"])],
        )
        assert r[0].passed

    def test_must_have_keys_fails_when_missing(self):
        r = evaluate(
            '{"executive_summary": "x"}',
            [_rule("json_must_have_keys", ["executive_summary", "risk_narrative"])],
        )
        assert not r[0].passed
        assert "risk_narrative" in r[0].detail

    def test_path_equals_passes(self):
        r = evaluate(
            '{"findings": [{"code": "APP.SQL.NVL"}]}',
            [_rule("json_path_equals", {"findings.0.code": "APP.SQL.NVL"})],
        )
        assert r[0].passed

    def test_path_equals_fails_with_actual_listed(self):
        r = evaluate(
            '{"findings": [{"code": "OTHER"}]}',
            [_rule("json_path_equals", {"findings.0.code": "APP.SQL.NVL"})],
        )
        assert not r[0].passed
        assert "OTHER" in r[0].detail

    def test_array_min_len_passes(self):
        r = evaluate('{"findings": [1, 2, 3]}', [_rule("json_array_min_len", {"findings": 3})])
        assert r[0].passed

    def test_array_min_len_fails_when_too_short(self):
        r = evaluate('{"findings": [1]}', [_rule("json_array_min_len", {"findings": 3})])
        assert not r[0].passed
        assert "len 1 < 3" in r[0].detail


class TestJsonFences:
    def test_strips_json_fence(self):
        r = evaluate('```json\n{"k": 1}\n```', [_rule("json_must_have_keys", ["k"])])
        assert r[0].passed

    def test_strips_unlabeled_fence(self):
        r = evaluate('```\n{"k": 1}\n```', [_rule("json_must_have_keys", ["k"])])
        assert r[0].passed


class TestLengthRules:
    def test_max_chars_passes(self):
        assert evaluate("hi", [_rule("max_chars", 10)])[0].passed

    def test_max_chars_fails(self):
        r = evaluate("a" * 100, [_rule("max_chars", 10)])
        assert not r[0].passed and "100 > 10" in r[0].detail

    def test_min_chars(self):
        assert not evaluate("hi", [_rule("min_chars", 5)])[0].passed


class TestRuleErrorHandling:
    def test_unknown_rule_kind(self):
        r = evaluate("anything", [_rule("nonexistent", None)])
        assert not r[0].passed and "unknown rule kind" in r[0].detail

    def test_handler_exception_does_not_propagate(self):
        # Bad JSON triggers handler exception path.
        r = evaluate("not json at all", [_rule("json_must_have_keys", ["k"])])
        assert not r[0].passed
        # detail should mention the failure mode (JSONDecodeError shape)
        assert "rule raised" in r[0].detail or "JSON" in r[0].detail


# ─── ScoreRule.from_dict ─────────────────────────────────────────────────────


class TestScoreRuleFromDict:
    def test_single_key_dict(self):
        sr = ScoreRule.from_dict({"must_contain": ["x"]})
        assert sr.kind == "must_contain" and sr.config == ["x"]

    def test_multi_key_dict_rejected(self):
        with pytest.raises(ValueError):
            ScoreRule.from_dict({"a": 1, "b": 2})


# ─── Runner ──────────────────────────────────────────────────────────────────


def _stub_invoker(canned: dict):
    """Returns an invoker that maps case input → canned response.

    Match key is `case.inputs["id"]` so each fixture can dictate what
    the "LLM" returns for it.
    """

    def invoke(inputs):
        return canned[inputs["__case_id"]]

    return invoke


def _suite_with(cases):
    from src.ai.eval.types import EvalSuite

    return EvalSuite(prompt_id="test", cases=cases)


class TestRunner:
    def test_records_pass_and_fail_counts(self):
        cases = [
            EvalCase(id="a", inputs={"__case_id": "a"}, rules=[_rule("must_contain", ["good"])]),
            EvalCase(id="b", inputs={"__case_id": "b"}, rules=[_rule("must_contain", ["good"])]),
        ]
        canned = {"a": "this is good", "b": "this is bad"}
        result = EvalRunner(invoke=_stub_invoker(canned)).run(_suite_with(cases))
        assert result.total == 2
        assert result.passed == 1
        assert result.failed == 1
        assert result.pass_rate == 0.5

    def test_invoker_exception_marked_as_failed_with_error(self):
        def bad_invoke(inputs):
            raise RuntimeError("API down")

        cases = [EvalCase(id="a", inputs={}, rules=[])]
        result = EvalRunner(invoke=bad_invoke).run(_suite_with(cases))
        assert not result.cases[0].passed
        assert "API down" in result.cases[0].error

    def test_records_latency(self):
        cases = [EvalCase(id="a", inputs={"__case_id": "a"}, rules=[_rule("must_contain", ["x"])])]
        result = EvalRunner(invoke=_stub_invoker({"a": "x"})).run(_suite_with(cases))
        assert result.cases[0].latency_ms >= 0


class TestFormatReport:
    def test_includes_per_case_status(self):
        cases = [
            EvalCase(
                id="case-x", inputs={"__case_id": "case-x"}, rules=[_rule("must_contain", ["y"])]
            )
        ]
        result = EvalRunner(invoke=_stub_invoker({"case-x": "y"})).run(_suite_with(cases))
        report = format_report(result)
        assert "case-x" in report
        assert "PASS" in report

    def test_includes_pass_rate(self):
        cases = [
            EvalCase(id="a", inputs={"__case_id": "a"}, rules=[_rule("must_contain", ["y"])]),
            EvalCase(id="b", inputs={"__case_id": "b"}, rules=[_rule("must_contain", ["y"])]),
        ]
        result = EvalRunner(invoke=_stub_invoker({"a": "y", "b": "no"})).run(_suite_with(cases))
        assert "1/2" in format_report(result)

    def test_failed_rule_detail_in_report(self):
        cases = [
            EvalCase(
                id="case-fail",
                inputs={"__case_id": "case-fail"},
                rules=[_rule("must_contain", ["expected"])],
            )
        ]
        result = EvalRunner(invoke=_stub_invoker({"case-fail": "absent"})).run(_suite_with(cases))
        report = format_report(result)
        assert "FAIL" in report and "expected" in report


# ─── Bundled corpus loads + scores correctly ─────────────────────────────────


class TestBundledCorpus:
    def test_load_app_impact_suite(self):
        s = load_suite("app_impact")
        assert s.prompt_id == "app_impact"
        assert len(s.cases) >= 5
        for c in s.cases:
            assert c.id and c.rules, f"case {c.id} has no rules"

    def test_load_runbook_suite(self):
        s = load_suite("runbook")
        assert s.prompt_id == "runbook"
        assert len(s.cases) >= 3

    def test_missing_suite_raises(self):
        with pytest.raises(FileNotFoundError):
            load_suite("does_not_exist")

    def test_corpus_lines_skipped_for_comments(self, tmp_path):
        d = tmp_path / "x"
        d.mkdir()
        (d / "cases.jsonl").write_text(
            "// header comment\n"
            '{"id": "ok", "inputs": {}, "rules": [{"must_contain": ["x"]}]}\n'
            "\n"  # blank line
            "// another comment\n"
        )
        s = load_suite("x", root=tmp_path)
        assert len(s.cases) == 1 and s.cases[0].id == "ok"

    def test_synthetic_responses_against_app_impact_corpus(self):
        """Verify the corpus rules actually pass when given a 'good' response.

        Each case feeds one finding to the prompt, so a realistic AI response
        returns one finding back with the matching code. We synthesize that
        per-case response and assert every case passes — locks in the
        contract so corpus edits can't accidentally make every rule fail.
        """
        suite = load_suite("app_impact")
        # Per-case responses tailored to satisfy the rules in each case.
        case_responses = {
            "rownum-medium": json.dumps(
                {
                    "findings": [
                        {
                            "code": "APP.SQL.ROWNUM",
                            "explanation": "Use LIMIT or ROW_NUMBER() OVER instead.",
                            "before": "SELECT * FROM employees WHERE ROWNUM <= 10",
                            "after": "SELECT * FROM employees ORDER BY id LIMIT 10",
                            "caveats": [],
                        }
                    ]
                }
            ),
            "nvl-medium": json.dumps(
                {
                    "findings": [
                        {
                            "code": "APP.SQL.FN.NVL",
                            "explanation": "Replace NVL with COALESCE.",
                            "before": "NVL(name, '-')",
                            "after": "COALESCE(name, '-')",
                            "caveats": [],
                        }
                    ]
                }
            ),
            "dblink-critical": json.dumps(
                {
                    "findings": [
                        {
                            "code": "APP.SQL.DBLINK",
                            "explanation": "Replace dblink with postgres_fdw or move the join to the app.",
                            "before": "INSERT INTO local_t SELECT * FROM remote_t@prod_link",
                            "after": "INSERT INTO local_t SELECT * FROM remote_t  -- via postgres_fdw",
                            "caveats": [],
                        }
                    ]
                }
            ),
            "merge-high": json.dumps(
                {
                    "findings": [
                        {
                            "code": "APP.SQL.MERGE",
                            "explanation": "Use MERGE on PG 15+ or rewrite as INSERT ... ON CONFLICT.",
                            "before": "MERGE INTO orders t USING staging s ON ...",
                            "after": "INSERT INTO orders ... ON CONFLICT (id) DO UPDATE ...",
                            "caveats": [],
                        }
                    ]
                }
            ),
            "outer-join-plus-critical": json.dumps(
                {
                    "findings": [
                        {
                            "code": "APP.SQL.OUTER_JOIN_PLUS",
                            "explanation": "Rewrite as ANSI LEFT OUTER JOIN.",
                            "before": "FROM orders o, customers c WHERE o.customer_id = c.id [oracle-plus]",
                            "after": "FROM orders o LEFT JOIN customers c ON o.customer_id = c.id",
                            "caveats": [],
                        }
                    ]
                }
            ),
        }

        def invoke(inputs):
            # Each case carries no `id` in inputs; we recover it by matching
            # the unique code in the input.
            code = inputs["findings"][0]["code"]
            for cid, case in zip([c.id for c in suite.cases], [c for c in suite.cases]):
                if case.inputs["findings"][0]["code"] == code:
                    return case_responses[cid]
            raise KeyError(code)

        result = EvalRunner(invoke=invoke).run(suite)
        failures = [
            (c.case_id, [r.detail for r in c.rule_results if not r.passed])
            for c in result.cases
            if not c.passed
        ]
        assert result.failed == 0, f"corpus failed against good response: {failures}"

"""Tests for the app-impact analyzer — classifier rules + end-to-end run
against the Java/Python/SQL fixtures under tests/fixtures/app_impact/."""
from pathlib import Path

import pytest

from src.analyze.app_impact import (
    AppImpactAnalyzer,
    AppImpactReport,
    Finding,
    RiskLevel,
)
from src.core.ir.nodes import ConstructTag
from src.source.oracle.parser import parse


FIXTURES = Path(__file__).parent / "fixtures" / "app_impact"


@pytest.fixture(scope="module")
def schema():
    return parse((FIXTURES / "schema" / "schema.sql").read_text())


@pytest.fixture
def analyzer(schema):
    return AppImpactAnalyzer(schema=schema)


# ─── Risk-classifier behavior ────────────────────────────────────────────────


class TestClassifierRules:
    """Single-fragment classification — assert each rule fires and at the
    right risk level."""

    def _findings(self, analyzer, sql: str) -> list:
        from src.analyze.sql_extractor import SqlFragment
        frag = SqlFragment(sql=sql, file="<inline>", line=1)
        return analyzer._classify_fragment("<inline>", frag)

    def test_plain_select_no_findings(self, analyzer):
        # Plain SELECT against known table should produce no findings.
        assert self._findings(analyzer, "SELECT id, name FROM employees WHERE id = 1") == []

    def test_rownum_high(self, analyzer):
        fs = self._findings(analyzer, "SELECT * FROM employees WHERE ROWNUM <= 10")
        codes = {f.code for f in fs}
        assert "APP.SQL.ROWNUM" in codes
        rownum = next(f for f in fs if f.code == "APP.SQL.ROWNUM")
        assert rownum.risk == RiskLevel.HIGH

    def test_sysdate_medium(self, analyzer):
        fs = self._findings(analyzer, "UPDATE employees SET updated_at = SYSDATE WHERE id = 1")
        sysdate = [f for f in fs if f.code == "APP.SQL.FN.SYSDATE"]
        assert sysdate and sysdate[0].risk == RiskLevel.MEDIUM

    def test_nvl_medium(self, analyzer):
        fs = self._findings(analyzer, "SELECT NVL(notes, '-') FROM employees")
        nvl = [f for f in fs if f.code == "APP.SQL.FN.NVL"]
        assert nvl and nvl[0].risk == RiskLevel.MEDIUM

    def test_connect_by_high(self, analyzer):
        fs = self._findings(analyzer, "SELECT 1 FROM employees START WITH manager_id IS NULL CONNECT BY PRIOR id = manager_id")
        cb = [f for f in fs if f.code == "APP.SQL.CONNECT_BY"]
        assert cb and cb[0].risk == RiskLevel.HIGH

    def test_merge_high(self, analyzer):
        fs = self._findings(analyzer, "MERGE INTO employees t USING staging s ON (t.id = s.id) WHEN MATCHED THEN UPDATE SET t.name = s.name")
        m = [f for f in fs if f.code == "APP.SQL.MERGE"]
        assert m and m[0].risk == RiskLevel.HIGH

    def test_dbms_output_critical(self, analyzer):
        fs = self._findings(analyzer, "BEGIN DBMS_OUTPUT.PUT_LINE('hi'); END;")
        do = [f for f in fs if f.code == "APP.SQL.DBMS_OUTPUT"]
        assert do and do[0].risk == RiskLevel.CRITICAL

    def test_dblink_critical(self, analyzer):
        fs = self._findings(analyzer, "INSERT INTO local_t SELECT * FROM remote_t@prod_link")
        db = [f for f in fs if f.code == "APP.SQL.DBLINK"]
        assert db and db[0].risk == RiskLevel.CRITICAL

    def test_dual_critical(self, analyzer):
        fs = self._findings(analyzer, "SELECT SYSDATE FROM DUAL")
        dual = [f for f in fs if f.code == "APP.SQL.SYSREF.DUAL"]
        assert dual and dual[0].risk == RiskLevel.CRITICAL

    def test_outer_join_plus_critical(self, analyzer):
        fs = self._findings(
            analyzer,
            "SELECT o.id, c.name FROM orders o, customers c WHERE o.customer_id = c.id(+)"
        )
        oj = [f for f in fs if f.code == f"APP.SQL.{ConstructTag.OUTER_JOIN_PLUS.value}"]
        assert oj and oj[0].risk == RiskLevel.CRITICAL

    def test_unknown_table_critical(self, analyzer):
        fs = self._findings(analyzer, "SELECT * FROM legacy_audit WHERE id > 0")
        unk = [f for f in fs if f.code == "APP.SCHEMA.UNKNOWN_OBJECT"]
        assert unk and unk[0].risk == RiskLevel.CRITICAL
        assert "LEGACY_AUDIT" in unk[0].schema_objects

    def test_known_table_no_unknown_finding(self, analyzer):
        fs = self._findings(analyzer, "SELECT * FROM employees")
        unk = [f for f in fs if f.code == "APP.SCHEMA.UNKNOWN_OBJECT"]
        assert unk == []


# ─── Suggestions present and useful ──────────────────────────────────────────


class TestSuggestions:
    def test_each_finding_has_a_suggestion(self, analyzer):
        from src.analyze.sql_extractor import SqlFragment
        frag = SqlFragment(
            sql="UPDATE employees SET updated_at = SYSDATE WHERE ROWNUM <= 10",
            file="<inline>", line=1,
        )
        for f in analyzer._classify_fragment("<inline>", frag):
            assert f.suggestion and len(f.suggestion) > 10, f
            assert f.snippet
            assert f.line == 1


# ─── End-to-end against fixture files ────────────────────────────────────────


class TestEndToEnd:
    def test_java_fixture(self, analyzer):
        java_dir = FIXTURES / "java"
        report = analyzer.analyze_directory(java_dir, languages=["java"])
        assert report.total_files_scanned == 1
        # Multiple findings expected from the Oracle-specific patterns in the fixture.
        assert report.total_findings >= 6
        assert report.findings_by_risk.get("critical", 0) >= 3
        assert report.findings_by_risk.get("high", 0) >= 2
        assert report.findings_by_risk.get("medium", 0) >= 2

        fi = report.files[0]
        codes = {f.code for f in fi.findings}
        assert "APP.SQL.ROWNUM" in codes
        assert "APP.SQL.CONNECT_BY" in codes
        assert "APP.SQL.MERGE" in codes
        assert "APP.SQL.DBMS_OUTPUT" in codes
        assert "APP.SQL.DBLINK" in codes
        assert "APP.SQL.SYSREF.DUAL" in codes
        assert "APP.SQL.FN.NVL" in codes
        assert "APP.SQL.FN.SYSDATE" in codes
        assert "APP.SCHEMA.UNKNOWN_OBJECT" in codes  # legacy_audit_events

    def test_python_fixture(self, analyzer):
        py_dir = FIXTURES / "python"
        report = analyzer.analyze_directory(py_dir, languages=["python"])
        assert report.total_files_scanned == 1
        codes = {f.code for fi in report.files for f in fi.findings}
        assert "APP.SQL.ROWNUM" in codes
        assert "APP.SQL.FN.NVL" in codes
        assert "APP.SQL.OUTER_JOIN_PLUS" in codes
        assert "APP.SQL.DBMS_SCHEDULER" in codes
        assert "APP.SCHEMA.UNKNOWN_OBJECT" in codes

    def test_top_files_ordering(self, analyzer):
        report = analyzer.analyze_directory(FIXTURES, languages=["java", "python"])
        top = report.top_files(limit=5)
        # Files are ordered by max risk descending.
        for i in range(len(top) - 1):
            from src.analyze.app_impact import _rank
            assert _rank(top[i].max_risk) >= _rank(top[i + 1].max_risk)

    def test_unknown_extension_skipped(self, analyzer, tmp_path):
        (tmp_path / "notes.txt").write_text("SELECT * FROM employees")
        report = analyzer.analyze_directory(tmp_path)
        assert report.total_files_scanned == 0

    def test_directory_must_exist(self, analyzer, tmp_path):
        with pytest.raises(ValueError):
            analyzer.analyze_directory(tmp_path / "nope")


# ─── Report structure ────────────────────────────────────────────────────────


class TestReportShape:
    def test_findings_by_risk_aggregates(self, analyzer):
        report = analyzer.analyze_directory(FIXTURES / "java", languages=["java"])
        # All four risk levels appear in this fixture.
        for level in ("low", "medium", "high", "critical"):
            # Low isn't required (we don't emit findings for clean SQL),
            # but the others must be present.
            if level != "low":
                assert level in report.findings_by_risk, level

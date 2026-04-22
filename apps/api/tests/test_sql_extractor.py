"""Tests for the source-code SQL fragment extractors."""
import pytest

from src.analyze.sql_extractor import (
    JAVA_EXTRACTOR,
    PYTHON_EXTRACTOR,
    SQL_EXTRACTOR,
    looks_like_sql,
    pick_extractor,
)


# ─── looks_like_sql ──────────────────────────────────────────────────────────


class TestSqlShape:
    @pytest.mark.parametrize("text", [
        "SELECT 1 FROM dual",
        "  select * from t",
        "INSERT INTO foo VALUES (1)",
        "UPDATE x SET y = 1",
        "DELETE FROM x",
        "MERGE INTO t USING s",
        "WITH cte AS (SELECT 1) SELECT * FROM cte",
        "BEGIN dbms_output.put_line('x'); END;",
        "DECLARE v NUMBER; BEGIN NULL; END;",
        "CREATE TABLE foo (id NUMBER)",
        "ALTER TABLE foo ADD COLUMN x NUMBER",
        "DROP INDEX ix_foo",
        "TRUNCATE TABLE staging",
        "CALL my_proc(1, 2)",
    ])
    def test_recognized(self, text):
        assert looks_like_sql(text)

    @pytest.mark.parametrize("text", [
        "Hello, world",
        "selectric typewriter",      # starts with "select" but not as a word
        "",
        " ",
        "SELE",                      # too short
        "https://example.com/select/foo",
    ])
    def test_rejected(self, text):
        assert not looks_like_sql(text)


# ─── Java extractor ──────────────────────────────────────────────────────────


class TestJavaExtractor:
    def test_simple_string(self):
        results = JAVA_EXTRACTOR.fn('String s = "SELECT 1 FROM dual";')
        assert results == [(1, "SELECT 1 FROM dual")]

    def test_multiple_strings(self):
        src = '"first";\n"second";'
        results = JAVA_EXTRACTOR.fn(src)
        # Lines reported correctly.
        assert (1, "first") in results
        assert (2, "second") in results

    def test_text_block(self):
        src = 'String q = """\nSELECT *\nFROM t\n""";'
        results = JAVA_EXTRACTOR.fn(src)
        # Whole text block as one entry, starting at line 1.
        assert any("SELECT *" in text and "FROM t" in text for line, text in results)

    def test_string_concatenation_via_plus(self):
        # Each "..." is its own literal; the extractor returns each.
        src = '"SELECT * FROM t " +\n"WHERE x = 1"'
        results = JAVA_EXTRACTOR.fn(src)
        assert len(results) == 2

    def test_escape_sequence(self):
        src = r'"line1\nline2 \"quoted\""'
        results = JAVA_EXTRACTOR.fn(src)
        assert results and "quoted" in results[0][1]

    def test_line_comment_skipped(self):
        src = '// "fake string in comment"\n"real string";'
        results = JAVA_EXTRACTOR.fn(src)
        assert results == [(2, "real string")]

    def test_block_comment_skipped(self):
        src = '/* "fake"\nstill commented */ "real";'
        results = JAVA_EXTRACTOR.fn(src)
        assert results == [(2, "real")]

    def test_char_literal_not_string(self):
        # Java 'A' is a char, not a string — must not produce a fragment.
        src = "char c = 'A'; String s = \"S\";"
        results = JAVA_EXTRACTOR.fn(src)
        assert results == [(1, "S")]


# ─── Python extractor ───────────────────────────────────────────────────────


class TestPythonExtractor:
    def test_single_quote(self):
        assert PYTHON_EXTRACTOR.fn("x = 'SELECT 1'") == [(1, "SELECT 1")]

    def test_double_quote(self):
        assert PYTHON_EXTRACTOR.fn('x = "SELECT 1"') == [(1, "SELECT 1")]

    def test_triple_double_quote(self):
        results = PYTHON_EXTRACTOR.fn('x = """\nSELECT *\nFROM t\n"""')
        assert results == [(1, "\nSELECT *\nFROM t\n")]

    def test_triple_single_quote(self):
        results = PYTHON_EXTRACTOR.fn("x = '''SELECT 1\nFROM t'''")
        assert results and "SELECT 1" in results[0][1]

    def test_comment_skipped(self):
        results = PYTHON_EXTRACTOR.fn("# 'fake string in comment'\nx = 'real'")
        assert results == [(2, "real")]

    def test_line_count_after_triple_string(self):
        src = '"""one\ntwo\nthree"""\nx = "after"'
        results = PYTHON_EXTRACTOR.fn(src)
        # The 'after' literal must report line 4.
        after = [line for line, t in results if t == "after"]
        assert after == [4]


# ─── pick_extractor ─────────────────────────────────────────────────────────


class TestPickExtractor:
    @pytest.mark.parametrize("path,lang", [
        ("/x/y/A.java", "java"),
        ("/x/y/repo.py", "python"),
        ("/x/y/schema.sql", "sql"),
    ])
    def test_recognized_extensions(self, path, lang):
        from pathlib import Path
        ex = pick_extractor(Path(path))
        assert ex is not None and ex.language == lang

    def test_unknown_extension(self):
        from pathlib import Path
        assert pick_extractor(Path("/x/y/notes.txt")) is None

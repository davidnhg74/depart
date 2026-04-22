"""Tests for the source-code SQL fragment extractors."""

import pytest

from src.analyze.sql_extractor import (
    JAVA_EXTRACTOR,
    PYTHON_EXTRACTOR,
    looks_like_sql,
    pick_extractor,
)


# ─── looks_like_sql ──────────────────────────────────────────────────────────


class TestSqlShape:
    @pytest.mark.parametrize(
        "text",
        [
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
        ],
    )
    def test_recognized(self, text):
        assert looks_like_sql(text)

    @pytest.mark.parametrize(
        "text",
        [
            "Hello, world",
            "selectric typewriter",  # starts with "select" but not as a word
            "",
            " ",
            "SELE",  # too short
            "https://example.com/select/foo",
        ],
    )
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
    @pytest.mark.parametrize(
        "path,lang",
        [
            ("/x/y/A.java", "java"),
            ("/x/y/repo.py", "python"),
            ("/x/y/schema.sql", "sql"),
            ("/x/y/Repo.cs", "csharp"),
            ("/x/y/UserMapper.xml", "mybatis"),
        ],
    )
    def test_recognized_extensions(self, path, lang):
        from pathlib import Path

        ex = pick_extractor(Path(path))
        assert ex is not None and ex.language == lang

    def test_unknown_extension(self):
        from pathlib import Path

        assert pick_extractor(Path("/x/y/notes.txt")) is None


# ─── C# extractor ────────────────────────────────────────────────────────────


class TestCsharpExtractor:
    def test_simple_string(self):
        from src.analyze.sql_extractor import CSHARP_EXTRACTOR

        assert CSHARP_EXTRACTOR.fn('var s = "SELECT 1 FROM dual";') == [(1, "SELECT 1 FROM dual")]

    def test_verbatim_multiline(self):
        from src.analyze.sql_extractor import CSHARP_EXTRACTOR

        src = 'var q = @"SELECT *\nFROM t\nWHERE x=1";'
        results = CSHARP_EXTRACTOR.fn(src)
        assert results and "SELECT *" in results[0][1] and "WHERE x=1" in results[0][1]

    def test_verbatim_doubled_quote_escape(self):
        from src.analyze.sql_extractor import CSHARP_EXTRACTOR

        # @"...""..." escapes a single double-quote.
        src = 'var s = @"it""s ok";'
        results = CSHARP_EXTRACTOR.fn(src)
        assert results == [(1, 'it"s ok')]

    def test_interpolated_string(self):
        from src.analyze.sql_extractor import CSHARP_EXTRACTOR

        src = 'var s = $"SELECT * FROM {tableName}";'
        results = CSHARP_EXTRACTOR.fn(src)
        # The interpolation expression is captured as literal characters; the
        # SQL prefix is preserved so looks_like_sql still fires.
        assert results and results[0][1].startswith("SELECT * FROM ")

    def test_line_comment_skipped(self):
        from src.analyze.sql_extractor import CSHARP_EXTRACTOR

        src = '// "fake string"\n"real";'
        results = CSHARP_EXTRACTOR.fn(src)
        assert results == [(2, "real")]

    def test_char_literal_skipped(self):
        from src.analyze.sql_extractor import CSHARP_EXTRACTOR

        src = "char c = 'A'; var s = \"S\";"
        results = CSHARP_EXTRACTOR.fn(src)
        assert results == [(1, "S")]


# ─── MyBatis XML extractor ──────────────────────────────────────────────────


class TestMyBatisExtractor:
    def test_select_tag(self):
        from src.analyze.sql_extractor import MYBATIS_EXTRACTOR

        src = (
            '<mapper namespace="x">\n'
            '  <select id="findById" resultType="Emp">\n'
            "    SELECT id, name FROM employees WHERE id = #{id}\n"
            "  </select>\n"
            "</mapper>\n"
        )
        results = MYBATIS_EXTRACTOR.fn(src)
        assert len(results) == 1
        line, body = results[0]
        assert "SELECT id, name FROM employees" in body
        assert "#{id}" in body

    def test_multiple_statements(self):
        from src.analyze.sql_extractor import MYBATIS_EXTRACTOR

        src = (
            "<mapper>\n"
            '  <select id="a">SELECT 1 FROM dual</select>\n'
            '  <update id="b">UPDATE t SET x=1</update>\n'
            '  <delete id="c">DELETE FROM t WHERE id=1</delete>\n'
            '  <insert id="d">INSERT INTO t VALUES (1)</insert>\n'
            '  <sql id="cols">id, name</sql>\n'
            "</mapper>\n"
        )
        results = MYBATIS_EXTRACTOR.fn(src)
        assert len(results) == 5
        bodies = [b for _, b in results]
        assert "SELECT 1 FROM dual" in bodies[0]
        assert "UPDATE t" in bodies[1]
        assert "DELETE FROM" in bodies[2]
        assert "INSERT INTO" in bodies[3]

    def test_cdata_stripped(self):
        from src.analyze.sql_extractor import MYBATIS_EXTRACTOR

        src = '<select id="x">\n' "  <![CDATA[ SELECT * FROM t WHERE x < 5 ]]>\n" "</select>\n"
        results = MYBATIS_EXTRACTOR.fn(src)
        assert results
        assert "x < 5" in results[0][1]
        assert "CDATA" not in results[0][1]

    def test_no_statements(self):
        from src.analyze.sql_extractor import MYBATIS_EXTRACTOR

        results = MYBATIS_EXTRACTOR.fn('<mapper><resultMap id="m"/></mapper>')
        assert results == []

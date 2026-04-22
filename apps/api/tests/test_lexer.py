"""Tokenizer behavior tests.

The interim lexer's contract is "tokens with correct string/comment
awareness." Every test here also locks in behavior the regex parser got
wrong, so we'll catch any regression even after the ANTLR swap.
"""

import pytest

from src.source.oracle._lexer import TokenKind, tokenize


# ─── Comments ────────────────────────────────────────────────────────────────


class TestComments:
    def test_line_comment_consumed_to_eol(self):
        toks = tokenize("CREATE -- a line comment\nTABLE foo (id NUMBER);")
        kinds = [(t.kind, t.upper) for t in toks]
        assert (TokenKind.KEYWORD, "CREATE") in kinds
        assert (TokenKind.KEYWORD, "TABLE") in kinds
        # Words from the comment must not appear as tokens.
        assert not any("LINE" in t.upper or "COMMENT" in t.upper for t in toks)

    def test_block_comment_spans_multiple_lines(self):
        src = "CREATE /* one\ntwo\nthree */ TABLE foo (id NUMBER);"
        toks = tokenize(src)
        kinds = [(t.kind, t.upper) for t in toks]
        assert (TokenKind.KEYWORD, "TABLE") in kinds
        # Line counter still advances inside the block comment.
        table_tok = next(t for t in toks if t.upper == "TABLE")
        assert table_tok.line == 3

    def test_block_comment_unterminated_does_not_loop(self):
        # Pathological input — must terminate, not infinite-loop.
        toks = tokenize("CREATE /* never closed")
        # We can't assert much about content; just that the call returns.
        assert any(t.upper == "CREATE" for t in toks)

    def test_double_dash_inside_string_is_not_a_comment(self):
        toks = tokenize("INSERT INTO t VALUES ('a -- b')")
        strings = [t.text for t in toks if t.kind == TokenKind.STRING]
        assert strings == ["'a -- b'"]


# ─── String literals ─────────────────────────────────────────────────────────


class TestStrings:
    def test_simple_string(self):
        toks = tokenize("SELECT 'hello' FROM dual")
        s = [t.text for t in toks if t.kind == TokenKind.STRING]
        assert s == ["'hello'"]

    def test_doubled_quote_escape(self):
        # Oracle escapes a single-quote inside a string by doubling: 'it''s'
        toks = tokenize("SELECT 'it''s ok' FROM dual")
        s = [t.text for t in toks if t.kind == TokenKind.STRING]
        assert s == ["'it''s ok'"]

    def test_multiline_string(self):
        toks = tokenize("VALUES ('line1\nline2')")
        s = [t for t in toks if t.kind == TokenKind.STRING]
        assert len(s) == 1
        # The token after the string must be on line 2.
        idx = toks.index(s[0])
        # Find the next non-whitespace token (which should be `)` on line 2).
        after = toks[idx + 1]
        assert after.line == 2

    def test_keyword_inside_string_is_not_a_keyword(self):
        toks = tokenize("VALUES ('CONNECT BY MERGE INTO PRAGMA AUTONOMOUS_TRANSACTION')")
        kws = [
            t
            for t in toks
            if t.kind == TokenKind.KEYWORD
            and t.upper in ("CONNECT", "MERGE", "PRAGMA", "AUTONOMOUS_TRANSACTION")
        ]
        assert kws == []


class TestQuotedStrings:
    @pytest.mark.parametrize(
        "opener,closer",
        [
            ("[", "]"),
            ("(", ")"),
            ("{", "}"),
            ("<", ">"),
            ("!", "!"),
            ("|", "|"),
        ],
    )
    def test_q_string_paired_delimiters(self, opener, closer):
        src = f"SELECT q'{opener}don't worry{closer}' FROM dual"
        toks = tokenize(src)
        s = [t for t in toks if t.kind == TokenKind.STRING]
        assert len(s) == 1
        assert s[0].text == f"q'{opener}don't worry{closer}'"

    def test_uppercase_Q_string(self):
        toks = tokenize("SELECT Q'[hello]' FROM dual")
        s = [t for t in toks if t.kind == TokenKind.STRING]
        assert s and s[0].text == "Q'[hello]'"


# ─── Identifiers ─────────────────────────────────────────────────────────────


class TestIdentifiers:
    def test_quoted_identifier_preserves_case(self):
        toks = tokenize('SELECT "MixedCase" FROM dual')
        idents = [t for t in toks if t.kind == TokenKind.IDENT]
        # Quoted identifier kept as ident (not keyword); upper preserves the inner text uppercased.
        quoted = next(t for t in idents if t.text == '"MixedCase"')
        assert quoted.upper == "MIXEDCASE"

    def test_percent_attribute(self):
        toks = tokenize("v_id employees.employee_id%TYPE;")
        attrs = [t for t in toks if t.kind == TokenKind.PERCENT_ATTR]
        assert any(t.upper == "%TYPE" for t in attrs)

    def test_at_dblink(self):
        toks = tokenize("SELECT * FROM remote_t@dblink_prod;")
        dbls = [t for t in toks if t.kind == TokenKind.AT_DBLINK]
        assert any(t.upper == "@DBLINK_PROD" for t in dbls)

    def test_keywords_classified(self):
        toks = tokenize("BEGIN INSERT INTO foo VALUES (1); END;")
        for word in ("BEGIN", "INSERT", "INTO", "VALUES", "END"):
            assert any(t.kind == TokenKind.KEYWORD and t.upper == word for t in toks), word


# ─── Operators / punctuation / numbers ───────────────────────────────────────


class TestOperators:
    @pytest.mark.parametrize("text", [":=", "=>", "||", "<=", ">=", "<>", "!=", "**"])
    def test_multichar_operators(self, text):
        # Wrap in something tokenizable to exercise the hot path.
        src = f"a {text} b"
        toks = tokenize(src)
        ops = [t for t in toks if t.kind == TokenKind.OPERATOR]
        assert any(
            t.text == text for t in ops
        ), f"missing operator {text!r} in {[t.text for t in ops]}"

    def test_punctuation(self):
        toks = tokenize("foo(1, 2);")
        puncts = [t.text for t in toks if t.kind == TokenKind.PUNCT]
        assert puncts == ["(", ",", ")", ";"]


class TestNumbers:
    @pytest.mark.parametrize("num", ["1", "42", "3.14", ".5", "1e10", "1.5E-3"])
    def test_numbers(self, num):
        toks = tokenize(f"SELECT {num} FROM dual")
        nums = [t.text for t in toks if t.kind == TokenKind.NUMBER]
        assert num in nums


# ─── Position tracking ──────────────────────────────────────────────────────


class TestPositions:
    def test_line_column_of_first_token(self):
        toks = tokenize("CREATE TABLE foo")
        first = toks[0]
        assert first.upper == "CREATE"
        assert first.line == 1
        assert first.col == 1

    def test_line_advance_after_newlines(self):
        toks = tokenize("\n\nCREATE TABLE foo")
        assert toks[0].line == 3
        assert toks[0].col == 1

    def test_column_of_word_after_spaces(self):
        toks = tokenize("    CREATE")
        assert toks[0].col == 5

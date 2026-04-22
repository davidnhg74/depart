"""Oracle PL/SQL tokenizer — interim implementation.

Replaces the previous regex parser's two worst sins:

  * It counted `'CONNECT BY'` inside a string literal as a CONNECT BY use.
  * It counted `BEGIN` inside a comment as an unbalanced block.

This module's job is to walk the source character-by-character and emit
Tokens with correct treatment of:
  * Single-line comments (`-- ...`)
  * Block comments (`/* ... */`)
  * Single-quoted string literals (`'...'`, including `''` escape)
  * Q-quoted string literals (`q'[...]'`, `q'<...>'`, `q'(...)'`, `q'!...!'`, etc.)
  * Quoted identifiers (`"..."`)
  * Numeric literals
  * Keywords vs. identifiers (case-insensitive)
  * Whitespace and newlines (preserved as line/col offsets in tokens)

This is a tokenizer, not a parser. It is replaced by the ANTLR-generated
lexer once `make grammar` runs in CI/Docker, but its TOKENS contract is
identical — see `_visitor.py` for the parse-tree -> IR layer that swaps in.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Iterator, List, Optional


class TokenKind(str, Enum):
    KEYWORD = "KEYWORD"
    IDENT = "IDENT"
    STRING = "STRING"
    NUMBER = "NUMBER"
    PUNCT = "PUNCT"  # ( ) , ; . :
    OPERATOR = "OPERATOR"  # = + - * / || := => etc.
    PERCENT_ATTR = "PERCENT_ATTR"  # %TYPE / %ROWTYPE / %FOUND etc.
    AT_DBLINK = "AT_DBLINK"  # @dblink_name
    EOF = "EOF"


@dataclass(frozen=True)
class Token:
    kind: TokenKind
    text: str  # original text, case preserved
    upper: str  # uppercased; "" for STRING/NUMBER
    line: int  # 1-indexed
    col: int  # 1-indexed (start column)
    end_line: int
    end_col: int  # exclusive

    def is_kw(self, *names: str) -> bool:
        return self.kind == TokenKind.KEYWORD and self.upper in names


# Keyword set — superset of what we currently key off. Adding here is harmless;
# missing one just classifies as IDENT and we lose a detection. Conservative
# bias toward classifying-as-keyword on common SQL/PL-SQL words.
_KEYWORDS = frozenset(
    {
        "CREATE",
        "OR",
        "REPLACE",
        "PROCEDURE",
        "FUNCTION",
        "PACKAGE",
        "BODY",
        "TRIGGER",
        "VIEW",
        "MATERIALIZED",
        "INDEX",
        "UNIQUE",
        "SEQUENCE",
        "TABLE",
        "GLOBAL",
        "TEMPORARY",
        "TYPE",
        "AS",
        "IS",
        "BEGIN",
        "END",
        "DECLARE",
        "RETURN",
        "RETURNS",
        "LANGUAGE",
        "PLPGSQL",
        "PRAGMA",
        "AUTONOMOUS_TRANSACTION",
        "EXCEPTION",
        "RAISE",
        "WHEN",
        "OTHERS",
        "THEN",
        "ELSE",
        "ELSIF",
        "IF",
        "FOR",
        "WHILE",
        "LOOP",
        "EXIT",
        "CONTINUE",
        "GOTO",
        "NULL",
        "TRUE",
        "FALSE",
        "SELECT",
        "FROM",
        "WHERE",
        "AND",
        "OR",
        "NOT",
        "IN",
        "ON",
        "INTO",
        "USING",
        "INSERT",
        "UPDATE",
        "DELETE",
        "MERGE",
        "VALUES",
        "SET",
        "WITH",
        "CONNECT",
        "BY",
        "PRIOR",
        "START",
        "LEVEL",
        "RECURSIVE",
        "EXECUTE",
        "IMMEDIATE",
        "CURSOR",
        "BULK",
        "COLLECT",
        "FORALL",
        "OPEN",
        "FETCH",
        "CLOSE",
        "REF",
        "OUT",
        "INOUT",
        "PRIMARY",
        "KEY",
        "FOREIGN",
        "REFERENCES",
        "CHECK",
        "CONSTRAINT",
        "DEFAULT",
        "NOT_NULL_HINT",  # NOT NULL handled at parse-time
        "PARTITION",
        "BY",
        "RANGE",
        "LIST",
        "HASH",
        "INTERVAL",
        "TABLESPACE",
        "STORAGE",
        "PCTFREE",
        "INITRANS",
        "LOGGING",
        "NOCOMPRESS",
        "PARALLEL",
        "NOPARALLEL",
        "ENABLE",
        "DISABLE",
        "VALIDATE",
        "NOVALIDATE",
    }
)


class Lexer:
    """Hand-coded interim Oracle lexer. Replaced by the ANTLR-generated
    PlSqlLexer once grammar generation runs (the public Token interface
    above is preserved)."""

    def __init__(self, source: str) -> None:
        self._src = source
        self._i = 0
        self._line = 1
        self._col = 1
        self._n = len(source)

    # ─── public ──────────────────────────────────────────────────────────────

    def tokens(self) -> Iterator[Token]:
        while self._i < self._n:
            ch = self._src[self._i]
            if ch in " \t\r":
                self._advance()
                continue
            if ch == "\n":
                self._newline()
                continue
            # Comments
            if ch == "-" and self._peek(1) == "-":
                self._skip_line_comment()
                continue
            if ch == "/" and self._peek(1) == "*":
                self._skip_block_comment()
                continue
            # Strings: q'...' or '...'
            if (ch in "qQ") and self._peek(1) == "'":
                tok = self._read_q_string()
                if tok:
                    yield tok
                    continue
                # fall through to identifier
            if ch == "'":
                yield self._read_string()
                continue
            # Quoted identifier
            if ch == '"':
                yield self._read_quoted_ident()
                continue
            # Numbers
            if ch.isdigit() or (ch == "." and self._peek(1).isdigit()):
                yield self._read_number()
                continue
            # %TYPE / %ROWTYPE / %FOUND / etc.
            if ch == "%":
                yield self._read_percent_attr()
                continue
            # @ database link
            if ch == "@":
                yield self._read_at_dblink()
                continue
            # Identifiers / keywords
            if ch.isalpha() or ch == "_":
                yield self._read_ident_or_keyword()
                continue
            # Operators
            if ch in "+-*/<>=!|:":
                yield self._read_operator()
                continue
            # Punctuation
            if ch in "(),;.":
                yield self._punct()
                continue
            # Unknown — skip with no token to avoid infinite loops
            self._advance()

        yield Token(TokenKind.EOF, "", "", self._line, self._col, self._line, self._col)

    # ─── helpers ─────────────────────────────────────────────────────────────

    def _peek(self, off: int) -> str:
        j = self._i + off
        return self._src[j] if 0 <= j < self._n else ""

    def _advance(self) -> None:
        self._i += 1
        self._col += 1

    def _newline(self) -> None:
        self._i += 1
        self._line += 1
        self._col = 1

    def _skip_line_comment(self) -> None:
        while self._i < self._n and self._src[self._i] != "\n":
            self._advance()

    def _skip_block_comment(self) -> None:
        # consume opening /*
        self._advance()
        self._advance()
        while self._i < self._n:
            if self._src[self._i] == "*" and self._peek(1) == "/":
                self._advance()
                self._advance()
                return
            if self._src[self._i] == "\n":
                self._newline()
            else:
                self._advance()

    def _read_string(self) -> Token:
        line, col = self._line, self._col
        start = self._i
        self._advance()  # opening '
        while self._i < self._n:
            if self._src[self._i] == "'":
                # '' is escape for single quote inside string
                if self._peek(1) == "'":
                    self._advance()
                    self._advance()
                    continue
                self._advance()
                break
            if self._src[self._i] == "\n":
                self._newline()
            else:
                self._advance()
        text = self._src[start : self._i]
        return Token(TokenKind.STRING, text, "", line, col, self._line, self._col)

    def _read_q_string(self) -> Optional[Token]:
        # q'<delim>...<close-delim>' where close depends on opener:
        # ( ) [ ] { } < > are paired; anything else is its own close.
        if self._peek(2) == "" or self._peek(2) == " ":
            return None
        line, col = self._line, self._col
        start = self._i
        self._advance()  # q
        self._advance()  # '
        opener = self._src[self._i]
        closer = {"(": ")", "[": "]", "{": "}", "<": ">"}.get(opener, opener)
        self._advance()
        while self._i < self._n:
            if self._src[self._i] == closer and self._peek(1) == "'":
                self._advance()
                self._advance()
                break
            if self._src[self._i] == "\n":
                self._newline()
            else:
                self._advance()
        text = self._src[start : self._i]
        return Token(TokenKind.STRING, text, "", line, col, self._line, self._col)

    def _read_quoted_ident(self) -> Token:
        line, col = self._line, self._col
        start = self._i
        self._advance()  # opening "
        while self._i < self._n and self._src[self._i] != '"':
            if self._src[self._i] == "\n":
                self._newline()
            else:
                self._advance()
        if self._i < self._n:
            self._advance()  # closing "
        text = self._src[start : self._i]
        return Token(
            TokenKind.IDENT, text, text.strip('"').upper(), line, col, self._line, self._col
        )

    def _read_number(self) -> Token:
        line, col = self._line, self._col
        start = self._i
        while self._i < self._n and (self._src[self._i].isdigit() or self._src[self._i] in ".eE+-"):
            # Be careful with sign — only consume +/- after e/E
            if (
                self._src[self._i] in "+-"
                and self._i > start
                and self._src[self._i - 1] not in "eE"
            ):
                break
            self._advance()
        text = self._src[start : self._i]
        return Token(TokenKind.NUMBER, text, "", line, col, self._line, self._col)

    def _read_percent_attr(self) -> Token:
        line, col = self._line, self._col
        start = self._i
        self._advance()  # %
        while self._i < self._n and (self._src[self._i].isalnum() or self._src[self._i] == "_"):
            self._advance()
        text = self._src[start : self._i]
        return Token(TokenKind.PERCENT_ATTR, text, text.upper(), line, col, self._line, self._col)

    def _read_at_dblink(self) -> Token:
        line, col = self._line, self._col
        start = self._i
        self._advance()  # @
        while self._i < self._n and (self._src[self._i].isalnum() or self._src[self._i] in "_."):
            self._advance()
        text = self._src[start : self._i]
        return Token(TokenKind.AT_DBLINK, text, text.upper(), line, col, self._line, self._col)

    def _read_ident_or_keyword(self) -> Token:
        line, col = self._line, self._col
        start = self._i
        while self._i < self._n and (self._src[self._i].isalnum() or self._src[self._i] == "_"):
            self._advance()
        text = self._src[start : self._i]
        upper = text.upper()
        kind = TokenKind.KEYWORD if upper in _KEYWORDS else TokenKind.IDENT
        return Token(kind, text, upper, line, col, self._line, self._col)

    def _read_operator(self) -> Token:
        line, col = self._line, self._col
        start = self._i
        # Multi-char operators: := => || <= >= <> != **
        c = self._src[self._i]
        nxt = self._peek(1)
        if (c, nxt) in {
            (":", "="),
            ("=", ">"),
            ("|", "|"),
            ("<", "="),
            (">", "="),
            ("<", ">"),
            ("!", "="),
            ("*", "*"),
        }:
            self._advance()
            self._advance()
        else:
            self._advance()
        text = self._src[start : self._i]
        return Token(TokenKind.OPERATOR, text, text, line, col, self._line, self._col)

    def _punct(self) -> Token:
        line, col = self._line, self._col
        ch = self._src[self._i]
        self._advance()
        return Token(TokenKind.PUNCT, ch, ch, line, col, self._line, self._col)


def tokenize(source: str) -> List[Token]:
    """Convenience: collect all tokens into a list (excluding EOF)."""
    return [t for t in Lexer(source).tokens() if t.kind != TokenKind.EOF]

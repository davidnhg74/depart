"""Pull SQL fragments out of source code.

Strategy: scan strings character-by-character with language-appropriate
quoting rules, then test each string with a SQL-shape heuristic. The
heuristic is intentionally permissive (any string starting with a SQL
keyword) — false positives become low-confidence findings; false
negatives are silent and worse, so we err toward catching too much.

The extractors are not full language parsers. We deliberately accept
that we don't track Java's text blocks or Python's f-string interpolation
edge cases — what we get back is "the source text of the literal, plus
its 1-indexed line." Good enough to find SQL.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, List, Optional


@dataclass(frozen=True)
class SqlFragment:
    """One string literal that looked like SQL."""

    sql: str
    file: str
    line: int  # 1-indexed line of the opening quote


@dataclass(frozen=True)
class Extractor:
    language: str
    extensions: tuple
    fn: Callable[[str], List[tuple]]  # (line, text) tuples


# ─── Heuristic: does this string look like SQL? ──────────────────────────────


_SQL_KEYWORDS = (
    "SELECT",
    "INSERT",
    "UPDATE",
    "DELETE",
    "MERGE",
    "WITH",
    "CREATE",
    "ALTER",
    "DROP",
    "TRUNCATE",
    "BEGIN",
    "DECLARE",
    "CALL",
)
_SQL_RX = re.compile(
    r"^\s*(" + "|".join(_SQL_KEYWORDS) + r")\b",
    re.IGNORECASE,
)


def looks_like_sql(text: str) -> bool:
    if not text or len(text) < 6:
        return False
    return bool(_SQL_RX.search(text))


# ─── Java extractor ──────────────────────────────────────────────────────────


def extract_java(source: str) -> List[tuple]:
    """Extract Java string literals AND merge adjacent concatenations.

    Java SQL is commonly built as `"SELECT ... " + "FROM t " + "WHERE..."`.
    Each concatenated literal on its own may not start with a SQL keyword
    (e.g. the "CONNECT BY ..." piece), so the SQL-shape filter would skip
    it. We merge consecutive literals separated only by `+` and whitespace
    into a single fragment whose line is the line of the first piece.
    """
    raw = _extract_java_raw(source)
    return _merge_java_concatenations(raw, source)


JAVA_EXTRACTOR = Extractor(language="java", extensions=(".java",), fn=extract_java)


def _extract_java_raw(source: str) -> List[tuple]:
    """The original per-literal extractor — used by extract_java + tests."""
    out: List[tuple] = []
    i = 0
    line = 1
    n = len(source)
    while i < n:
        ch = source[i]
        if ch == "/" and i + 1 < n and source[i + 1] == "/":
            while i < n and source[i] != "\n":
                i += 1
            continue
        if ch == "/" and i + 1 < n and source[i + 1] == "*":
            i += 2
            while i + 1 < n and not (source[i] == "*" and source[i + 1] == "/"):
                if source[i] == "\n":
                    line += 1
                i += 1
            i += 2
            continue
        if ch == '"' and source[i : i + 3] == '"""':
            start_line = line
            i += 3
            buf = []
            while i + 2 < n and source[i : i + 3] != '"""':
                buf.append(source[i])
                if source[i] == "\n":
                    line += 1
                i += 1
            i += 3
            # Track end position by stuffing it into a tuple of len 3 — see merge step.
            out.append((start_line, "".join(buf), i))
            continue
        if ch == '"':
            start_line = line
            i += 1
            buf = []
            while i < n and source[i] != '"':
                if source[i] == "\\" and i + 1 < n:
                    buf.append(source[i + 1])
                    i += 2
                    continue
                if source[i] == "\n":
                    line += 1
                buf.append(source[i])
                i += 1
            i += 1
            out.append((start_line, "".join(buf), i))
            continue
        if ch == "'":
            i += 1
            while i < n and source[i] != "'":
                if source[i] == "\\":
                    i += 2
                    continue
                if source[i] == "\n":
                    line += 1
                i += 1
            i += 1
            continue
        if ch == "\n":
            line += 1
        i += 1
    return out


def _merge_java_concatenations(raw_with_end: List[tuple], source: str) -> List[tuple]:
    """Merge consecutive `"..."`'s separated only by `+`, whitespace,
    newlines, or comments. Returns (line, text) tuples like the original
    contract."""
    if not raw_with_end:
        return []
    merged: List[tuple] = []
    cur_line, cur_text, cur_end = raw_with_end[0]
    for next_line, next_text, next_end in raw_with_end[1:]:
        # The interpolated text between cur_end and the start of the next
        # literal — if it's only `+`, whitespace, and comments, it's a
        # continuation.
        gap = source[cur_end : _start_of(next_line, next_text, source, next_end)]
        if _is_string_concat_gap(gap):
            cur_text = cur_text + next_text
            cur_end = next_end
        else:
            merged.append((cur_line, cur_text))
            cur_line, cur_text, cur_end = next_line, next_text, next_end
    merged.append((cur_line, cur_text))
    return merged


def _start_of(line: int, text: str, source: str, end: int) -> int:
    """Find the start position of a literal whose end position is `end` and
    whose body is `text` — work backwards by length+quotes."""
    # The literal is preceded by either '"' or '"""'; we only need an
    # *upper bound* of where the gap ends, which is `end - len(text) - 2`
    # for normal strings and `end - len(text) - 6` for text blocks. Either
    # way the gap region is end-bounded by the *opening quote* — being
    # conservative is fine because _is_string_concat_gap ignores whitespace.
    return max(0, end - len(text) - 6)


_CONCAT_GAP_RX = re.compile(
    r"""^(
        \s+ |               # whitespace incl. newlines
        \+  |               # the concatenation operator
        //[^\n]* |          # line comment
        /\*.*?\*/           # block comment
    )*$""",
    re.VERBOSE | re.DOTALL,
)


def _is_string_concat_gap(gap: str) -> bool:
    return bool(_CONCAT_GAP_RX.match(gap)) and "+" in gap


# ─── Python extractor ────────────────────────────────────────────────────────


def extract_python(source: str) -> List[tuple]:
    """Return (line, text) for each Python string literal.

    Handles: '...', "...", '''...''', \"\"\"...\"\"\". Does NOT interpret
    f-string interpolation — we capture the raw literal text.
    """
    out: List[tuple] = []
    i = 0
    line = 1
    n = len(source)
    while i < n:
        ch = source[i]
        # Line comment
        if ch == "#":
            while i < n and source[i] != "\n":
                i += 1
            continue
        # Triple-quoted strings (must check BEFORE single-quoted)
        if ch in ('"', "'"):
            quote = ch
            triple = source[i : i + 3] == quote * 3
            start_line = line
            if triple:
                i += 3
                buf = []
                while i + 2 < n and source[i : i + 3] != quote * 3:
                    if source[i] == "\\" and i + 1 < n:
                        buf.append(source[i + 1])
                        if source[i + 1] == "\n":
                            line += 1
                        i += 2
                        continue
                    buf.append(source[i])
                    if source[i] == "\n":
                        line += 1
                    i += 1
                i += 3
            else:
                i += 1
                buf = []
                while i < n and source[i] != quote:
                    if source[i] == "\\" and i + 1 < n:
                        buf.append(source[i + 1])
                        i += 2
                        continue
                    if source[i] == "\n":
                        line += 1
                    buf.append(source[i])
                    i += 1
                i += 1
            out.append((start_line, "".join(buf)))
            continue
        if ch == "\n":
            line += 1
        i += 1
    return out


PYTHON_EXTRACTOR = Extractor(language="python", extensions=(".py",), fn=extract_python)


# ─── SQL extractor (whole-file is one fragment) ──────────────────────────────


def extract_sql(source: str) -> List[tuple]:
    return [(1, source)] if source.strip() else []


SQL_EXTRACTOR = Extractor(language="sql", extensions=(".sql",), fn=extract_sql)


# ─── C# extractor ────────────────────────────────────────────────────────────


_CSHARP_VERBATIM_RX = re.compile(r'@"((?:[^"]|"")*)"', re.DOTALL)
_CSHARP_INTERPOLATED_RX = re.compile(r'\$"((?:[^"\\]|\\.)*)"')


def extract_csharp(source: str) -> List[tuple]:
    """Extract C# string literals: regular "...", verbatim @"...", and
    interpolated $"...". We do NOT try to evaluate {expr} inside
    interpolated strings — we capture them as literal text, which is
    enough for the SQL-shape filter to fire on the prefix.
    """
    out: List[tuple] = []
    i = 0
    line = 1
    n = len(source)
    while i < n:
        ch = source[i]
        # Line comment
        if ch == "/" and i + 1 < n and source[i + 1] == "/":
            while i < n and source[i] != "\n":
                i += 1
            continue
        # Block comment
        if ch == "/" and i + 1 < n and source[i + 1] == "*":
            i += 2
            while i + 1 < n and not (source[i] == "*" and source[i + 1] == "/"):
                if source[i] == "\n":
                    line += 1
                i += 1
            i += 2
            continue
        # Verbatim @"..." or interpolated-verbatim @$"..." / $@"..."
        if ch == "@" and i + 1 < n and source[i + 1] == '"':
            start_line = line
            i += 2
            buf = []
            while i < n:
                if source[i] == '"' and i + 1 < n and source[i + 1] == '"':
                    buf.append('"')
                    i += 2
                    continue
                if source[i] == '"':
                    i += 1
                    break
                if source[i] == "\n":
                    line += 1
                buf.append(source[i])
                i += 1
            out.append((start_line, "".join(buf)))
            continue
        # Interpolated string $"..." (single-line; doesn't cross newlines)
        if ch == "$" and i + 1 < n and source[i + 1] == '"':
            start_line = line
            i += 2
            buf = []
            depth = 0  # track {} nesting (interpolation)
            while i < n:
                c = source[i]
                if c == "{" and source[i + 1 : i + 2] != "{":
                    depth += 1
                elif c == "}" and depth > 0:
                    depth -= 1
                elif c == '"' and depth == 0:
                    i += 1
                    break
                elif c == "\\" and i + 1 < n and depth == 0:
                    buf.append(source[i + 1])
                    i += 2
                    continue
                if c == "\n":
                    line += 1
                buf.append(c)
                i += 1
            out.append((start_line, "".join(buf)))
            continue
        # Plain "..."
        if ch == '"':
            start_line = line
            i += 1
            buf = []
            while i < n and source[i] != '"':
                if source[i] == "\\" and i + 1 < n:
                    buf.append(source[i + 1])
                    i += 2
                    continue
                if source[i] == "\n":
                    line += 1
                buf.append(source[i])
                i += 1
            i += 1
            out.append((start_line, "".join(buf)))
            continue
        # Char literal: skip
        if ch == "'":
            i += 1
            while i < n and source[i] != "'":
                if source[i] == "\\":
                    i += 2
                    continue
                i += 1
            i += 1
            continue
        if ch == "\n":
            line += 1
        i += 1
    return out


CSHARP_EXTRACTOR = Extractor(language="csharp", extensions=(".cs",), fn=extract_csharp)


# ─── MyBatis XML extractor ───────────────────────────────────────────────────


_MYBATIS_RX = re.compile(
    r"<\s*(?P<tag>select|insert|update|delete|sql)\b[^>]*>(?P<body>.*?)</\s*(?P=tag)\s*>",
    re.IGNORECASE | re.DOTALL,
)


def extract_mybatis(source: str) -> List[tuple]:
    """Extract SQL bodies from MyBatis mapper XML files.

    We strip CDATA fences and collapse whitespace, then return the SQL
    body of each <select>/<insert>/<update>/<delete>/<sql> element. The
    line is the line of the opening tag.

    The extractor is intentionally regex-based — MyBatis bodies contain
    SQL (often with ${} or #{} placeholders) inside what is otherwise a
    well-formed XML document; full XML parsing is overkill.
    """
    out: List[tuple] = []
    for m in _MYBATIS_RX.finditer(source):
        body = m.group("body")
        # Strip CDATA fences
        body = re.sub(r"<!\[CDATA\[(.*?)\]\]>", r"\1", body, flags=re.DOTALL)
        # Collapse whitespace runs to single spaces
        body = " ".join(body.split())
        line = source.count("\n", 0, m.start("body")) + 1
        out.append((line, body))
    return out


MYBATIS_EXTRACTOR = Extractor(language="mybatis", extensions=(".xml",), fn=extract_mybatis)


# ─── Registry ────────────────────────────────────────────────────────────────


EXTRACTORS = (
    JAVA_EXTRACTOR,
    PYTHON_EXTRACTOR,
    SQL_EXTRACTOR,
    CSHARP_EXTRACTOR,
    MYBATIS_EXTRACTOR,
)


def pick_extractor(path: Path) -> Optional[Extractor]:
    suffix = path.suffix.lower()
    for ex in EXTRACTORS:
        if suffix in ex.extensions:
            return ex
    return None


def extract_from_file(path: Path, extractor: Extractor) -> List[SqlFragment]:
    text = path.read_text(encoding="utf-8", errors="replace")
    fragments: List[SqlFragment] = []
    for line_no, raw in extractor.fn(text):
        if looks_like_sql(raw):
            fragments.append(SqlFragment(sql=raw.strip(), file=str(path), line=line_no))
    return fragments

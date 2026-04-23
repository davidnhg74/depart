"""Oracle parser facade.

Public surface: `parse(source, name=...) -> Module`.

Two implementations live behind this facade:

  1. **Interim** (this file, default): a hand-coded scanner over the
     string/comment-aware token stream from `_lexer.py`. It correctly
     identifies SchemaObjects (TABLE, VIEW, SEQUENCE, INDEX, TRIGGER,
     PROCEDURE, FUNCTION, PACKAGE, PACKAGE BODY) and tags occurrences of
     known constructs (CONNECT BY, MERGE, autonomous tx, BULK COLLECT,
     dblinks, etc.). It does NOT attempt to fully parse PL/SQL bodies.

  2. **ANTLR-backed** (next pass): drops in the parse tree from the
     vendored `grammar/PlSqlLexer.g4` + `grammar/PlSqlParser.g4` and a
     `_visitor.py` that walks the tree to produce the same Module shape.
     The interim implementation is then deleted entirely.

Switching between them is a one-line change in `_pick_impl()`. Consumers
(complexity, semantic, AI services) never see which one ran.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterator, List, Optional, Tuple

from ...core.diagnostics.diagnostic import Diagnostic, Severity, Span
from ...core.ir.nodes import (
    ConstructRef,
    ConstructTag,
    Index,
    Module,
    ObjectKind,
    Package,
    SchemaObject,
    Sequence,
    Subprogram,
    Table,
    Trigger,
    View,
)
from ._lexer import Token, TokenKind, tokenize


DIALECT = "oracle"


def parse(source: str, *, name: str = "<inline>") -> Module:
    """Parse Oracle DDL + PL/SQL into a canonical IR Module.

    Dispatch order:
      1. ANTLR-backed parser (`_visitor.parse_with_antlr`) when the
         generated package exists. This is the production path; CI and
         the Docker build always satisfy it.
      2. Interim string/comment-aware parser. Used both as a local-dev
         fallback (before `make grammar`) AND as a safety net when ANTLR
         throws on inputs the grammar's Python runtime can't handle —
         some vendored PL/SQL rules trip an assertion inside
         `CommonTokenStream.adjustSeekIndex` on short inputs. Rather
         than die on the user, we downgrade to the interim parser and
         tag a module-level diagnostic so downstream consumers know.

    The two paths produce the same `Module` shape — see
    `tests/test_parser_equivalence.py` for the gating test.
    """
    from . import _visitor

    if _visitor.is_available():
        try:
            module = _visitor.parse_with_antlr(source, name=name)
            if _looks_like_silent_miss(module, source):
                # ANTLR ran but didn't recognize the top-level rule (e.g.
                # vendored grammar gaps). Re-parse with the interim path
                # so the IR isn't empty for downstream callers.
                fallback = _InterimParser(source, name).parse()
                _attach_fallback_diag(
                    fallback, RuntimeError("ANTLR returned no objects for non-empty source")
                )
                return fallback
            # ANTLR is excellent at object structure (where TABLE/PROCEDURE/...
            # start and end) but only knows the constructs we've taught its
            # visitor about. The interim parser's token scanner sweeps the
            # long tail (DBMS_*, dblinks, %TYPE pseudo-attributes, RAISE_-
            # APPLICATION_ERROR, etc.) — much cheaper to keep that as a
            # second pass than to add a visitor method for every Oracle
            # keyword. Merge the construct refs into ANTLR's module.
            _augment_with_interim_constructs(module, source)
            return module
        except Exception as exc:  # noqa: BLE001 — grammar bugs vary widely
            module = _InterimParser(source, name).parse()
            _attach_fallback_diag(module, exc)
            return module
    return _InterimParser(source, name).parse()


def _augment_with_interim_constructs(module: Module, source: str) -> None:
    """Run the interim parser purely for its construct-detection pass and
    union those tags onto the ANTLR-produced module. We dedupe by
    (tag, span.start_line, span.start_col) so re-runs are idempotent."""
    interim = _InterimParser(source, "<augment>").parse()
    interim_refs: List[ConstructRef] = []
    for obj in interim.objects:
        interim_refs.extend(getattr(obj, "referenced_constructs", []))
    if not interim_refs:
        return
    sentinel = next(
        (o for o in module.objects if o.name == "<module-constructs>"), None
    )
    if sentinel is None:
        sentinel = Subprogram(
            kind=ObjectKind.UNKNOWN,
            name="<module-constructs>",
            span=module.span,
            line_count=0,
        )
        module.objects.append(sentinel)
    seen = {
        (r.tag, r.span.start_line, r.span.start_col)
        for r in sentinel.referenced_constructs
    }
    for r in interim_refs:
        key = (r.tag, r.span.start_line, r.span.start_col)
        if key in seen:
            continue
        sentinel.referenced_constructs.append(r)
        seen.add(key)


def _looks_like_silent_miss(module: Module, source: str) -> bool:
    """True when ANTLR produced no schema objects but the source clearly
    declares one. We only check for `CREATE` because everything else
    (anonymous PL/SQL blocks, statement-level scripts) legitimately yields
    empty Module shapes."""
    has_real_object = any(o.name != "<module-constructs>" for o in module.objects)
    if has_real_object:
        return False
    # Cheap upper-case scan; a substring match is good enough — false
    # positives just mean we re-run the interim parser unnecessarily.
    return "CREATE " in source.upper()


def _attach_fallback_diag(module: Module, exc: Exception) -> None:
    diag = Diagnostic(
        code="ORA.PARSE.ANTLR_FALLBACK",
        severity=Severity.WARNING,
        message=f"ANTLR parse failed ({type(exc).__name__}: {exc}); using interim parser",
        span=module.span,
    )
    # Re-use the sentinel if the interim parser emitted one; otherwise create one.
    if module.objects and module.objects[-1].name == "<module-constructs>":
        module.objects[-1].diagnostics.append(diag)
        return
    sentinel = Subprogram(
        kind=ObjectKind.UNKNOWN,
        name="<module-constructs>",
        span=module.span,
        line_count=0,
    )
    sentinel.diagnostics.append(diag)
    module.objects.append(sentinel)


def parse_with_interim(source: str, *, name: str = "<inline>") -> Module:
    """Force the interim implementation. Useful for the equivalence test
    and for callers that explicitly want stable behavior across grammar
    regenerations."""
    return _InterimParser(source, name).parse()


# ─── Interim implementation ───────────────────────────────────────────────────


@dataclass
class _InterimParser:
    source: str
    name: str

    def parse(self) -> Module:
        tokens = tokenize(self.source)
        module = Module(name=self.name, span=_span_for_source(self.source))
        if not tokens:
            return module

        module.objects = list(self._extract_objects(tokens))
        # Construct occurrences are collected globally (not per-object).
        # Properly scoping a CONNECT BY to its enclosing PROCEDURE requires a
        # real PL/SQL parser; the ANTLR pass restores that. Until then, the
        # complexity scorer uses global counts + a LOC-per-construct
        # heuristic — see analyze/complexity.py.
        all_refs = list(self._find_constructs(tokens))
        # We attach the flat list of refs to the module via a sentinel Subprogram
        # named "<module>" — this keeps the IR shape uniform without requiring
        # a separate field on Module.
        if all_refs:
            sentinel = Subprogram(
                kind=ObjectKind.UNKNOWN,
                name="<module-constructs>",
                span=module.span,
                line_count=0,
                referenced_constructs=all_refs,
            )
            module.objects.append(sentinel)
        return module

    # ─── object extraction ───────────────────────────────────────────────────

    def _extract_objects(self, tokens: List[Token]) -> Iterator[SchemaObject]:
        i = 0
        n = len(tokens)
        while i < n:
            t = tokens[i]
            # CREATE [OR REPLACE] <kind> ...
            if t.is_kw("CREATE"):
                start_i = i
                i += 1
                # OR REPLACE
                if i < n and tokens[i].is_kw("OR") and i + 1 < n and tokens[i + 1].is_kw("REPLACE"):
                    i += 2
                # Optional GLOBAL TEMPORARY
                is_global_temp = False
                if (
                    i < n
                    and tokens[i].is_kw("GLOBAL")
                    and i + 1 < n
                    and tokens[i + 1].is_kw("TEMPORARY")
                ):
                    is_global_temp = True
                    i += 2
                # Optional MATERIALIZED
                is_materialized = False
                if i < n and tokens[i].is_kw("MATERIALIZED"):
                    is_materialized = True
                    i += 1
                # Optional UNIQUE for INDEX
                is_unique = False
                if i < n and tokens[i].is_kw("UNIQUE"):
                    is_unique = True
                    i += 1
                if i >= n:
                    break
                kind_tok = tokens[i]
                kind = _ddl_kind_from_keyword(kind_tok.upper)
                if kind is None:
                    i += 1
                    continue
                i += 1
                # PACKAGE BODY?
                is_body = False
                if kind == ObjectKind.PACKAGE and i < n and tokens[i].is_kw("BODY"):
                    is_body = True
                    i += 1
                # TYPE BODY?
                if kind == ObjectKind.TYPE and i < n and tokens[i].is_kw("BODY"):
                    kind = ObjectKind.TYPE_BODY
                    i += 1
                # Object name (optionally schema-qualified)
                name, i = _read_qualified_name(tokens, i)
                if not name:
                    continue
                end_i = _find_object_end(tokens, i)
                span = _span_between(tokens[start_i], tokens[min(end_i, n - 1)])
                raw = self._slice(tokens[start_i], tokens[min(end_i, n - 1)])
                line_count = max(1, span.end_line - span.start_line + 1)

                yield _build_object(
                    kind=kind,
                    name=name,
                    is_global_temp=is_global_temp,
                    is_materialized=is_materialized,
                    is_unique=is_unique,
                    is_body=is_body,
                    span=span,
                    raw=raw,
                    line_count=line_count,
                )
                i = end_i
                continue
            i += 1

    # ─── construct detection ─────────────────────────────────────────────────

    def _find_constructs(self, tokens: List[Token]) -> Iterator[ConstructRef]:
        n = len(tokens)
        i = 0
        while i < n:
            t = tokens[i]

            # CONNECT BY
            if t.is_kw("CONNECT") and i + 1 < n and tokens[i + 1].is_kw("BY"):
                yield ConstructRef(
                    ConstructTag.CONNECT_BY, _span_between(t, tokens[i + 1]), "CONNECT BY"
                )
                i += 2
                continue

            # MERGE INTO
            if t.is_kw("MERGE") and i + 1 < n and tokens[i + 1].is_kw("INTO"):
                yield ConstructRef(
                    ConstructTag.MERGE, _span_between(t, tokens[i + 1]), "MERGE INTO"
                )
                i += 2
                continue

            # PRAGMA AUTONOMOUS_TRANSACTION
            if t.is_kw("PRAGMA") and i + 1 < n and tokens[i + 1].is_kw("AUTONOMOUS_TRANSACTION"):
                yield ConstructRef(
                    ConstructTag.AUTONOMOUS_TXN,
                    _span_between(t, tokens[i + 1]),
                    "PRAGMA AUTONOMOUS_TRANSACTION",
                )
                i += 2
                continue

            # EXECUTE IMMEDIATE
            if t.is_kw("EXECUTE") and i + 1 < n and tokens[i + 1].is_kw("IMMEDIATE"):
                yield ConstructRef(
                    ConstructTag.EXECUTE_IMMEDIATE,
                    _span_between(t, tokens[i + 1]),
                    "EXECUTE IMMEDIATE",
                )
                i += 2
                continue

            # BULK COLLECT
            if t.is_kw("BULK") and i + 1 < n and tokens[i + 1].is_kw("COLLECT"):
                yield ConstructRef(
                    ConstructTag.BULK_COLLECT, _span_between(t, tokens[i + 1]), "BULK COLLECT"
                )
                i += 2
                continue

            # FORALL
            if t.is_kw("FORALL"):
                yield ConstructRef(
                    ConstructTag.FORALL, t.line and _span_of(t) or Span.unknown(), "FORALL"
                )
                i += 1
                continue

            # PRAGMA EXCEPTION_INIT
            if (
                t.is_kw("PRAGMA")
                and i + 1 < n
                and tokens[i + 1].kind == TokenKind.IDENT
                and tokens[i + 1].upper == "EXCEPTION_INIT"
            ):
                yield ConstructRef(
                    ConstructTag.PRAGMA_EXCEPTION_INIT,
                    _span_between(t, tokens[i + 1]),
                    "PRAGMA EXCEPTION_INIT",
                )
                i += 2
                continue

            # %TYPE / %ROWTYPE
            if t.kind == TokenKind.PERCENT_ATTR and t.upper in ("%TYPE", "%ROWTYPE"):
                yield ConstructRef(ConstructTag.PERCENT_TYPE, _span_of(t), t.upper)
                i += 1
                continue

            # Oracle OUTER JOIN (+) operator — three consecutive tokens `(`, `+`, `)`.
            # The lexer leaves `+` as a PUNCT/operator token, so we match by text.
            if (
                t.kind == TokenKind.PUNCT
                and t.text == "("
                and i + 2 < n
                and tokens[i + 1].text == "+"
                and tokens[i + 2].kind == TokenKind.PUNCT
                and tokens[i + 2].text == ")"
            ):
                yield ConstructRef(
                    ConstructTag.OUTER_JOIN_PLUS,
                    _span_between(t, tokens[i + 2]),
                    "(+)",
                )
                i += 3
                continue

            # REF CURSOR — either `SYS_REFCURSOR` (single ident) or
            # `IS REF CURSOR` / `TYPE x IS REF CURSOR` (three consecutive keywords).
            if t.kind == TokenKind.IDENT and t.upper == "SYS_REFCURSOR":
                yield ConstructRef(ConstructTag.REF_CURSOR, _span_of(t), "SYS_REFCURSOR")
                i += 1
                continue
            if (
                t.is_kw("REF")
                and i + 1 < n
                and tokens[i + 1].is_kw("CURSOR")
            ):
                yield ConstructRef(
                    ConstructTag.REF_CURSOR,
                    _span_between(t, tokens[i + 1]),
                    "REF CURSOR",
                )
                i += 2
                continue

            # ROWNUM / ROWID / LEVEL pseudocolumns
            if t.kind == TokenKind.IDENT and t.upper == "ROWNUM":
                yield ConstructRef(ConstructTag.ROWNUM, _span_of(t), "ROWNUM")
                i += 1
                continue
            if t.kind == TokenKind.IDENT and t.upper == "ROWID":
                yield ConstructRef(ConstructTag.ROWID, _span_of(t), "ROWID")
                i += 1
                continue
            if t.is_kw("LEVEL"):
                yield ConstructRef(ConstructTag.HIERARCHICAL_PSEUDOCOLUMN, _span_of(t), "LEVEL")
                i += 1
                continue

            # DBMS_xxx and UTL_xxx packages — qualified call dbms_x.method(...)
            if t.kind == TokenKind.IDENT and (
                t.upper.startswith("DBMS_") or t.upper.startswith("UTL_")
            ):
                tag = _tag_for_oracle_pkg(t.upper)
                if tag is not None:
                    yield ConstructRef(tag, _span_of(t), t.text)
                i += 1
                continue

            # Database link reference: ident@dblink
            if t.kind == TokenKind.AT_DBLINK:
                yield ConstructRef(ConstructTag.DBLINK, _span_of(t), t.text)
                i += 1
                continue

            # Spatial / Oracle Text — surface on identifiers
            if t.kind == TokenKind.IDENT and t.upper in {"SDO_GEOMETRY", "MDSYS"}:
                yield ConstructRef(ConstructTag.SPATIAL, _span_of(t), t.text)
                i += 1
                continue
            if t.kind == TokenKind.IDENT and t.upper in {"CTXSYS", "CTXCAT", "CTXRULE"}:
                yield ConstructRef(ConstructTag.ORACLE_TEXT, _span_of(t), t.text)
                i += 1
                continue
            if t.kind == TokenKind.IDENT and t.upper == "CONTAINS":
                # CONTAINS(...) when used as a predicate is Oracle Text; in
                # a CHECK it's something else. Cheap to flag, easy to filter.
                yield ConstructRef(ConstructTag.ORACLE_TEXT, _span_of(t), "CONTAINS")
                i += 1
                continue

            # RAISE_APPLICATION_ERROR
            if t.kind == TokenKind.IDENT and t.upper == "RAISE_APPLICATION_ERROR":
                yield ConstructRef(
                    ConstructTag.RAISE_APPLICATION_ERROR, _span_of(t), "RAISE_APPLICATION_ERROR"
                )
                i += 1
                continue

            # NVL / NVL2 — match the function-call shape so we don't
            # flag a column literally named NVL. Both followed by `(`.
            if (
                t.kind == TokenKind.IDENT
                and t.upper in ("NVL", "NVL2")
                and i + 1 < n
                and tokens[i + 1].kind == TokenKind.PUNCT
                and tokens[i + 1].text == "("
            ):
                yield ConstructRef(ConstructTag.NVL, _span_of(t), t.upper)
                i += 1
                continue

            # DECODE — same function-call shape. Must come BEFORE any
            # generic IDENT handling so we can still detect a DECODE
            # used as a column alias elsewhere.
            if (
                t.kind == TokenKind.IDENT
                and t.upper == "DECODE"
                and i + 1 < n
                and tokens[i + 1].kind == TokenKind.PUNCT
                and tokens[i + 1].text == "("
            ):
                yield ConstructRef(ConstructTag.DECODE, _span_of(t), "DECODE")
                i += 1
                continue

            # INTERVAL — flag both the column-type qualifier and the
            # literal forms (`INTERVAL '2-6' YEAR TO MONTH`). The
            # converter ships one canonical example for both shapes.
            if t.is_kw("INTERVAL"):
                yield ConstructRef(ConstructTag.INTERVAL, _span_of(t), "INTERVAL")
                i += 1
                continue

            i += 1

    # ─── helpers ─────────────────────────────────────────────────────────────

    def _slice(self, t_start: Token, t_end: Token) -> str:
        return _slice_source(self.source, t_start.line, t_start.col, t_end.end_line, t_end.end_col)

    def _object_for_span(self, objects: List[SchemaObject], span: Span) -> Optional[SchemaObject]:
        for obj in objects:
            if obj.span.start_line <= span.start_line <= obj.span.end_line:
                return obj
        return None


# ─── module-level helpers ─────────────────────────────────────────────────────


def _ddl_kind_from_keyword(kw: str) -> Optional[ObjectKind]:
    return {
        "TABLE": ObjectKind.TABLE,
        "VIEW": ObjectKind.VIEW,
        "INDEX": ObjectKind.INDEX,
        "SEQUENCE": ObjectKind.SEQUENCE,
        "TRIGGER": ObjectKind.TRIGGER,
        "PROCEDURE": ObjectKind.PROCEDURE,
        "FUNCTION": ObjectKind.FUNCTION,
        "PACKAGE": ObjectKind.PACKAGE,
        "TYPE": ObjectKind.TYPE,
        "SYNONYM": ObjectKind.SYNONYM,
    }.get(kw)


def _read_qualified_name(tokens: List[Token], i: int) -> Tuple[str, int]:
    """Read either `name` or `schema.name`. Returns (name, new_i).
    The schema prefix is dropped (for now); callers can recover it from the
    raw source if needed."""
    n = len(tokens)
    if i >= n or tokens[i].kind not in (TokenKind.IDENT, TokenKind.KEYWORD):
        return "", i
    first = tokens[i].text.strip('"')
    i += 1
    if (
        i + 1 < n
        and tokens[i].kind == TokenKind.PUNCT
        and tokens[i].text == "."
        and tokens[i + 1].kind in (TokenKind.IDENT, TokenKind.KEYWORD)
    ):
        name = tokens[i + 1].text.strip('"')
        return name, i + 2
    return first, i


def _find_object_end(tokens: List[Token], i: int) -> int:
    """Find the end of a top-level CREATE statement.

    For DDL (TABLE/VIEW/INDEX/SEQUENCE/SYNONYM): the next ';' at paren-depth 0.
    For PL/SQL blocks (PROCEDURE/FUNCTION/PACKAGE/TYPE BODY/TRIGGER): the
    matching `END [name];` at block-depth 0.

    The interim implementation is intentionally conservative — when in doubt,
    it stops at the next ';' at paren-depth 0. ANTLR replaces this entirely.
    """
    n = len(tokens)
    paren_depth = 0
    j = i
    while j < n:
        t = tokens[j]
        if t.kind == TokenKind.PUNCT and t.text == "(":
            paren_depth += 1
        elif t.kind == TokenKind.PUNCT and t.text == ")":
            paren_depth = max(0, paren_depth - 1)
        elif t.kind == TokenKind.PUNCT and t.text == ";" and paren_depth == 0:
            return j + 1
        j += 1
    return n


def _build_object(
    *,
    kind: ObjectKind,
    name: str,
    is_global_temp: bool,
    is_materialized: bool,
    is_unique: bool,
    is_body: bool,
    span: Span,
    raw: str,
    line_count: int,
) -> SchemaObject:
    common = dict(name=name, span=span, raw_source=raw, line_count=line_count)
    if kind == ObjectKind.TABLE:
        return Table(kind=kind, is_global_temp=is_global_temp, **common)
    if kind in (ObjectKind.VIEW, ObjectKind.MATERIALIZED_VIEW):
        return View(
            kind=ObjectKind.MATERIALIZED_VIEW if is_materialized else ObjectKind.VIEW,
            is_materialized=is_materialized,
            **common,
        )
    if kind == ObjectKind.SEQUENCE:
        return Sequence(kind=kind, **common)
    if kind == ObjectKind.INDEX:
        return Index(kind=kind, unique=is_unique, **common)
    if kind == ObjectKind.TRIGGER:
        return Trigger(kind=kind, **common)
    if kind in (ObjectKind.PROCEDURE, ObjectKind.FUNCTION):
        return Subprogram(kind=kind, **common)
    if kind in (ObjectKind.PACKAGE, ObjectKind.PACKAGE_BODY):
        return Package(
            kind=ObjectKind.PACKAGE_BODY if is_body else ObjectKind.PACKAGE,
            is_body=is_body,
            **common,
        )
    return SchemaObject(kind=kind, **common)


def _module_level_construct(ref: ConstructRef) -> SchemaObject:
    obj = SchemaObject(kind=ObjectKind.UNKNOWN, name=f"<{ref.tag.value}>", span=ref.span)
    obj.diagnostics.append(_diag_for_construct(ref))
    return obj


def _diag_for_construct(ref: ConstructRef) -> Diagnostic:
    return Diagnostic(
        code=f"ORA.CONSTRUCT.{ref.tag.value}",
        severity=Severity.INFO,
        message=f"{ref.tag.value} occurrence",
        span=ref.span,
        details={"snippet": ref.snippet},
    )


def _tag_for_oracle_pkg(upper_ident: str) -> Optional[ConstructTag]:
    return {
        "DBMS_OUTPUT": ConstructTag.DBMS_OUTPUT,
        "DBMS_SCHEDULER": ConstructTag.DBMS_SCHEDULER,
        "DBMS_AQ": ConstructTag.DBMS_AQ,
        "DBMS_AQADM": ConstructTag.DBMS_AQ,
        "DBMS_CRYPTO": ConstructTag.DBMS_CRYPTO,
        "UTL_FILE": ConstructTag.UTL_FILE,
        "UTL_HTTP": ConstructTag.UTL_HTTP,
        "UTL_MAIL": ConstructTag.UTL_HTTP,  # similar refactor path
        "UTL_SMTP": ConstructTag.UTL_HTTP,
    }.get(upper_ident)


def _span_of(t: Token) -> Span:
    return Span(
        file=None, start_line=t.line, start_col=t.col, end_line=t.end_line, end_col=t.end_col
    )


def _span_between(a: Token, b: Token) -> Span:
    return Span(
        file=None, start_line=a.line, start_col=a.col, end_line=b.end_line, end_col=b.end_col
    )


def _span_for_source(source: str) -> Span:
    lines = source.split("\n")
    return Span(
        file=None,
        start_line=1,
        start_col=1,
        end_line=len(lines),
        end_col=len(lines[-1]) + 1 if lines else 1,
    )


def _slice_source(source: str, start_line: int, start_col: int, end_line: int, end_col: int) -> str:
    """Best-effort slice using line/col positions. Fast path: linear scan."""
    cur_line = 1
    cur_col = 1
    start_idx = end_idx = 0
    for idx, ch in enumerate(source):
        if cur_line == start_line and cur_col == start_col:
            start_idx = idx
        if cur_line == end_line and cur_col == end_col:
            end_idx = idx
            break
        if ch == "\n":
            cur_line += 1
            cur_col = 1
        else:
            cur_col += 1
    else:
        end_idx = len(source)
    return source[start_idx:end_idx]

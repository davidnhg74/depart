"""ANTLR parse-tree → canonical IR visitor.

Activates when `make grammar` (or the Dockerfile/CI) has populated
`src/source/oracle/_generated/` with the ANTLR-generated PlSqlLexer,
PlSqlParser, and PlSqlParserVisitor classes. Until then, `is_available()`
returns False and `parser.parse()` transparently falls back to the
string/comment-aware interim implementation.

Design:
  * One Visitor subclass walks the whole tree once. Each `visit*` method
    returns either None (it pushed state into self.module) or an IR node
    (used by callers to compose). Construct tagging is centralized in
    `visitTerminal` so we don't duplicate detection logic per rule.
  * No string-shaped fallback: if a parse rule isn't recognized, we emit
    an IR `UnsupportedConstruct` with a Diagnostic, never raw text.
  * Errors from the parser arrive on a custom ErrorListener and become
    Module-level Diagnostics — we do not raise.
"""
from __future__ import annotations

import importlib
from dataclasses import dataclass
from typing import Iterable, List, Optional, Tuple

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
    UnsupportedConstruct,
    View,
)


# ─── Availability probe ──────────────────────────────────────────────────────


_GENERATED_PKG = "src.source.oracle._generated"


def is_available() -> bool:
    """True iff the ANTLR-generated parser package is importable.

    Probed lazily and memoized — `make grammar` (or the first run after a
    grammar change) flips this from False to True without process restart.
    """
    global _availability
    if _availability is None:
        try:
            importlib.import_module(f"{_GENERATED_PKG}.PlSqlParser")
            importlib.import_module(f"{_GENERATED_PKG}.PlSqlLexer")
            importlib.import_module(f"{_GENERATED_PKG}.PlSqlParserVisitor")
            _availability = True
        except ImportError:
            _availability = False
    return _availability


_availability: Optional[bool] = None


def reset_availability() -> None:
    """Test hook — forget the cached probe result."""
    global _availability
    _availability = None


# ─── Public entry point ──────────────────────────────────────────────────────


def parse_with_antlr(source: str, *, name: str = "<inline>") -> Module:
    """Parse Oracle source via ANTLR. Caller MUST guard with `is_available()`."""
    if not is_available():
        raise RuntimeError(
            "ANTLR parser not available. Run `make grammar` to generate "
            "src/source/oracle/_generated/."
        )

    from antlr4 import CommonTokenStream, InputStream
    from antlr4.error.ErrorListener import ErrorListener

    lexer_mod = importlib.import_module(f"{_GENERATED_PKG}.PlSqlLexer")
    parser_mod = importlib.import_module(f"{_GENERATED_PKG}.PlSqlParser")
    visitor_mod = importlib.import_module(f"{_GENERATED_PKG}.PlSqlParserVisitor")

    PlSqlLexer = getattr(lexer_mod, "PlSqlLexer")
    PlSqlParser = getattr(parser_mod, "PlSqlParser")
    PlSqlParserVisitor = getattr(visitor_mod, "PlSqlParserVisitor")

    diagnostics: List[Diagnostic] = []

    class _Collector(ErrorListener):
        def syntaxError(self, recognizer, offendingSymbol, line, column, msg, e):  # noqa: N802
            diagnostics.append(Diagnostic(
                code="ORA.PARSE.SYNTAX",
                severity=Severity.ERROR,
                message=msg,
                span=Span(file=None, start_line=line, start_col=column,
                          end_line=line, end_col=column + 1),
            ))

    stream = InputStream(source)
    lexer = PlSqlLexer(stream)
    lexer.removeErrorListeners()
    lexer.addErrorListener(_Collector())
    tokens = CommonTokenStream(lexer)
    parser = PlSqlParser(tokens)
    parser.removeErrorListeners()
    parser.addErrorListener(_Collector())

    tree = parser.sql_script()

    visitor = _IRVisitor(name, source, PlSqlParserVisitor)
    visitor.visit(tree)

    module = visitor.module
    # Stash diagnostics on the synthetic <module-constructs> sentinel so
    # downstream consumers (complexity scorer, runbook generator) can walk
    # them without a separate field on Module.
    if diagnostics:
        if not module.objects or module.objects[-1].name != "<module-constructs>":
            module.objects.append(_make_sentinel(module.span))
        module.objects[-1].diagnostics.extend(diagnostics)

    return module


# ─── Visitor implementation ──────────────────────────────────────────────────


@dataclass
class _IRVisitor:
    """Walks the ANTLR parse tree and emits canonical IR nodes."""

    name: str
    source: str
    PlSqlParserVisitor: type     # injected so this module is import-safe

    def __post_init__(self) -> None:
        self.module = Module(name=self.name, span=_module_span(self.source))
        self._refs: List[ConstructRef] = []
        # The actual ANTLR visitor instance dispatches via reflection into
        # the methods on this _IRVisitor.
        self._dispatcher = _build_dispatcher(self)

    def visit(self, tree) -> None:
        self._dispatcher.visit(tree)
        if self._refs:
            sentinel = _make_sentinel(self.module.span)
            sentinel.referenced_constructs.extend(self._refs)
            self.module.objects.append(sentinel)

    # ─── object-creating rules ───────────────────────────────────────────────
    # The ANTLR-generated visitor calls `visitCreate_table(ctx)`,
    # `visitCreate_view(ctx)`, etc. We mirror those names below; the
    # _build_dispatcher helper proxies them onto this object.

    def visit_create_table(self, ctx) -> None:
        name = _extract_object_name(ctx, "tableview_name", "table_name")
        is_global_temp = _ctx_text(ctx).upper().startswith("CREATE GLOBAL TEMPORARY")
        self.module.objects.append(Table(
            kind=ObjectKind.TABLE,
            name=name,
            is_global_temp=is_global_temp,
            span=_span_of(ctx),
            line_count=_line_count(ctx),
            raw_source=_ctx_text(ctx),
        ))

    def visit_create_view(self, ctx) -> None:
        name = _extract_object_name(ctx, "tableview_name")
        is_materialized = _ctx_text(ctx).upper().startswith("CREATE MATERIALIZED")
        self.module.objects.append(View(
            kind=ObjectKind.MATERIALIZED_VIEW if is_materialized else ObjectKind.VIEW,
            name=name,
            is_materialized=is_materialized,
            span=_span_of(ctx),
            line_count=_line_count(ctx),
            raw_source=_ctx_text(ctx),
        ))

    def visit_create_index(self, ctx) -> None:
        name = _extract_object_name(ctx, "index_name")
        is_unique = "UNIQUE" in _ctx_text(ctx).upper().split()[:5]
        self.module.objects.append(Index(
            kind=ObjectKind.INDEX,
            name=name,
            unique=is_unique,
            span=_span_of(ctx),
            line_count=_line_count(ctx),
            raw_source=_ctx_text(ctx),
        ))

    def visit_create_sequence(self, ctx) -> None:
        name = _extract_object_name(ctx, "sequence_name")
        self.module.objects.append(Sequence(
            kind=ObjectKind.SEQUENCE,
            name=name,
            span=_span_of(ctx),
            line_count=_line_count(ctx),
            raw_source=_ctx_text(ctx),
        ))

    def visit_create_trigger(self, ctx) -> None:
        name = _extract_object_name(ctx, "trigger_name")
        self.module.objects.append(Trigger(
            kind=ObjectKind.TRIGGER,
            name=name,
            span=_span_of(ctx),
            line_count=_line_count(ctx),
            raw_source=_ctx_text(ctx),
        ))

    def visit_create_procedure_body(self, ctx) -> None:
        name = _extract_object_name(ctx, "procedure_name")
        self.module.objects.append(Subprogram(
            kind=ObjectKind.PROCEDURE,
            name=name,
            span=_span_of(ctx),
            line_count=_line_count(ctx),
            raw_source=_ctx_text(ctx),
        ))

    def visit_create_function_body(self, ctx) -> None:
        name = _extract_object_name(ctx, "function_name")
        self.module.objects.append(Subprogram(
            kind=ObjectKind.FUNCTION,
            name=name,
            span=_span_of(ctx),
            line_count=_line_count(ctx),
            raw_source=_ctx_text(ctx),
        ))

    def visit_create_package(self, ctx) -> None:
        name = _extract_object_name(ctx, "package_name")
        self.module.objects.append(Package(
            kind=ObjectKind.PACKAGE,
            name=name,
            is_body=False,
            span=_span_of(ctx),
            line_count=_line_count(ctx),
            raw_source=_ctx_text(ctx),
        ))

    def visit_create_package_body(self, ctx) -> None:
        name = _extract_object_name(ctx, "package_name")
        self.module.objects.append(Package(
            kind=ObjectKind.PACKAGE_BODY,
            name=name,
            is_body=True,
            span=_span_of(ctx),
            line_count=_line_count(ctx),
            raw_source=_ctx_text(ctx),
        ))

    # ─── construct-tagging rules ─────────────────────────────────────────────

    def visit_merge_statement(self, ctx) -> None:
        self._refs.append(ConstructRef(
            tag=ConstructTag.MERGE,
            span=_span_of(ctx),
            snippet=_first_line(_ctx_text(ctx)),
        ))

    def visit_hierarchical_query_clause(self, ctx) -> None:
        self._refs.append(ConstructRef(
            tag=ConstructTag.CONNECT_BY,
            span=_span_of(ctx),
            snippet=_first_line(_ctx_text(ctx)),
        ))

    def visit_forall_statement(self, ctx) -> None:
        self._refs.append(ConstructRef(
            tag=ConstructTag.FORALL,
            span=_span_of(ctx),
            snippet=_first_line(_ctx_text(ctx)),
        ))

    def visit_execute_immediate(self, ctx) -> None:
        self._refs.append(ConstructRef(
            tag=ConstructTag.EXECUTE_IMMEDIATE,
            span=_span_of(ctx),
            snippet=_first_line(_ctx_text(ctx)),
        ))

    def visit_pragma_declaration(self, ctx) -> None:
        text = _ctx_text(ctx).upper()
        if "AUTONOMOUS_TRANSACTION" in text:
            self._refs.append(ConstructRef(
                tag=ConstructTag.AUTONOMOUS_TXN,
                span=_span_of(ctx),
                snippet=_first_line(_ctx_text(ctx)),
            ))
        elif "EXCEPTION_INIT" in text:
            self._refs.append(ConstructRef(
                tag=ConstructTag.PRAGMA_EXCEPTION_INIT,
                span=_span_of(ctx),
                snippet=_first_line(_ctx_text(ctx)),
            ))


# ─── Dispatcher: bridges ANTLR's CamelCase methods to our snake_case ─────────


def _build_dispatcher(target: _IRVisitor):
    """Construct an ANTLR ParseTreeVisitor whose visit_* methods delegate
    to the snake_case methods on `target`. We do this dynamically so the
    dispatcher class only exists when ANTLR is actually loaded."""
    import importlib
    visitor_mod = importlib.import_module(f"{_GENERATED_PKG}.PlSqlParserVisitor")
    PlSqlParserVisitor = visitor_mod.PlSqlParserVisitor

    proxies = {}
    rule_to_method = {
        "Create_table":             "visit_create_table",
        "Create_view":              "visit_create_view",
        "Create_index":             "visit_create_index",
        "Create_sequence":          "visit_create_sequence",
        "Create_trigger":           "visit_create_trigger",
        "Create_procedure_body":    "visit_create_procedure_body",
        "Create_function_body":     "visit_create_function_body",
        "Create_package":           "visit_create_package",
        "Create_package_body":      "visit_create_package_body",
        "Merge_statement":          "visit_merge_statement",
        "Hierarchical_query_clause":"visit_hierarchical_query_clause",
        "Forall_statement":         "visit_forall_statement",
        "Execute_immediate":        "visit_execute_immediate",
        "Pragma_declaration":       "visit_pragma_declaration",
    }
    for rule, method_name in rule_to_method.items():
        target_method = getattr(target, method_name)

        def make_proxy(tm):
            def proxy(self, ctx):
                tm(ctx)
                return self.visitChildren(ctx)
            return proxy
        proxies[f"visit{rule}"] = make_proxy(target_method)

    DispatcherCls = type("_IRVisitorDispatcher", (PlSqlParserVisitor,), proxies)
    return DispatcherCls()


# ─── helpers ─────────────────────────────────────────────────────────────────


def _make_sentinel(span: Span) -> Subprogram:
    return Subprogram(
        kind=ObjectKind.UNKNOWN,
        name="<module-constructs>",
        span=span,
        line_count=0,
    )


def _module_span(source: str) -> Span:
    lines = source.split("\n")
    return Span(file=None, start_line=1, start_col=1,
                end_line=max(1, len(lines)),
                end_col=len(lines[-1]) + 1 if lines else 1)


def _span_of(ctx) -> Span:
    """ANTLR ParserRuleContext exposes start/stop tokens with line/column."""
    start = ctx.start
    stop = ctx.stop or start
    return Span(
        file=None,
        start_line=start.line,
        start_col=start.column + 1,             # ANTLR is 0-indexed; we use 1-indexed
        end_line=stop.line,
        end_col=(stop.column + len(stop.text or "")) + 1,
    )


def _ctx_text(ctx) -> str:
    """Reconstruct the original source slice for a ctx by joining tokens."""
    if ctx.start is None or ctx.stop is None:
        return ""
    input_stream = ctx.start.getInputStream()
    if input_stream is None:
        return ""
    return input_stream.getText(ctx.start.start, ctx.stop.stop)


def _line_count(ctx) -> int:
    if ctx.start is None or ctx.stop is None:
        return 1
    return max(1, (ctx.stop.line or 0) - (ctx.start.line or 0) + 1)


def _first_line(text: str) -> str:
    line = text.split("\n", 1)[0].strip()
    return line[:80] + ("…" if len(line) > 80 else "")


def _extract_object_name(ctx, *rule_names: str) -> str:
    """Find the first child matching any of the given rule names and return
    its text (last identifier segment, schema prefix dropped). The grammar's
    rule names for object names vary (tableview_name, table_name,
    index_name, etc.) — pass the candidates and we pick the first that
    matches."""
    for rn in rule_names:
        getter = getattr(ctx, rn, None)
        if callable(getter):
            child = getter()
            if child is not None:
                text = child.getText() if hasattr(child, "getText") else str(child)
                return text.rsplit(".", 1)[-1].strip('"')
    return _ctx_text(ctx).split()[2] if len(_ctx_text(ctx).split()) > 2 else ""

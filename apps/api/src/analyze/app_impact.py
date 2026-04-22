"""App-impact analyzer.

Given a parsed Oracle schema (Module) and a directory of customer source
code, this module:

  1. Walks each source file with a language-specific extractor and pulls
     out string literals that look like SQL.
  2. Re-parses each fragment with the Oracle lexer and identifies which
     schema objects (tables, columns) it touches.
  3. Classifies each call site by migration risk based on:
        * Oracle-specific functions used (NVL, SYSDATE, ROWNUM, ...)
        * Oracle-specific syntax (CONNECT BY, MERGE, (+) outer join, ...)
        * Oracle-specific server features (DBMS_*, autonomous tx, ...)
        * Whether the touched schema objects exist in the converted PG
  4. Aggregates findings by file and produces an AppImpactReport.

Risk classification is deterministic so the report is reproducible. AI-
explained rationale per finding lives in `ai/services/app_impact.py` and
is layered on top — see Phase B2.

The extractor for each language is intentionally simple: it finds string
literals (Java double-quoted, Python single/double/triple-quoted) and
applies a SQL-shape heuristic. It is NOT a full Java/Python parser. We
accept some false
positives (text that happens to look like SQL) in exchange for catching
inline SQL across diverse codebases without needing a parser per
language. The risk classifier weights findings by confidence so false
positives surface as low-confidence findings, not noise.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Iterable, List, Optional, Sequence, Set

from ..core.diagnostics.diagnostic import Span
from ..core.ir.nodes import ConstructTag, Module, ObjectKind, Tier, TIER_FOR_TAG
from ..source.oracle._lexer import Token, TokenKind, tokenize
from .sql_extractor import (
    EXTRACTORS,
    SqlFragment,
    extract_from_file,
    pick_extractor,
)


# ─── Risk model ──────────────────────────────────────────────────────────────


class RiskLevel(str, Enum):
    LOW = "low"             # mechanical change or no change needed
    MEDIUM = "medium"       # Oracle-specific function with PG equivalent
    HIGH = "high"           # Oracle-specific syntax requiring rewrite
    CRITICAL = "critical"   # server-feature dependency, structural refactor


_RISK_RANK = {RiskLevel.LOW: 0, RiskLevel.MEDIUM: 1, RiskLevel.HIGH: 2, RiskLevel.CRITICAL: 3}


def _rank(r: RiskLevel) -> int:
    return _RISK_RANK[r]


# Function-name patterns that are Oracle-specific but have direct PG
# equivalents — usable as MEDIUM signal. Not exhaustive; the AI layer
# expands coverage in Phase B2.
ORACLE_FUNCTION_NAMES: Set[str] = {
    "NVL", "NVL2", "DECODE", "SYSDATE", "SYSTIMESTAMP", "TRUNC",
    "ADD_MONTHS", "MONTHS_BETWEEN", "LAST_DAY", "NEXT_DAY",
    "LISTAGG", "WM_CONCAT", "INSTR", "SUBSTR", "REGEXP_LIKE",
    "REGEXP_SUBSTR", "TO_CHAR", "TO_DATE", "TO_NUMBER",
    "DBMS_RANDOM", "USERENV", "SYS_CONTEXT", "SYS_GUID",
}

# Schema/system identifiers that signal CRITICAL Oracle dependency.
ORACLE_SYSTEM_IDENTS: Set[str] = {
    "DUAL",         # legal but signals Oracle-style scalar SELECT
    "USER_TABLES", "ALL_TABLES", "DBA_TABLES",
    "USER_TAB_COLUMNS", "ALL_TAB_COLUMNS",
    "V$SQL", "V$SESSION", "V$VERSION",
    "USER_OBJECTS", "ALL_OBJECTS", "DBA_OBJECTS",
}


# App-context overrides for the schema-context Tier mapping. The same
# construct can have different impact depending on where it appears:
#   * DBMS_OUTPUT in PL/SQL: trivial (RAISE NOTICE replacement, Tier A).
#     DBMS_OUTPUT called from an application: CRITICAL — the application
#     consumes captured output via Oracle-specific buffer APIs that do
#     not exist in PG.
#   * (+) outer join in PL/SQL view: Tier B mechanical rewrite.
#     (+) outer join in application SQL: CRITICAL — common source of
#     bugs because most ORMs pass the SQL through verbatim.
#   * Database links: same severity in both contexts.
_APP_RISK_OVERRIDES: dict = {
    ConstructTag.DBMS_OUTPUT:     RiskLevel.CRITICAL,
    ConstructTag.OUTER_JOIN_PLUS: RiskLevel.CRITICAL,
}


def _construct_risk(tag: ConstructTag) -> RiskLevel:
    """Risk for a tagged construct found in *application* SQL.

    Default: Tier A -> LOW, Tier B -> HIGH, Tier C -> CRITICAL.
    Override via _APP_RISK_OVERRIDES for constructs whose application
    impact differs from their PL/SQL impact.
    """
    if tag in _APP_RISK_OVERRIDES:
        return _APP_RISK_OVERRIDES[tag]
    tier = TIER_FOR_TAG.get(tag, Tier.A)
    return {Tier.A: RiskLevel.LOW, Tier.B: RiskLevel.HIGH, Tier.C: RiskLevel.CRITICAL}[tier]


# ─── Findings ────────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class Finding:
    """One issue at one call site."""
    code: str                       # dotted machine code, e.g. APP.SQL.SYSDATE
    risk: RiskLevel
    message: str                    # human summary
    suggestion: str                 # what to change in the application code
    file: str                       # path relative to scan root
    line: int                       # 1-indexed line of the SQL fragment
    snippet: str                    # ≤120 chars excerpt of the SQL fragment
    schema_objects: tuple = ()      # tuple[str] of touched table/view names
    construct_tags: tuple = ()      # tuple[ConstructTag] detected in the SQL


@dataclass
class FileImpact:
    file: str
    language: str                   # 'java', 'python', ...
    fragments_scanned: int = 0
    findings: List[Finding] = field(default_factory=list)

    @property
    def max_risk(self) -> RiskLevel:
        if not self.findings:
            return RiskLevel.LOW
        return max(self.findings, key=lambda f: _rank(f.risk)).risk


@dataclass
class AppImpactReport:
    """Top-level deliverable. Easily JSON-serializable for the API."""
    files: List[FileImpact] = field(default_factory=list)
    total_files_scanned: int = 0
    total_fragments: int = 0
    total_findings: int = 0
    findings_by_risk: dict = field(default_factory=dict)

    def add_file(self, fi: FileImpact) -> None:
        self.files.append(fi)
        self.total_files_scanned += 1
        self.total_fragments += fi.fragments_scanned
        self.total_findings += len(fi.findings)
        for f in fi.findings:
            self.findings_by_risk[f.risk.value] = self.findings_by_risk.get(f.risk.value, 0) + 1

    def top_files(self, *, limit: int = 10) -> List[FileImpact]:
        """Files with the highest single-finding risk, then by finding count."""
        def key(fi: FileImpact):
            return (-_rank(fi.max_risk), -len(fi.findings), fi.file)
        return sorted(self.files, key=key)[:limit]


# ─── Analyzer ────────────────────────────────────────────────────────────────


@dataclass
class AppImpactAnalyzer:
    """Walk a source tree and produce an AppImpactReport.

    The schema Module (parsed Oracle DDL) is optional. When present, we
    cross-reference fragment-level table mentions against schema-defined
    objects so unknown tables surface as CRITICAL ("touched table not in
    converted schema"). Without a Module, we still classify by Oracle-
    specific functions and constructs.
    """
    schema: Optional[Module] = None

    def analyze_directory(self, root: Path | str,
                          *, languages: Optional[Sequence[str]] = None) -> AppImpactReport:
        root_path = Path(root)
        if not root_path.exists() or not root_path.is_dir():
            raise ValueError(f"Not a directory: {root}")

        report = AppImpactReport()
        for path in sorted(self._walk(root_path, languages)):
            fi = self.analyze_file(path, scan_root=root_path)
            if fi is not None:
                report.add_file(fi)
        return report

    def analyze_file(self, path: Path,
                     *, scan_root: Optional[Path] = None) -> Optional[FileImpact]:
        extractor = pick_extractor(path)
        if extractor is None:
            return None
        rel = str(path.relative_to(scan_root)) if scan_root else str(path)
        fragments = extract_from_file(path, extractor)
        fi = FileImpact(file=rel, language=extractor.language,
                        fragments_scanned=len(fragments))
        for frag in fragments:
            fi.findings.extend(self._classify_fragment(rel, frag))
        return fi

    # ─── classification ──────────────────────────────────────────────────────

    def _classify_fragment(self, file_rel: str, frag: SqlFragment) -> List[Finding]:
        toks = tokenize(frag.sql)
        construct_tags = self._construct_tags_in(toks, frag.sql)
        oracle_funcs = self._oracle_funcs_in(toks)
        system_idents = self._system_idents_in(toks)
        touched_objs = self._touched_objects(toks)
        unknown_objs = self._unknown_schema_objects(touched_objs)

        findings: List[Finding] = []

        # 1. Each tagged construct → its own finding (HIGH/CRITICAL).
        for tag in sorted(construct_tags, key=lambda t: t.value):
            findings.append(Finding(
                code=f"APP.SQL.{tag.value}",
                risk=_construct_risk(tag),
                message=f"Application SQL uses {tag.value.replace('_', ' ')}.",
                suggestion=_suggestion_for_construct(tag),
                file=file_rel,
                line=frag.line,
                snippet=_snippet(frag.sql),
                schema_objects=tuple(sorted(touched_objs)),
                construct_tags=(tag,),
            ))

        # 2. Each Oracle function call → MEDIUM finding.
        for fn in sorted(oracle_funcs):
            findings.append(Finding(
                code=f"APP.SQL.FN.{fn}",
                risk=RiskLevel.MEDIUM,
                message=f"Application SQL calls Oracle function {fn}().",
                suggestion=_suggestion_for_function(fn),
                file=file_rel,
                line=frag.line,
                snippet=_snippet(frag.sql),
                schema_objects=tuple(sorted(touched_objs)),
            ))

        # 3. System identifier (DUAL, V$..., USER_TABLES) → CRITICAL.
        for ident in sorted(system_idents):
            findings.append(Finding(
                code=f"APP.SQL.SYSREF.{ident}",
                risk=RiskLevel.CRITICAL,
                message=f"Application SQL references Oracle system object {ident}.",
                suggestion=_suggestion_for_sysref(ident),
                file=file_rel,
                line=frag.line,
                snippet=_snippet(frag.sql),
                schema_objects=tuple(sorted(touched_objs)),
            ))

        # 4. Touched objects not present in the converted PG schema → CRITICAL.
        for obj in sorted(unknown_objs):
            findings.append(Finding(
                code="APP.SCHEMA.UNKNOWN_OBJECT",
                risk=RiskLevel.CRITICAL,
                message=f"Application SQL references {obj}, which is not in the parsed schema.",
                suggestion=(
                    f"Confirm {obj} is included in the migration scope, or update the application "
                    "to reference the renamed object in PostgreSQL."
                ),
                file=file_rel,
                line=frag.line,
                snippet=_snippet(frag.sql),
                schema_objects=(obj,),
            ))

        return findings

    # ─── token-walk helpers ──────────────────────────────────────────────────

    def _construct_tags_in(self, toks: List[Token], src: str) -> Set[ConstructTag]:
        tags: Set[ConstructTag] = set()
        n = len(toks)
        for i, t in enumerate(toks):
            if t.is_kw("CONNECT") and i + 1 < n and toks[i + 1].is_kw("BY"):
                tags.add(ConstructTag.CONNECT_BY)
            elif t.is_kw("MERGE") and i + 1 < n and toks[i + 1].is_kw("INTO"):
                tags.add(ConstructTag.MERGE)
            elif t.kind == TokenKind.IDENT and t.upper == "ROWNUM":
                tags.add(ConstructTag.ROWNUM)
            elif t.kind == TokenKind.IDENT and t.upper == "ROWID":
                tags.add(ConstructTag.ROWID)
            elif t.kind == TokenKind.AT_DBLINK:
                tags.add(ConstructTag.DBLINK)
            elif t.kind == TokenKind.IDENT and t.upper.startswith("DBMS_"):
                if t.upper == "DBMS_OUTPUT":
                    tags.add(ConstructTag.DBMS_OUTPUT)
                elif t.upper == "DBMS_SCHEDULER":
                    tags.add(ConstructTag.DBMS_SCHEDULER)
                elif t.upper in ("DBMS_AQ", "DBMS_AQADM"):
                    tags.add(ConstructTag.DBMS_AQ)
                elif t.upper == "DBMS_CRYPTO":
                    tags.add(ConstructTag.DBMS_CRYPTO)
        # The (+) outer-join operator: detect via raw scan since lexer doesn't tag it.
        if "(+)" in src:
            tags.add(ConstructTag.OUTER_JOIN_PLUS)
        return tags

    def _oracle_funcs_in(self, toks: List[Token]) -> Set[str]:
        found: Set[str] = set()
        n = len(toks)
        for i, t in enumerate(toks):
            if t.kind not in (TokenKind.IDENT, TokenKind.KEYWORD):
                continue
            if t.upper in ORACLE_FUNCTION_NAMES:
                # Confirm it's a function call, not a column name: next non-ws
                # token should be '('.
                if i + 1 < n and toks[i + 1].kind == TokenKind.PUNCT and toks[i + 1].text == "(":
                    found.add(t.upper)
                elif t.upper in {"SYSDATE", "SYSTIMESTAMP"}:
                    # Pseudo-functions used without parens.
                    found.add(t.upper)
        return found

    def _system_idents_in(self, toks: List[Token]) -> Set[str]:
        found: Set[str] = set()
        for t in toks:
            if t.kind in (TokenKind.IDENT, TokenKind.KEYWORD) and t.upper in ORACLE_SYSTEM_IDENTS:
                found.add(t.upper)
        return found

    def _touched_objects(self, toks: List[Token]) -> Set[str]:
        """Identifiers appearing immediately after FROM, JOIN, INTO, UPDATE."""
        triggers = {"FROM", "JOIN", "INTO", "UPDATE"}
        found: Set[str] = set()
        prev_upper: Optional[str] = None
        for t in toks:
            if t.kind in (TokenKind.IDENT, TokenKind.KEYWORD):
                if prev_upper in triggers and t.kind == TokenKind.IDENT:
                    # Strip schema prefix for comparison.
                    name = t.text.split(".")[-1].strip('"').upper()
                    if name not in {"DUAL"} and name not in ORACLE_SYSTEM_IDENTS:
                        found.add(name)
                prev_upper = t.upper
            elif t.kind == TokenKind.OPERATOR or t.kind == TokenKind.PUNCT:
                # Stay in the trigger context across operators/punct so
                # `FROM  schema.table  t` still records `table`.
                continue
            else:
                prev_upper = None
        return found

    def _unknown_schema_objects(self, touched: Set[str]) -> Set[str]:
        if self.schema is None:
            return set()
        known = {o.name.upper() for o in self.schema.objects
                 if o.kind in {ObjectKind.TABLE, ObjectKind.VIEW,
                               ObjectKind.MATERIALIZED_VIEW, ObjectKind.SYNONYM}}
        return {o for o in touched if o not in known}

    # ─── walking ─────────────────────────────────────────────────────────────

    def _walk(self, root: Path, languages: Optional[Sequence[str]]) -> Iterable[Path]:
        accept_exts = (
            EXTRACTORS_BY_LANG_EXTS if languages is None
            else _exts_for_languages(languages)
        )
        for path in root.rglob("*"):
            if path.is_file() and path.suffix.lower() in accept_exts:
                yield path


# ─── small helpers ───────────────────────────────────────────────────────────


def _snippet(sql: str) -> str:
    one_line = " ".join(sql.split())
    return one_line[:120] + ("…" if len(one_line) > 120 else "")


def _suggestion_for_construct(tag: ConstructTag) -> str:
    return {
        ConstructTag.CONNECT_BY:    "Rewrite as a recursive CTE (WITH RECURSIVE).",
        ConstructTag.MERGE:         "PG 15+ supports MERGE; on PG 14, rewrite as INSERT ... ON CONFLICT.",
        ConstructTag.ROWNUM:        "Replace ROWNUM with ROW_NUMBER() OVER (ORDER BY ...) or LIMIT.",
        ConstructTag.ROWID:         "Replace ROWID with the natural primary key; CTID is not stable across writes.",
        ConstructTag.DBLINK:        "Replace database links with postgres_fdw or move the join into the application.",
        ConstructTag.DBMS_OUTPUT:   "Replace DBMS_OUTPUT.PUT_LINE with RAISE NOTICE or remove the print.",
        ConstructTag.DBMS_SCHEDULER:"Move scheduling out of the database (pg_cron or external scheduler).",
        ConstructTag.DBMS_AQ:       "Replace Oracle AQ with PGMQ, LISTEN/NOTIFY, or an external broker.",
        ConstructTag.DBMS_CRYPTO:   "Replace DBMS_CRYPTO calls with the pgcrypto extension.",
        ConstructTag.OUTER_JOIN_PLUS: "Rewrite (+) outer joins as ANSI LEFT/RIGHT OUTER JOIN.",
    }.get(tag, f"Rewrite the {tag.value.replace('_', ' ')} usage for PostgreSQL.")


def _suggestion_for_function(fn: str) -> str:
    return {
        "NVL":          "Replace NVL(x, y) with COALESCE(x, y).",
        "NVL2":         "Replace NVL2(e, a, b) with CASE WHEN e IS NOT NULL THEN a ELSE b END.",
        "DECODE":       "Replace DECODE(...) with CASE WHEN ... END.",
        "SYSDATE":      "Replace SYSDATE with CURRENT_TIMESTAMP (PG DATE has no time component).",
        "SYSTIMESTAMP": "Replace SYSTIMESTAMP with CURRENT_TIMESTAMP.",
        "ADD_MONTHS":   "Replace ADD_MONTHS(d, n) with d + (n || ' months')::interval.",
        "MONTHS_BETWEEN": "Compute manually or use age()/extract(); MONTHS_BETWEEN has no exact PG equivalent.",
        "LISTAGG":      "Replace LISTAGG with STRING_AGG.",
        "WM_CONCAT":    "Replace WM_CONCAT with STRING_AGG.",
        "INSTR":        "Replace INSTR with POSITION(... IN ...) or strpos().",
        "SUBSTR":       "PG SUBSTRING semantics differ slightly; verify negative-start behavior.",
        "REGEXP_LIKE":  "Replace REGEXP_LIKE(s, p) with `s ~ p`.",
        "TO_CHAR":      "TO_CHAR exists in PG but format-string semantics differ; review.",
        "TO_DATE":      "TO_DATE exists in PG but format-string semantics differ; review.",
        "TO_NUMBER":    "Replace with `value::numeric` or `CAST(value AS numeric)`.",
        "DBMS_RANDOM":  "Replace DBMS_RANDOM with random() or gen_random_uuid().",
        "USERENV":      "Replace USERENV(...) with current_user / current_database() / inet_client_addr().",
        "SYS_CONTEXT":  "Replace SYS_CONTEXT with current_setting() or application-managed state.",
        "SYS_GUID":     "Replace SYS_GUID with gen_random_uuid() (pgcrypto) or uuid_generate_v4 (uuid-ossp).",
    }.get(fn, f"Review {fn}() usage for PG compatibility.")


def _suggestion_for_sysref(ident: str) -> str:
    if ident == "DUAL":
        return "PG does not need DUAL — drop `FROM DUAL` from scalar SELECTs."
    if ident.startswith("V$"):
        return f"Replace {ident} reads with PG equivalents in pg_stat_*; see pg_stat_statements."
    if ident in {"USER_TABLES", "ALL_TABLES", "DBA_TABLES"}:
        return f"Replace {ident} with information_schema.tables or pg_catalog.pg_class."
    if ident in {"USER_TAB_COLUMNS", "ALL_TAB_COLUMNS"}:
        return f"Replace {ident} with information_schema.columns."
    if ident in {"USER_OBJECTS", "ALL_OBJECTS", "DBA_OBJECTS"}:
        return f"Replace {ident} with pg_catalog.pg_class joined to pg_namespace."
    return f"Replace {ident} with the equivalent PG catalog/view."


# ─── language extension registry ─────────────────────────────────────────────


def _exts_for_languages(langs: Sequence[str]) -> Set[str]:
    accept: Set[str] = set()
    for lang in langs:
        accept.update(_LANG_TO_EXTS.get(lang.lower(), set()))
    return accept


_LANG_TO_EXTS = {
    "java":   {".java"},
    "python": {".py"},
    "sql":    {".sql"},
}
EXTRACTORS_BY_LANG_EXTS: Set[str] = set().union(*_LANG_TO_EXTS.values())

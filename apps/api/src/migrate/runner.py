"""Data-movement runner.

Wires the planner + keyset query builder + COPY writer + sequence
catch-up + Merkle verifier into a single end-to-end loop. The runner
takes a `LoadPlan` and two database handles (source = Oracle/Postgres,
target = Postgres) and walks each table:

    for group in plan.groups:
        with deferred_constraints(group):
            for table in group.tables:
                copy_table(source, target, table)
    catch_up_sequences(target)
    verify(source, target, plan)

Checkpointing is built in: after every batch, we record `{table,
last_pk}` so a resumed run picks up exactly where the crash hit. The
verifier runs at the end and reports per-table results — failures don't
roll back the load (the data is already there); they surface so a
human can decide between bisecting, retrying the bad table, or
accepting the discrepancy.

Source-side reads use SQLAlchemy text() statements, so the runner
works for both Oracle and Postgres sources without dialect branching.
The target side uses raw psycopg for COPY.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable, Dict, Iterable, Iterator, List, Optional, Sequence

from sqlalchemy import text

from .copy import CopyResult, copy_rows_to_postgres
from .keyset import Dialect, build_first_page, build_next_page
from .planner import LoadGroup, LoadPlan, TableRef
from .sequences import CatchupResult, catch_up_all
from .verify import TableHash, hash_table


# ─── Public types ────────────────────────────────────────────────────────────


@dataclass
class TableSpec:
    """Per-table introspection result. The runner needs one per table
    before any reading begins.

    `source_table` and `target_table` are separate because the production
    case is Oracle→Postgres (HR.EMPLOYEES → public.employees). If
    `target_table` is omitted, the runner uses `source_table` for both —
    handy for Postgres-to-Postgres tests and identity migrations.
    """

    source_table: TableRef
    columns: List[str]
    pk_columns: List[str]
    target_table: TableRef | None = None

    def __post_init__(self) -> None:
        if self.target_table is None:
            self.target_table = self.source_table

    @property
    def pk_indexes(self) -> List[int]:
        return [self.columns.index(c) for c in self.pk_columns]


@dataclass
class TableRunResult:
    rows_copied: int
    last_pk: tuple | None
    source_hash: TableHash
    target_hash: TableHash
    verified: bool

    @property
    def discrepancy(self) -> str | None:
        if self.verified:
            return None
        if self.source_hash.row_count != self.target_hash.row_count:
            return (
                f"row-count mismatch: source={self.source_hash.row_count}, "
                f"target={self.target_hash.row_count}"
            )
        return "merkle root mismatch"


@dataclass
class RunResult:
    tables: Dict[str, TableRunResult] = field(default_factory=dict)
    sequences: List[CatchupResult] = field(default_factory=list)

    @property
    def all_verified(self) -> bool:
        return all(r.verified for r in self.tables.values())

    @property
    def total_rows(self) -> int:
        return sum(r.rows_copied for r in self.tables.values())


# ─── Checkpoint hook ─────────────────────────────────────────────────────────


CheckpointFn = Callable[[TableRef, tuple | None, int], None]
"""Called after every batch with (table, last_pk, rows_so_far). Default
is a no-op; production callers wire this into CheckpointManager so a
crashed run resumes from the last successful batch."""


def _noop_checkpoint(table: TableRef, last_pk: tuple | None, rows: int) -> None:
    pass


ResumeFn = Callable[[TableRef], Optional[tuple]]
"""Called at the start of each table copy to ask: "is there already a
checkpoint for this table from a prior run?" Returns the last PK that
was successfully loaded, or None to start from the beginning. The
runner seeds its keyset walk with this PK so already-copied rows are
skipped."""


def _noop_resume(table: TableRef) -> Optional[tuple]:
    return None


RowTransformFn = Callable[[List[tuple], "TableSpec"], List[tuple]]
"""Optional per-batch row transformer. Called after each source fetch
and before the target COPY write. Used for PII masking: the service
layer compiles masking rules into a transform and hands it to the
Runner. Verification hashes are computed over the *post-transform*
source stream so the source/target merkle roots still match when
masking is active."""


# ─── Runner ──────────────────────────────────────────────────────────────────


@dataclass
class Runner:
    """Stateful coordinator. One instance per migration run.

    `source_session` and `target_session` are SQLAlchemy Sessions for
    reads and metadata work. `target_pg_conn` is a raw psycopg
    connection on the same database as `target_session`, used for the
    binary COPY protocol (which SQLAlchemy doesn't expose).
    """

    source_session: object
    target_session: object
    target_pg_conn: object
    source_dialect: Dialect
    batch_size: int = 5000
    checkpoint: CheckpointFn = _noop_checkpoint
    resume: ResumeFn = _noop_resume
    row_transform: Optional[RowTransformFn] = None
    # Per-target-table list of self-referential FK columns to handle
    # via NULL-then-UPDATE. Required when the operator pre-creates the
    # target with the self-FK installed AND the source data isn't
    # hierarchically ordered (parent rows always loading before
    # children). The runner NULLs these columns during COPY so the FK
    # check passes, then issues an UPDATE pass to populate them after
    # the data is in. Empty default keeps the existing flow unchanged.
    null_then_update_columns: Dict[str, List[str]] = field(default_factory=dict)

    def execute(self, plan: LoadPlan, specs: Dict[str, TableSpec]) -> RunResult:
        """Run the entire plan. Returns a RunResult with per-table
        verification and sequence catch-up details. The plan refers to
        target tables; `specs` is keyed on the target table's qualified
        name."""
        result = RunResult()
        for group in plan.groups:
            with self._deferred_constraints(group):
                for target in group.tables:
                    spec = specs[target.qualified()]
                    result.tables[target.qualified()] = self._copy_table(spec)
        # Sequences run after everything is loaded; if they fail, the
        # data is still correct, just the next INSERT will collide.
        target_schema = self._pick_target_schema(plan)
        if target_schema:
            result.sequences = catch_up_all(self.target_pg_conn, schema=target_schema)
        return result

    # ─── per-table ──────────────────────────────────────────────────────────

    def _copy_table(self, spec: TableSpec) -> TableRunResult:
        rows_total = 0
        # If a prior run checkpointed a PK for this target, skip ahead.
        # The verification passes below intentionally do NOT resume —
        # they read the whole table on both sides to hash-compare.
        last_pk: tuple | None = self.resume(spec.target_table)
        source_batches = self._iter_batches(
            self.source_session,
            self.source_dialect,
            spec.source_table,
            spec,
            start_after_pk=last_pk,
        )

        # Self-referential FK handling: if the caller flagged columns
        # for null-then-update, we NULL them during COPY so the FK
        # check passes, then run a follow-up UPDATE pass once all
        # rows are in. Index the columns once outside the hot loop.
        null_cols = self.null_then_update_columns.get(spec.target_table.qualified(), [])
        null_indexes = (
            [spec.columns.index(c) for c in null_cols] if null_cols else []
        )

        for batch in self._apply_transform(source_batches, spec):
            copy_batch = (
                _null_columns(batch, null_indexes) if null_indexes else batch
            )
            cp: CopyResult = copy_rows_to_postgres(
                pg_conn=self.target_pg_conn,
                table=spec.target_table.qualified(),
                columns=spec.columns,
                rows=copy_batch,
                pk_column_indexes=spec.pk_indexes,
            )
            rows_total += cp.rows_written
            if cp.last_pk is not None:
                last_pk = cp.last_pk
            self.checkpoint(spec.target_table, last_pk, rows_total)

        # Pass 2 for self-FK tables: re-read the source and UPDATE the
        # target's deferred columns. Per-row UPDATE is the simple v1;
        # a temp-table-driven bulk update would be faster on large
        # tables and is the obvious follow-up if this becomes a
        # bottleneck. The verify hashes downstream read the post-
        # update target, so they match the source as expected.
        if null_indexes:
            self._populate_self_fks(spec, null_cols)

        # Two independent passes for verification — iterators can't be
        # replayed, and a second cheap read avoids holding the entire
        # table in memory. When masking is active, we hash the
        # post-transform source so the root matches the target (which
        # is already masked).
        source_hash = hash_table(
            self._apply_transform(
                self._iter_batches(self.source_session, self.source_dialect, spec.source_table, spec),
                spec,
            )
        )
        target_hash = hash_table(
            self._iter_batches(self.target_session, Dialect.POSTGRES, spec.target_table, spec)
        )
        return TableRunResult(
            rows_copied=rows_total,
            last_pk=last_pk,
            source_hash=source_hash,
            target_hash=target_hash,
            verified=source_hash.matches(target_hash),
        )

    def _populate_self_fks(self, spec: TableSpec, null_cols: List[str]) -> None:
        """Pass 2 for self-FK tables: UPDATE the target's deferred
        columns from the source.

        Re-reads the source so we don't have to materialize the whole
        table in memory between passes. For each row, builds an
        UPDATE … WHERE pk = ... that writes the original FK values.
        """
        from sqlalchemy import text as _text

        col_indexes = [spec.columns.index(c) for c in null_cols]
        pk_indexes = list(spec.pk_indexes)
        pk_cols = list(spec.pk_columns)
        target_q = _quote_qualified(spec.target_table.qualified())
        set_clause = ", ".join(f'"{c}" = :v{i}' for i, c in enumerate(null_cols))
        where_clause = " AND ".join(
            f'"{c}" = :p{i}' for i, c in enumerate(pk_cols)
        )
        sql = f"UPDATE {target_q} SET {set_clause} WHERE {where_clause}"

        for batch in self._iter_batches(
            self.source_session, self.source_dialect, spec.source_table, spec
        ):
            for row in batch:
                params: dict = {}
                for i, idx in enumerate(col_indexes):
                    params[f"v{i}"] = row[idx]
                for i, idx in enumerate(pk_indexes):
                    params[f"p{i}"] = row[idx]
                self.target_session.execute(_text(sql), params)
        self.target_session.commit()

    def _apply_transform(
        self, batches: Iterator[List[Sequence]], spec: TableSpec
    ) -> Iterator[List[Sequence]]:
        """Pass every batch through the configured row_transform, or
        yield unchanged if none is set. Called for both the copy loop
        and the source-side verification hash so both see the same
        post-transform stream."""
        if self.row_transform is None:
            yield from batches
            return
        for batch in batches:
            yield self.row_transform(batch, spec)

    def _iter_batches(
        self,
        session,
        dialect: Dialect,
        table: TableRef,
        spec: TableSpec,
        start_after_pk: tuple | None = None,
    ) -> Iterator[List[Sequence]]:
        yield from _stream_batches(
            session,
            dialect,
            table,
            spec.columns,
            spec.pk_columns,
            self.batch_size,
            start_after_pk=start_after_pk,
        )

    # ─── group-level constraint deferral ────────────────────────────────────

    def _deferred_constraints(self, group: LoadGroup):
        """Context manager that issues `SET CONSTRAINTS ... DEFERRED` for
        every FK that needs deferring inside a cycle group, and lets the
        outer COMMIT enforce them at exit. For acyclic groups it's a
        no-op."""
        runner = self

        class _Ctx:
            def __enter__(self_inner):
                if not group.deferred_constraints:
                    return
                names = ", ".join(
                    f'"{fk.name}"' for fk in group.deferred_constraints
                )
                runner.target_session.execute(text(f"SET CONSTRAINTS {names} DEFERRED"))

            def __exit__(self_inner, *exc):
                # Constraints become IMMEDIATE again on the next
                # statement automatically when we return to the default
                # mode at COMMIT time. Nothing to do here.
                return False

        return _Ctx()

    def _pick_target_schema(self, plan: LoadPlan) -> str | None:
        for group in plan.groups:
            for tbl in group.tables:
                if tbl.schema:
                    return tbl.schema
        return None


# ─── Batch streaming (free function so tests can hit it directly) ────────────


def _stream_batches(
    session,
    dialect: Dialect,
    table: TableRef,
    columns: Sequence[str],
    pk_columns: Sequence[str],
    batch_size: int,
    start_after_pk: tuple | None = None,
) -> Iterator[List[Sequence]]:
    """Yield batches of `batch_size` rows from `table` via keyset
    pagination. Stops when a page returns fewer rows than requested.
    Caller-supplied `columns` and `pk_columns` so the same helper works
    for source-side reads and target-side verification reads.

    If `start_after_pk` is supplied, the first page is built as a
    keyset-continuation (`WHERE pk > start_after_pk`), so resumed runs
    skip rows already loaded by a prior attempt."""
    pk_indexes = [columns.index(c) for c in pk_columns]
    last_pk: tuple | None = start_after_pk
    while True:
        if last_pk is None:
            q = build_first_page(
                dialect=dialect,
                table=table.qualified(),
                columns=columns,
                pk_columns=pk_columns,
                batch_size=batch_size,
            )
        else:
            q = build_next_page(
                dialect=dialect,
                table=table.qualified(),
                columns=columns,
                pk_columns=pk_columns,
                last_pk=list(last_pk),
                batch_size=batch_size,
            )
        rows = list(session.execute(text(q.sql), q.params).all())
        if not rows:
            return
        # SQLAlchemy returns Row objects; convert to plain tuples so
        # downstream consumers (COPY writer, hash) deal with one shape.
        # Each value is also materialized — oracledb returns CLOB/BLOB
        # as lazy LOB objects that can't be serialized by COPY and that
        # `repr()` to their memory address (catastrophic for hashing).
        plain = [tuple(_materialize_value(v) for v in r) for r in rows]
        yield plain
        if len(plain) < batch_size:
            return
        last_row = plain[-1]
        last_pk = tuple(last_row[i] for i in pk_indexes)


# ─── Driver-specific value coercion ──────────────────────────────────────────


# Primitive Python types that drivers return directly. We pre-list them so
# the materializer's hot loop can short-circuit without touching getattr().
_PRIMITIVE_TYPES = (
    type(None),
    str,
    bytes,
    bytearray,
    memoryview,
    int,
    float,
    bool,
)


def _materialize_value(v):
    """Resolve driver-specific lazy value types into concrete Python.

    The motivating case: oracledb returns CLOB/NCLOB/BLOB columns as
    `oracledb.LOB` objects whose contents only load when ``.read()``
    is called. Without this step:
      * psycopg's binary COPY can't serialize a LOB object → migration
        crashes when it hits any LOB-bearing table.
      * verify.py's hash falls into the ``R:`` fallback and ``repr()``s
        the LOB, which prints the memory address — different on every
        read, so the source/target merkle roots never agree.

    Duck-typed by design: we recognize anything with a callable
    ``.read()`` that isn't a known primitive. In practice this only
    matches oracledb LOBs (and any future analogous lazy-fetch type),
    and it keeps this module independent of the oracledb import path.

    Read failures fall back to returning the original object — the
    downstream COPY or hash will surface a clearer error than a
    swallowed exception here would.
    """
    if isinstance(v, _PRIMITIVE_TYPES):
        return v
    read = getattr(v, "read", None)
    if callable(read):
        try:
            return read()
        except Exception:  # noqa: BLE001 — driver-specific surface
            return v
    return v


# ─── Self-FK helpers ─────────────────────────────────────────────────────────


def _null_columns(
    batch: List[Sequence], indexes: Sequence[int]
) -> List[Sequence]:
    """Return a new list of rows with the columns at `indexes` set to
    None. Used by the self-FK NULL-then-UPDATE path so the COPY
    doesn't trip a foreign-key check that the second pass will then
    satisfy by writing the real values."""
    out: List[Sequence] = []
    for row in batch:
        as_list = list(row)
        for idx in indexes:
            as_list[idx] = None
        out.append(tuple(as_list))
    return out


def _quote_qualified(qualified: str) -> str:
    """Quote `schema.table` (or bare `table`) for inclusion in a SQL
    statement. Mirrors keyset.py's quoting rule — quote everything,
    no bare names."""
    if "." in qualified:
        schema, name = qualified.split(".", 1)
        return f'"{schema}"."{name}"'
    return f'"{qualified}"'

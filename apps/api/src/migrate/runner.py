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
        for batch in self._apply_transform(source_batches, spec):
            cp: CopyResult = copy_rows_to_postgres(
                pg_conn=self.target_pg_conn,
                table=spec.target_table.qualified(),
                columns=spec.columns,
                rows=batch,
                pk_column_indexes=spec.pk_indexes,
            )
            rows_total += cp.rows_written
            if cp.last_pk is not None:
                last_pk = cp.last_pk
            self.checkpoint(spec.target_table, last_pk, rows_total)

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
        plain = [tuple(r) for r in rows]
        yield plain
        if len(plain) < batch_size:
            return
        last_row = plain[-1]
        last_pk = tuple(last_row[i] for i in pk_indexes)

"""End-to-end runner tests using Postgres-as-source-and-target.

The production flow has Oracle on the source side, but the runner is
dialect-agnostic. We use two schemas in the same Postgres for the test
rig — one acts as the source (production data), one as the target
(empty, ready to receive). This exercises the orchestrator without an
Oracle container.
"""

from __future__ import annotations

import uuid

import psycopg
import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.config import settings
from src.migrate.ddl import apply_ddl, generate_schema_ddl, map_pg_type
from src.migrate.introspect import introspect
from src.migrate.keyset import Dialect
from src.migrate.planner import LoadGroup, LoadPlan, TableRef
from src.migrate.runner import Runner, TableSpec, _stream_batches


# ─── Test rig ────────────────────────────────────────────────────────────────


@pytest.fixture
def pg_url():
    return settings.database_url.replace("postgresql+psycopg://", "postgresql://")


@pytest.fixture
def schemas(pg_url):
    """Two throwaway schemas — `src_*` and `dst_*` — torn down after."""
    src = f"runner_src_{uuid.uuid4().hex[:6]}"
    dst = f"runner_dst_{uuid.uuid4().hex[:6]}"
    conn = psycopg.connect(pg_url)
    with conn.cursor() as cur:
        cur.execute(f"CREATE SCHEMA {src}")
        cur.execute(f"CREATE SCHEMA {dst}")
    conn.commit()
    conn.close()
    yield src, dst
    conn = psycopg.connect(pg_url)
    with conn.cursor() as cur:
        cur.execute(f"DROP SCHEMA {src} CASCADE")
        cur.execute(f"DROP SCHEMA {dst} CASCADE")
    conn.commit()
    conn.close()


@pytest.fixture
def sessions(pg_url):
    """Two SQLAlchemy sessions on the same DB plus a raw psycopg conn
    for COPY. The session/connection split mirrors how production wires
    things — SQLAlchemy is for ORM/DDL, psycopg is for binary COPY."""
    engine = create_engine(settings.database_url)
    SrcSession = sessionmaker(bind=engine)
    DstSession = sessionmaker(bind=engine)
    src = SrcSession()
    dst = DstSession()
    pg_conn = psycopg.connect(pg_url, autocommit=True)  # so COPY commits immediately
    yield src, dst, pg_conn
    src.close()
    dst.close()
    pg_conn.close()


def _create_seeded_source(pg_conn, schema: str, table: str, rows: list[tuple]) -> None:
    with pg_conn.cursor() as cur:
        cur.execute(
            f"CREATE TABLE {schema}.{table} (id INTEGER PRIMARY KEY, label TEXT, qty INTEGER)"
        )
        if rows:
            cur.executemany(
                f"INSERT INTO {schema}.{table} (id, label, qty) VALUES (%s, %s, %s)",
                rows,
            )


def _create_empty_target(pg_conn, schema: str, table: str) -> None:
    with pg_conn.cursor() as cur:
        cur.execute(
            f"CREATE TABLE {schema}.{table} (id INTEGER PRIMARY KEY, label TEXT, qty INTEGER)"
        )


def _spec(src_schema: str, dst_schema: str, table: str, *, pk=("id",), cols=("id", "label", "qty")) -> TableSpec:
    return TableSpec(
        source_table=TableRef(schema=src_schema, name=table),
        target_table=TableRef(schema=dst_schema, name=table),
        columns=list(cols),
        pk_columns=list(pk),
    )


# ─── Single-table happy path via Runner.execute ──────────────────────────────


def test_runner_executes_single_table_plan(schemas, sessions):
    src_schema, dst_schema = schemas
    src_session, dst_session, pg_conn = sessions
    rows = [(i, f"item-{i}", i * 10) for i in range(1, 51)]
    _create_seeded_source(pg_conn, src_schema, "items", rows)
    _create_empty_target(pg_conn, dst_schema, "items")

    spec = _spec(src_schema, dst_schema, "items")
    plan = LoadPlan(groups=[LoadGroup(tables=[spec.target_table])])
    runner = Runner(
        source_session=src_session,
        target_session=dst_session,
        target_pg_conn=pg_conn,
        source_dialect=Dialect.POSTGRES,
        batch_size=20,
    )
    result = runner.execute(plan, {spec.target_table.qualified(): spec})

    target_result = result.tables[spec.target_table.qualified()]
    assert target_result.rows_copied == 50
    assert target_result.last_pk == (50,)
    assert target_result.verified
    assert result.all_verified
    assert result.total_rows == 50


# ─── Verifier flags discrepancies ────────────────────────────────────────────


def test_runner_flags_corrupted_target(schemas, sessions):
    src_schema, dst_schema = schemas
    src_session, dst_session, pg_conn = sessions
    _create_seeded_source(
        pg_conn, src_schema, "items", [(1, "a", 10), (2, "b", 20), (3, "c", 30)]
    )
    _create_empty_target(pg_conn, dst_schema, "items")
    # Pre-populate the target with a wrong row — the COPY will fail on
    # PK conflict, so we corrupt AFTER the COPY by patching the runner
    # to write to a separate scratch table. Easier: skip the runner
    # and directly compute hashes on mismatched data.
    with pg_conn.cursor() as cur:
        cur.executemany(
            f"INSERT INTO {dst_schema}.items VALUES (%s, %s, %s)",
            [(1, "a", 10), (2, "TAMPERED", 20), (3, "c", 30)],
        )

    spec = _spec(src_schema, dst_schema, "items")

    from src.migrate.verify import hash_table

    src_batches = _stream_batches(src_session, Dialect.POSTGRES, spec.source_table, spec.columns, spec.pk_columns, 10)
    dst_batches = _stream_batches(dst_session, Dialect.POSTGRES, spec.target_table, spec.columns, spec.pk_columns, 10)
    src_hash = hash_table(src_batches)
    dst_hash = hash_table(dst_batches)
    assert src_hash.row_count == dst_hash.row_count == 3
    assert not src_hash.matches(dst_hash)


# ─── Composite PK keyset walk ────────────────────────────────────────────────


def test_runner_handles_composite_pk(schemas, sessions):
    src_schema, dst_schema = schemas
    src_session, dst_session, pg_conn = sessions
    with pg_conn.cursor() as cur:
        cur.execute(
            f"""
            CREATE TABLE {src_schema}.line_items (
                order_id INTEGER, line_no INTEGER, sku TEXT,
                PRIMARY KEY (order_id, line_no)
            )
            """
        )
        cur.execute(
            f"""
            CREATE TABLE {dst_schema}.line_items (
                order_id INTEGER, line_no INTEGER, sku TEXT,
                PRIMARY KEY (order_id, line_no)
            )
            """
        )
        cur.executemany(
            f"INSERT INTO {src_schema}.line_items VALUES (%s, %s, %s)",
            [(1, 1, "A"), (1, 2, "B"), (2, 1, "C"), (2, 2, "D"), (3, 1, "E")],
        )

    spec = TableSpec(
        source_table=TableRef(schema=src_schema, name="line_items"),
        target_table=TableRef(schema=dst_schema, name="line_items"),
        columns=["order_id", "line_no", "sku"],
        pk_columns=["order_id", "line_no"],
    )
    plan = LoadPlan(groups=[LoadGroup(tables=[spec.target_table])])
    runner = Runner(
        source_session=src_session,
        target_session=dst_session,
        target_pg_conn=pg_conn,
        source_dialect=Dialect.POSTGRES,
        batch_size=2,  # forces multi-batch keyset walk
    )
    result = runner.execute(plan, {spec.target_table.qualified(): spec})
    table_res = result.tables[spec.target_table.qualified()]
    assert table_res.rows_copied == 5
    assert table_res.last_pk == (3, 1)
    assert table_res.verified


# ─── Checkpoint hook called once per batch ────────────────────────────────────


def test_runner_invokes_checkpoint_per_batch(schemas, sessions):
    src_schema, dst_schema = schemas
    src_session, dst_session, pg_conn = sessions
    rows = [(i, "x", i) for i in range(1, 11)]  # 10 rows
    _create_seeded_source(pg_conn, src_schema, "items", rows)
    _create_empty_target(pg_conn, dst_schema, "items")

    spec = _spec(src_schema, dst_schema, "items")
    plan = LoadPlan(groups=[LoadGroup(tables=[spec.target_table])])

    seen: list = []

    def record(table, last_pk, rows_so_far):
        seen.append((table.qualified(), last_pk, rows_so_far))

    runner = Runner(
        source_session=src_session,
        target_session=dst_session,
        target_pg_conn=pg_conn,
        source_dialect=Dialect.POSTGRES,
        batch_size=4,
        checkpoint=record,
    )
    runner.execute(plan, {spec.target_table.qualified(): spec})

    # 10 rows / 4 -> 3 batches: cumulative [4, 8, 10]
    assert [s[2] for s in seen] == [4, 8, 10]
    # Final batch's last_pk is the row with id=10.
    assert seen[-1][1] == (10,)
    # All three checkpoints reference the destination table.
    assert all(s[0] == spec.target_table.qualified() for s in seen)


# ─── Resume picks up after a prior checkpoint ────────────────────────────────


def test_runner_resumes_from_checkpointed_pk(schemas, sessions):
    """Simulate a prior run that loaded half the rows: the first 25
    rows are pre-populated on the target, and the runner is told (via
    the `resume` callback) that the last checkpointed PK was 25.
    The runner should keyset-skip past those rows and copy only 26-50,
    while the final Merkle verification still covers the full table."""
    src_schema, dst_schema = schemas
    src_session, dst_session, pg_conn = sessions
    rows = [(i, f"item-{i}", i * 10) for i in range(1, 51)]
    _create_seeded_source(pg_conn, src_schema, "items", rows)
    _create_empty_target(pg_conn, dst_schema, "items")
    # Pre-populate the target with rows 1..25 so the resumed run only
    # needs to copy 26..50.
    with pg_conn.cursor() as cur:
        cur.executemany(
            f"INSERT INTO {dst_schema}.items VALUES (%s, %s, %s)",
            rows[:25],
        )

    spec = _spec(src_schema, dst_schema, "items")
    plan = LoadPlan(groups=[LoadGroup(tables=[spec.target_table])])

    seen: list = []

    def record(table, last_pk, rows_so_far):
        seen.append((last_pk, rows_so_far))

    runner = Runner(
        source_session=src_session,
        target_session=dst_session,
        target_pg_conn=pg_conn,
        source_dialect=Dialect.POSTGRES,
        batch_size=10,
        checkpoint=record,
        resume=lambda table: (25,),
    )
    result = runner.execute(plan, {spec.target_table.qualified(): spec})

    target_result = result.tables[spec.target_table.qualified()]
    # Only 25 rows copied this run (26..50), but the full-table Merkle
    # pass should still match.
    assert target_result.rows_copied == 25
    assert target_result.last_pk == (50,)
    assert target_result.verified
    # Checkpoints only cover the resumed window.
    assert [pk for pk, _ in seen] == [(35,), (45,), (50,)]


# ─── DDL-generated target tables round-trip through the runner ───────────────


def test_runner_loads_into_ddl_generated_target(schemas, sessions, pg_url):
    """Introspect a seeded source, generate CREATE TABLE DDL for an
    empty target, run the DDL, then load via the Runner. This is the
    canonical Oracle-less rehearsal of the full greenfield flow:
    introspect → generate DDL → apply DDL → copy → verify."""
    src_schema, dst_schema = schemas
    src_session, dst_session, pg_conn = sessions

    # Seed the source with a mix of types that exercises the mapper.
    with pg_conn.cursor() as cur:
        cur.execute(
            f"""
            CREATE TABLE {src_schema}.orders (
                id INTEGER PRIMARY KEY,
                customer TEXT NOT NULL,
                total NUMERIC(10, 2),
                placed_at TIMESTAMP
            )
            """
        )
        cur.executemany(
            f"INSERT INTO {src_schema}.orders VALUES (%s, %s, %s, %s)",
            [
                (1, "alice", 12.50, "2026-04-01 10:00:00"),
                (2, "bob", 99.99, "2026-04-02 11:00:00"),
                (3, "carol", 7.25, "2026-04-03 12:00:00"),
            ],
        )

    # Introspect and generate DDL targeting the destination schema.
    schema = introspect(src_session, Dialect.POSTGRES, src_schema)
    specs = schema.build_specs(target_schema=dst_schema)
    assert specs, "introspect should find the seeded table"

    # Build a cols/pks map keyed by the target qualified name so the
    # generated CREATE TABLEs land in the destination schema.
    cols_by_target = {
        spec.target_table.qualified(): schema.column_metadata[spec.source_table.qualified()]
        for spec in specs.values()
    }
    pks_by_target = {
        spec.target_table.qualified(): spec.pk_columns for spec in specs.values()
    }
    stmts = generate_schema_ddl(
        [s.target_table for s in specs.values()],
        cols_by_target,
        pks_by_target,
        map_type=map_pg_type,
    )

    # Apply on a dedicated non-autocommit connection so a mid-batch
    # failure would roll back the whole thing.
    ddl_conn = psycopg.connect(pg_url)
    try:
        apply_ddl(ddl_conn, stmts)
    finally:
        ddl_conn.close()

    # Target table exists now — load data through the runner.
    plan = LoadPlan(groups=[LoadGroup(tables=[s.target_table for s in specs.values()])])
    runner = Runner(
        source_session=src_session,
        target_session=dst_session,
        target_pg_conn=pg_conn,
        source_dialect=Dialect.POSTGRES,
        batch_size=10,
    )
    result = runner.execute(plan, specs)

    target_qn = next(iter(specs)).split(".", 1)[0] + "." + "orders"
    target_result = result.tables[target_qn]
    assert target_result.rows_copied == 3
    assert target_result.verified


# ─── Sequence catch-up runs when target schema has owned sequences ───────────


def test_runner_catches_up_target_sequences(schemas, sessions):
    src_schema, dst_schema = schemas
    src_session, dst_session, pg_conn = sessions
    # Source: plain INTEGER column with explicit values.
    with pg_conn.cursor() as cur:
        cur.execute(f"CREATE TABLE {src_schema}.items (id INTEGER PRIMARY KEY, label TEXT)")
        cur.executemany(
            f"INSERT INTO {src_schema}.items VALUES (%s, %s)",
            [(100, "a"), (101, "b"), (102, "c")],
        )
        # Target: SERIAL — has an owned sequence that needs catch-up
        # after we copy explicit ids in.
        cur.execute(f"CREATE TABLE {dst_schema}.items (id SERIAL PRIMARY KEY, label TEXT)")

    spec = TableSpec(
        source_table=TableRef(schema=src_schema, name="items"),
        target_table=TableRef(schema=dst_schema, name="items"),
        columns=["id", "label"],
        pk_columns=["id"],
    )
    plan = LoadPlan(groups=[LoadGroup(tables=[spec.target_table])])
    runner = Runner(
        source_session=src_session,
        target_session=dst_session,
        target_pg_conn=pg_conn,
        source_dialect=Dialect.POSTGRES,
        batch_size=10,
    )
    result = runner.execute(plan, {spec.target_table.qualified(): spec})
    assert result.tables[spec.target_table.qualified()].verified
    assert len(result.sequences) == 1
    assert result.sequences[0].set_to == 102

    # Insert without specifying id — sequence should hand back 103.
    with pg_conn.cursor() as cur:
        cur.execute(f"INSERT INTO {dst_schema}.items (label) VALUES ('next') RETURNING id")
        (next_id,) = cur.fetchone()
    assert next_id == 103

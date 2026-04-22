"""CLI entry point for the data-movement runner.

  python -m src.migrate \
      --source 'oracle+oracledb://hr:hr@oracle:1521/?service_name=FREEPDB1' \
      --target 'postgresql+psycopg://depart_user:depart_secure_password@localhost:5432/depart' \
      --source-schema HR \
      --target-schema public \
      [--tables EMPLOYEES,DEPARTMENTS]   # restrict to a subset
      [--batch-size 5000]
      [--no-verify]                       # skip the Merkle hash pass

The CLI is deliberately thin — every primitive (introspection, plan,
runner, checkpoint adapter) is a library call. This file just parses
flags, opens connections, runs the loop, and prints a result table.
"""

from __future__ import annotations

import argparse
import sys
from typing import Optional

import psycopg
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from ..migration.checkpoint import CheckpointManager
from .checkpoint_adapter import make_checkpoint_callback, make_resume_callback
from .ddl import apply_ddl, generate_schema_ddl, map_oracle_type, map_pg_type
from .introspect import introspect
from .keyset import Dialect
from .planner import plan_load_order
from .runner import Runner


def main(argv: Optional[list[str]] = None) -> int:
    args = _parse_args(argv)

    src_dialect = Dialect.ORACLE if args.source.startswith("oracle") else Dialect.POSTGRES

    src_engine = create_engine(args.source)
    dst_engine = create_engine(args.target)
    SrcSession = sessionmaker(bind=src_engine)
    DstSession = sessionmaker(bind=dst_engine)
    src_session = SrcSession()
    dst_session = DstSession()

    # Raw psycopg conn for the binary COPY protocol.
    pg_url = args.target.replace("postgresql+psycopg://", "postgresql://")
    pg_conn = psycopg.connect(pg_url, autocommit=True)

    print(f"introspecting source schema {args.source_schema!r}...", file=sys.stderr)
    schema = introspect(src_session, src_dialect, args.source_schema)
    if args.tables:
        wanted = {t.strip() for t in args.tables.split(",")}
        schema.tables = [t for t in schema.tables if t.name in wanted]

    specs = schema.build_specs(target_schema=args.target_schema)
    if not specs:
        print(
            f"no migratable tables found in {args.source_schema!r} "
            f"(after PK/whitelist filtering)",
            file=sys.stderr,
        )
        return 1

    plan = plan_load_order(
        [s.target_table for s in specs.values()],
        # Rewrite FK refs to point at the target schema so the planner
        # operates in the destination namespace.
        [
            type(fk)(
                name=fk.name,
                from_table=_rewrite_schema(fk.from_table, args.source_schema, args.target_schema),
                to_table=_rewrite_schema(fk.to_table, args.source_schema, args.target_schema),
                deferrable=fk.deferrable,
            )
            for fk in schema.foreign_keys
        ],
    )
    print(f"plan: {len(plan.groups)} group(s), {len(plan.flat_tables())} table(s)", file=sys.stderr)

    if args.create_tables:
        _create_target_tables(pg_url, schema, specs, plan, src_dialect)

    # Checkpoint manager + adapter — all batches are persisted to the
    # migrations table so a crash can resume. If `--migration-id` is
    # supplied we reuse that row and pick up from the last checkpoint;
    # otherwise we start a fresh migration.
    manager = CheckpointManager(dst_session)
    if args.migration_id:
        migration_id = args.migration_id
        print(f"resuming migration_id: {migration_id}", file=sys.stderr)
    else:
        migration_id = manager.create_migration(
            f"{args.source_schema}->{args.target_schema}",
        )
        print(f"migration_id: {migration_id}", file=sys.stderr)
    callback = make_checkpoint_callback(manager, migration_id)
    resume = make_resume_callback(manager, migration_id)

    runner = Runner(
        source_session=src_session,
        target_session=dst_session,
        target_pg_conn=pg_conn,
        source_dialect=src_dialect,
        batch_size=args.batch_size,
        checkpoint=callback,
        resume=resume,
    )

    result = runner.execute(plan, specs)

    print()
    print(f"{'table':40s}  {'rows':>10s}  verified")
    print("-" * 65)
    for qn, tr in result.tables.items():
        verified = "✓" if tr.verified else "✗"
        print(f"{qn:40s}  {tr.rows_copied:>10d}  {verified}")
    print()
    print(f"sequences advanced: {len(result.sequences)}")
    print(f"all verified: {result.all_verified}")
    print(f"total rows: {result.total_rows}")

    src_session.close()
    dst_session.close()
    pg_conn.close()
    return 0 if result.all_verified else 2


def _parse_args(argv: Optional[list[str]]) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    p.add_argument("--source", required=True, help="SQLAlchemy URL for the source DB")
    p.add_argument("--target", required=True, help="SQLAlchemy URL for the target Postgres")
    p.add_argument(
        "--source-schema",
        required=True,
        help="Source schema/owner (e.g. HR for Oracle, public for Postgres)",
    )
    p.add_argument(
        "--target-schema",
        required=True,
        help="Destination schema in Postgres (typically public)",
    )
    p.add_argument(
        "--tables",
        default=None,
        help="Comma-separated table names to restrict to (default: every table with a PK)",
    )
    p.add_argument("--batch-size", type=int, default=5000, help="Rows per COPY batch (default 5000)")
    p.add_argument(
        "--migration-id",
        default=None,
        help="Resume an existing migration row by id instead of creating a new one. "
        "Per-table keyset walks pick up after the last checkpointed PK.",
    )
    p.add_argument(
        "--create-tables",
        action="store_true",
        help="Before loading, emit and run `CREATE TABLE IF NOT EXISTS` statements "
        "in the target schema based on the introspected source. Columns use "
        "best-effort type mappings; FKs are not emitted.",
    )
    return p.parse_args(argv)


def _rewrite_schema(ref, src_schema: str, dst_schema: str):
    """If `ref.schema` matches the source schema, swap it to the target.
    Used so the FK graph lives in the destination namespace."""
    from .planner import TableRef

    if ref.schema and ref.schema.upper() == src_schema.upper():
        return TableRef(schema=dst_schema, name=ref.name)
    return ref


def _create_target_tables(pg_url: str, schema, specs, plan, src_dialect) -> None:
    """Generate and execute `CREATE TABLE IF NOT EXISTS` statements in
    the target Postgres in load-plan order, so parents exist before
    children. Uses a dedicated non-autocommit psycopg connection so a
    mid-batch failure rolls the whole schema back."""
    map_type = map_oracle_type if src_dialect == Dialect.ORACLE else map_pg_type

    # Build columns-by-target-qualified-name and PKs-by-target-qualified-name
    # from the specs so the DDL is emitted in the destination namespace.
    cols_by_target: dict = {}
    pks_by_target: dict = {}
    for spec in specs.values():
        source_qn = spec.source_table.qualified()
        target_qn = spec.target_table.qualified()
        cols_by_target[target_qn] = schema.column_metadata[source_qn]
        pks_by_target[target_qn] = spec.pk_columns

    stmts = generate_schema_ddl(
        plan.flat_tables(),
        cols_by_target,
        pks_by_target,
        map_type=map_type,
    )
    print(f"creating {len(stmts)} target table(s)...", file=sys.stderr)
    ddl_conn = psycopg.connect(pg_url)  # non-autocommit
    try:
        apply_ddl(ddl_conn, stmts)
    finally:
        ddl_conn.close()


if __name__ == "__main__":
    sys.exit(main())

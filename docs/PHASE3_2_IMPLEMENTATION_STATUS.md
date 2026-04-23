# Phase 3.2 Implementation Status

**Status:** 🟡 Foundation rebuilt — data-movement core shipped, validation layers 6–7 still open
**Last Updated:** 2026-04-23

---

## Architecture (current)

The original Phase 3.2 design had a single `apps/api/src/migration/` package
with `orchestrator.py` and `validators.py`. Both were deleted during the
foundation pass and the data-movement work was rewritten under a new
`apps/api/src/migrate/` package built on COPY + keyset pagination + Merkle
hashing. `migration/` now contains only `checkpoint.py`, which `migrate/`
re-exports via `migrate/checkpoint.py`.

```
apps/api/src/migrate/
├── planner.py       Tarjan SCC + Kahn topo sort → FK-safe LoadPlan
├── keyset.py        Dialect-aware keyset pagination query builder
├── introspect.py    Source-side schema introspection (columns, PKs, FKs)
├── ddl.py           Target-side DDL emission
├── copy.py          Binary COPY writer to Postgres (raw psycopg)
├── runner.py        Wires planner + keyset + COPY + sequences + verify
├── sequences.py     Post-load sequence catch-up
├── verify.py        Merkle-hash batch verification (SHA-256)
├── checkpoint.py    Re-export shim → migration/checkpoint.py
├── checkpoint_adapter.py
└── __main__.py      CLI entry point
```

The split is intentional: `planner.py` is pure (no DB), `runner.py` is
the only stateful coordinator, and `verify.py` lets a failed verification
narrow the bad batch in O(log n) hashes instead of O(n) row diffs.

---

## What's shipped

### Data movement core ✅
- **FK-safe load order** via Tarjan SCC + Kahn — multi-table cycles get
  a `LoadGroup` wrapped in `SET CONSTRAINTS … DEFERRED`; self-referential
  FKs flagged for NULL-then-UPDATE pass
- **Keyset pagination** for both Oracle (`FETCH FIRST n ROWS ONLY`) and
  Postgres (`LIMIT n`) source reads
- **Binary COPY** to Postgres target via raw psycopg (SQLAlchemy doesn't
  expose the COPY protocol)
- **Sequence catch-up** runs after all tables are loaded
- **Per-batch checkpoints** (`{table, last_pk}`) — resume picks up exactly
  where the crash hit
- **PII masking** during migration (per-column redaction, separate service)
- **CLI**: `python -m apps.api.src.migrate ...`

### Verification ✅
- **Merkle-hash verification** — chained SHA-256 over canonical row
  serialization; whole-table hashes compared post-load
- Failed verification surfaces as a per-table `discrepancy` — does not
  roll back the load (data is already there); operator decides between
  bisecting, retrying the bad table, or accepting

### Background execution ✅
- Switched from Celery (originally planned) to **arq** (commit `97c4cae`)
- Cron-driven recurring migrations supported via `arq` worker
- Webhooks (signed delivery) fire on terminal states

### API surface ✅
Mounted at `/api/v1/migrations/` (not `/api/v3/migration/` as the original
draft claimed):

```
POST   /api/v1/migrations/test-connection
POST   /api/v1/migrations                          create
GET    /api/v1/migrations                          list
GET    /api/v1/migrations/{id}
POST   /api/v1/migrations/{id}/run                 background task
DELETE /api/v1/migrations/{id}
POST   /api/v1/migrations/{id}/plan                returns LoadPlan
GET    /api/v1/migrations/{id}/progress
```

### Test coverage ✅
Twelve test files under `apps/api/tests/`:
`test_migrate_planner`, `test_migrate_runner`, `test_migrate_copy`,
`test_migrate_keyset`, `test_migrate_verify`, `test_migrate_sequences`,
`test_migrate_ddl`, `test_migrate_introspect`,
`test_migrate_introspect_oracle_live`, `test_migrate_checkpoint_adapter`,
`test_migrate_cli`, `test_migration_checkpoint`.

### Phase 3.3 cockpit ✅
Permission analyzer, benchmark analyzer, connection pool, validation
framework, checkpoint recovery — all shipped. See
`PHASE_3_3_COMPLETION_SUMMARY.md`.

---

## Validation layers — current state

The seven-layer validation plan from `DATA_INTEGRITY_VALIDATION.md`:

| Layer | What | Status |
|-------|------|--------|
| 1 — Structural | Tables, columns, PKs, FKs exist | Covered by introspect + DDL emission |
| 2 — Volume | Row counts, NULL distribution | Covered by Merkle hash (stronger than counts) |
| 3 — Quality | Value ranges, distributions | ❌ not implemented |
| 4 — Logical | Orphan detection, UNIQUE intact | Partially: FK-safe load order prevents orphans by construction |
| 5 — Temporal | Timestamp precision, timezone | ❌ not implemented |
| 6 — Anomaly (Claude) | ML-based outlier detection | ❌ not implemented |
| 7 — Production monitor | Post-cutover drift detection | ❌ not implemented |

Layers 3, 5, 6, 7 are the explicit gaps. Layer 6 was originally stubbed
in `validators.py` (now deleted); the stub has not been rewritten in
`migrate/`.

---

## Promises vs reality

The original Phase 3.2 doc promised a Claude-driven migration planner
that returns optimal chunk sizes, worker count, table order, and index
recommendations. Reality:

| Promise | Status |
|---------|--------|
| Table load order | ✅ shipped — but deterministic (Tarjan SCC + Kahn), not Claude. Reproducible plans are arguably the right call here. |
| Chunk sizes | ❌ — `runner.batch_size = 5000` default; caller-overridable, no Claude advice |
| Worker count / parallelization | ❌ — single-threaded loop today |
| Index recommendations | ❌ — no analyzer wired |
| Real-time WS dashboard | ❌ — no WebSocket code anywhere; UI polls `/progress` |

There is **no `claude` / `anthropic` / `llm` import anywhere in
`apps/api/src/migrate/`**. If we want Claude in the planning loop, the
narrowest useful place to add it is a chunk-size + index advisor — see
the open work list below.

---

## Open work

In rough priority order:

1. **Claude chunk-size + index advisor** — accept introspection output
   (table sizes, PK type, FK fan-out, available RAM) and return
   recommended `batch_size` per table plus pre-load index suggestions.
   Plug into `Runner` as an optional pre-flight step.
2. **Layer 6 — anomaly detection** — feed sampled distributions from
   target tables to Claude, surface high-severity anomalies in the
   migration report. Off by default; opt-in per migration.
3. **Layer 7 — production monitor** — poll-based (no WS today): every
   N minutes, run a configurable critical-query suite against
   PostgreSQL, alert on threshold breach. Re-uses connection pool.
4. **Layer 3 + 5** — quality + temporal validators. Lower priority than
   6/7; Merkle hash already gives us bit-identical proof, so quality
   drift can only enter via masking transforms (which already log).
5. **Real-time dashboard** — decision: ship WebSocket or accept poll +
   document it. The UI already polls `/progress`; current latency is
   acceptable for batch migrations.
6. **Doc the architecture move** — done by this rewrite, but the
   integration strategy doc (`PHASE3_INTEGRATION_STRATEGY.md`) still
   references the deleted layout.

---

## Related files

| File | Purpose |
|------|---------|
| `PHASE3_INTEGRATION_STRATEGY.md` | Day-by-day workflow narrative — needs the same architecture refresh |
| `DATA_MIGRATION_ORCHESTRATION.md` | Original design doc |
| `DATA_INTEGRITY_VALIDATION.md` | Seven-layer validation spec |
| `PHASE_3_3_COMPLETION_SUMMARY.md` | HITL cockpit (shipped) |
| `apps/api/src/migrate/` | Live data-movement package |
| `apps/api/src/routers/migrations.py` | `/api/v1/migrations/*` endpoints |
| `apps/api/src/services/migration_runner.py` | Background runner glue |

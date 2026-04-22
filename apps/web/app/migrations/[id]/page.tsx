'use client';

/**
 * /migrations/[id] — migration detail + live progress.
 *
 * Post-polish pass:
 *   * Duration (live counter while active, final value after done)
 *   * Context-aware action button: "Run" / "Resume" / "Re-run"
 *   * Clearer completed_with_warnings — per-table discrepancy list
 *   * Admin-only Delete for terminal-state migrations
 *   * Optimistic refresh after Run/Delete so status flips instantly
 *
 * Polls /progress every 2 seconds while the migration is active
 * (pending/queued/in_progress). Stops polling once terminal state is
 * reached.
 */

import { useCallback, useEffect, useRef, useState } from 'react';
import { useParams, useRouter } from 'next/navigation';
import Link from 'next/link';

import SelfHostedGuard from '@/app/components/SelfHostedGuard';
import {
  CheckpointSummary,
  MigrationDetail,
  MigrationPlan,
  deleteMigration,
  getMigration,
  pollMigrationProgress,
  previewMigrationPlan,
  runMigration,
} from '@/app/lib/api';
import { useAuthStore } from '@/app/store/authStore';

import { StatusBadge } from '../StatusBadge';


const TERMINAL_STATES = new Set([
  'completed',
  'completed_with_warnings',
  'failed',
]);


export default function MigrationDetailPage() {
  return (
    <SelfHostedGuard>
      <DetailContent />
    </SelfHostedGuard>
  );
}


function DetailContent() {
  const { id } = useParams<{ id: string }>();
  const router = useRouter();
  const { user } = useAuthStore();
  const isAdmin = !user || user.role === 'admin';
  const canRun = !user || user.role === 'admin' || user.role === 'operator';

  const [migration, setMigration] = useState<MigrationDetail | null>(null);
  const [error, setError] = useState('');
  const [starting, setStarting] = useState(false);
  const [deleting, setDeleting] = useState(false);
  const [plan, setPlan] = useState<MigrationPlan | null>(null);
  const [planLoading, setPlanLoading] = useState(false);
  const [planError, setPlanError] = useState('');

  async function preview() {
    setPlanLoading(true);
    setPlanError('');
    setPlan(null);
    try {
      setPlan(await previewMigrationPlan(id));
    } catch (e: any) {
      setPlanError(
        e?.response?.data?.detail || e?.message || 'Plan preview failed.',
      );
    } finally {
      setPlanLoading(false);
    }
  }

  const refresh = useCallback(async () => {
    setError('');
    try {
      setMigration(await getMigration(id));
    } catch (e: any) {
      setError(e?.response?.data?.detail || e?.message || 'Failed to load.');
    }
  }, [id]);

  useEffect(() => {
    void refresh();
  }, [refresh]);

  // Poll while active. Stops on terminal state.
  const timerRef = useRef<ReturnType<typeof setInterval> | null>(null);
  useEffect(() => {
    if (!migration) return;
    const active = !TERMINAL_STATES.has(migration.status);
    if (!active) {
      if (timerRef.current) clearInterval(timerRef.current);
      timerRef.current = null;
      return;
    }
    timerRef.current = setInterval(async () => {
      try {
        const next = await pollMigrationProgress(id);
        setMigration(next);
      } catch {
        /* transient failure — keep polling */
      }
    }, 2000);
    return () => {
      if (timerRef.current) clearInterval(timerRef.current);
      timerRef.current = null;
    };
  }, [migration, id]);

  async function startRun() {
    setStarting(true);
    setError('');
    try {
      await runMigration(id);
      await refresh();
    } catch (e: any) {
      setError(
        e?.response?.data?.detail || e?.message || 'Could not start migration.',
      );
    } finally {
      setStarting(false);
    }
  }

  async function remove() {
    if (!migration) return;
    if (
      !confirm(
        `Delete migration "${migration.name || migration.id}"? This removes ` +
          `its config and per-table checkpoint rows. Data already copied to ` +
          `the target stays untouched.`,
      )
    ) {
      return;
    }
    setDeleting(true);
    setError('');
    try {
      await deleteMigration(id);
      router.push('/migrations');
    } catch (e: any) {
      setError(e?.response?.data?.detail || e?.message || 'Delete failed.');
      setDeleting(false);
    }
  }

  if (!migration && !error) {
    return (
      <main className="min-h-screen bg-gray-50">
        <div className="container mx-auto max-w-5xl px-4 py-12 text-sm text-gray-500">
          Loading migration…
        </div>
      </main>
    );
  }
  if (!migration) {
    return (
      <main className="min-h-screen bg-gray-50">
        <div className="container mx-auto max-w-5xl px-4 py-12">
          <div className="rounded-lg border border-red-200 bg-red-50 p-4 text-sm text-red-700">
            {error}
          </div>
          <Link
            href="/migrations"
            className="mt-4 inline-block text-purple-700 hover:underline"
          >
            ← Back to migrations
          </Link>
        </div>
      </main>
    );
  }

  const active = !TERMINAL_STATES.has(migration.status);
  const runLabel = buttonLabelFor(migration);
  const canDelete = isAdmin && !active;
  const warningTables = migration.checkpoints.filter(
    (c) => c.status !== 'completed' && c.status !== 'in_progress',
  );
  const warnings =
    migration.status === 'completed_with_warnings' ||
    (migration.error_message?.includes('verification') ?? false);

  return (
    <main className="min-h-screen bg-gray-50">
      <div className="container mx-auto max-w-5xl px-4 py-12">
        <Link
          href="/migrations"
          className="mb-6 inline-block text-sm text-purple-700 hover:underline"
        >
          ← Back to migrations
        </Link>

        <div className="flex items-start justify-between gap-4">
          <div>
            <h1 className="text-3xl font-bold text-gray-900">
              {migration.name || '(unnamed)'}
            </h1>
            <p className="mt-2 font-mono text-sm text-gray-600">
              {migration.source_schema}{' '}
              <span className="text-gray-400">→</span> {migration.target_schema}
            </p>
          </div>
          <div className="flex flex-col items-end gap-2">
            <div className="flex items-center gap-3">
              <StatusBadge status={migration.status} />
              {active && (
                <span className="text-xs text-gray-500">polling every 2s…</span>
              )}
            </div>
            <div className="flex items-center gap-2">
              <button
                onClick={preview}
                disabled={planLoading}
                className="rounded-md border border-purple-300 bg-white px-4 py-2 text-sm font-semibold text-purple-700 shadow-sm hover:bg-purple-50 disabled:opacity-50"
                title="Introspect + plan without executing. Safe."
              >
                {planLoading ? 'Planning…' : 'Preview plan'}
              </button>
              <button
                onClick={startRun}
                disabled={starting || !canRun || active}
                title={
                  active
                    ? 'Already running — wait for completion.'
                    : !canRun
                    ? 'Viewer role — ask an admin to run.'
                    : ''
                }
                className="rounded-md bg-purple-600 px-5 py-2 font-semibold text-white shadow-sm transition hover:bg-purple-700 disabled:bg-gray-300"
              >
                {starting ? 'Starting…' : runLabel}
              </button>
              {canDelete && (
                <button
                  onClick={remove}
                  disabled={deleting}
                  className="rounded-md border border-red-200 bg-white px-4 py-2 text-sm font-semibold text-red-600 hover:bg-red-50 disabled:opacity-50"
                >
                  {deleting ? 'Deleting…' : 'Delete'}
                </button>
              )}
            </div>
          </div>
        </div>

        {error && (
          <div className="mt-6 rounded-lg border border-red-200 bg-red-50 p-4 text-sm text-red-700">
            {error}
          </div>
        )}

        <SummaryStrip migration={migration} />

        {(plan || planError) && (
          <PlanPanel plan={plan} error={planError} onDismiss={() => { setPlan(null); setPlanError(''); }} />
        )}

        {warnings && warningTables.length > 0 && (
          <WarningsPanel migration={migration} tables={warningTables} />
        )}

        {migration.error_message && (
          <div className="mt-6 rounded-lg border border-amber-200 bg-amber-50 p-4 text-sm">
            <div className="font-semibold text-amber-900">Runtime message</div>
            <pre className="mt-2 whitespace-pre-wrap font-mono text-xs text-amber-900">
{migration.error_message}
            </pre>
          </div>
        )}

        <ConfigPanel migration={migration} />
        <CheckpointsPanel checkpoints={migration.checkpoints} />
      </div>
    </main>
  );
}


/* ─── Summary strip ────────────────────────────────────────────────────── */

function SummaryStrip({ migration }: { migration: MigrationDetail }) {
  // Live tick so "elapsed" updates while active.
  const [, forceTick] = useState(0);
  useEffect(() => {
    if (!migration.started_at || migration.completed_at) return;
    const t = setInterval(() => forceTick((n) => n + 1), 1000);
    return () => clearInterval(t);
  }, [migration.started_at, migration.completed_at]);

  const duration = computeDuration(migration.started_at, migration.completed_at);

  return (
    <section className="mt-8 grid grid-cols-2 gap-4 md:grid-cols-4">
      <Stat
        label="Rows transferred"
        value={migration.rows_transferred.toLocaleString()}
      />
      <Stat label="Duration" value={duration ?? '—'} />
      <Stat
        label="Tables"
        value={String(
          migration.tables?.length ?? migration.checkpoints.length ?? 0,
        )}
      />
      <Stat
        label="Batch size"
        value={(migration.batch_size ?? 5000).toLocaleString()}
      />
    </section>
  );
}


function Stat({ label, value }: { label: string; value: string }) {
  return (
    <div className="rounded-lg border border-gray-200 bg-white p-4 shadow-sm">
      <div className="text-xs font-medium uppercase tracking-wide text-gray-500">
        {label}
      </div>
      <div className="mt-1 text-lg font-semibold text-gray-900">{value}</div>
    </div>
  );
}


function computeDuration(
  started: string | null,
  completed: string | null,
): string | null {
  if (!started) return null;
  const start = new Date(started).getTime();
  const end = completed ? new Date(completed).getTime() : Date.now();
  const seconds = Math.max(0, Math.floor((end - start) / 1000));

  if (seconds < 60) return `${seconds}s`;
  if (seconds < 3600) {
    const m = Math.floor(seconds / 60);
    const s = seconds % 60;
    return `${m}m ${s}s`;
  }
  const h = Math.floor(seconds / 3600);
  const m = Math.floor((seconds % 3600) / 60);
  return `${h}h ${m}m`;
}


function buttonLabelFor(m: MigrationDetail): string {
  if (m.status === 'pending') return 'Run migration';
  if (m.status === 'failed') return 'Retry from last checkpoint';
  if (m.status === 'completed') return 'Re-run from scratch';
  if (m.status === 'completed_with_warnings') return 'Re-run (review warnings)';
  if (m.status === 'in_progress' || m.status === 'queued') return 'Running…';
  return 'Run';
}


/* ─── Warnings panel ──────────────────────────────────────────────────── */

function WarningsPanel({
  migration,
  tables,
}: {
  migration: MigrationDetail;
  tables: CheckpointSummary[];
}) {
  return (
    <section className="mt-8 rounded-xl border border-orange-200 bg-orange-50 p-6 shadow-sm">
      <h2 className="text-lg font-semibold text-orange-900">
        Verification warnings
      </h2>
      <p className="mt-2 text-sm text-orange-800">
        The data copied — every row is physically in the target — but one or
        more per-table Merkle checks didn&apos;t match. Usually this is a
        transient-data issue (source still writing during the run) or a type
        quirk in the introspected schema. Review each entry below.
      </p>
      <ul className="mt-4 space-y-2">
        {tables.map((t) => (
          <li
            key={t.table_name}
            className="rounded-md border border-orange-200 bg-white p-3 text-sm"
          >
            <div className="flex items-baseline justify-between">
              <span className="font-mono text-gray-900">{t.table_name}</span>
              <span className="text-xs text-orange-700">{t.status}</span>
            </div>
            {t.error_message && (
              <p className="mt-1 font-mono text-xs text-orange-900">
                {t.error_message}
              </p>
            )}
          </li>
        ))}
      </ul>
      {migration.error_message && (
        <p className="mt-4 text-xs text-orange-800">
          Full runner output is shown below in the runtime-message block.
        </p>
      )}
    </section>
  );
}


/* ─── Config (read-only) ───────────────────────────────────────────────── */

function ConfigPanel({ migration }: { migration: MigrationDetail }) {
  const kv = [
    ['Source URL', maskDsn(migration.source_url)],
    ['Target URL', maskDsn(migration.target_url)],
    ['Source schema', migration.source_schema || '—'],
    ['Target schema', migration.target_schema || '—'],
    ['Tables', migration.tables?.join(', ') || 'all with PK'],
    ['Create tables', migration.create_tables ? 'yes' : 'no'],
    [
      'Started',
      migration.started_at
        ? new Date(migration.started_at).toLocaleString()
        : '—',
    ],
    [
      'Completed',
      migration.completed_at
        ? new Date(migration.completed_at).toLocaleString()
        : '—',
    ],
  ] as const;

  return (
    <section className="mt-8 rounded-xl border border-gray-200 bg-white p-6 shadow-sm">
      <h2 className="mb-4 text-lg font-semibold text-gray-900">Configuration</h2>
      <dl className="grid grid-cols-1 gap-x-8 gap-y-3 md:grid-cols-2">
        {kv.map(([k, v]) => (
          <div key={k} className="flex items-start justify-between gap-4">
            <dt className="text-xs font-medium uppercase tracking-wide text-gray-500">
              {k}
            </dt>
            <dd className="max-w-[60%] break-words text-right font-mono text-xs text-gray-900">
              {v}
            </dd>
          </div>
        ))}
      </dl>
    </section>
  );
}


function maskDsn(dsn: string | null): string {
  if (!dsn) return '—';
  return dsn.replace(/(:\/\/[^:]+:)([^@]+)(@)/, '$1•••$3');
}


/* ─── Per-table checkpoints + progress bars ────────────────────────────── */

function CheckpointsPanel({ checkpoints }: { checkpoints: CheckpointSummary[] }) {
  return (
    <section className="mt-8 rounded-xl border border-gray-200 bg-white p-6 shadow-sm">
      <h2 className="mb-4 text-lg font-semibold text-gray-900">
        Per-table progress
      </h2>
      {checkpoints.length === 0 ? (
        <p className="py-6 text-center text-sm text-gray-500">
          No batches recorded yet. Progress appears here once the runner writes
          its first checkpoint.
        </p>
      ) : (
        <ul className="space-y-3">
          {checkpoints.map((c) => (
            <li key={c.table_name} className="rounded-md border border-gray-100 p-3">
              <div className="flex items-baseline justify-between">
                <span className="font-mono text-sm text-gray-900">
                  {c.table_name}
                </span>
                <span className="text-xs text-gray-500">
                  {c.rows_processed.toLocaleString()} rows
                  {c.total_rows > 0 && ` / ${c.total_rows.toLocaleString()}`} ·{' '}
                  {c.status}
                  {c.updated_at && (
                    <>
                      {' · '}
                      <span title={new Date(c.updated_at).toLocaleString()}>
                        {relativeTime(c.updated_at)}
                      </span>
                    </>
                  )}
                </span>
              </div>
              <div className="mt-2 h-2 w-full overflow-hidden rounded-full bg-gray-100">
                <div
                  className={`h-full transition-all ${
                    c.status === 'completed'
                      ? 'bg-green-500'
                      : c.status === 'failed'
                      ? 'bg-red-500'
                      : 'bg-purple-500'
                  }`}
                  style={{
                    width: `${Math.min(
                      100,
                      Math.max(0, c.progress_percentage || 0),
                    )}%`,
                  }}
                />
              </div>
              {c.error_message && (
                <p className="mt-2 text-xs text-red-600">{c.error_message}</p>
              )}
            </li>
          ))}
        </ul>
      )}
    </section>
  );
}


function relativeTime(iso: string): string {
  const diff = Math.floor((Date.now() - new Date(iso).getTime()) / 1000);
  if (diff < 5) return 'just now';
  if (diff < 60) return `${diff}s ago`;
  if (diff < 3600) return `${Math.floor(diff / 60)}m ago`;
  if (diff < 86400) return `${Math.floor(diff / 3600)}h ago`;
  return `${Math.floor(diff / 86400)}d ago`;
}


/* ─── Plan preview panel ────────────────────────────────────────────────── */

function PlanPanel({
  plan,
  error,
  onDismiss,
}: {
  plan: MigrationPlan | null;
  error: string;
  onDismiss: () => void;
}) {
  if (error) {
    return (
      <section className="mt-8 rounded-xl border border-red-200 bg-red-50 p-5">
        <div className="flex items-start justify-between gap-4">
          <div>
            <h2 className="text-lg font-semibold text-red-900">
              Plan preview failed
            </h2>
            <p className="mt-1 text-sm text-red-800">{error}</p>
          </div>
          <button
            onClick={onDismiss}
            className="text-xs text-red-700 hover:underline"
          >
            Dismiss
          </button>
        </div>
      </section>
    );
  }
  if (!plan) return null;

  return (
    <section className="mt-8 rounded-xl border border-purple-200 bg-purple-50/40 p-6 shadow-sm">
      <div className="flex items-start justify-between">
        <div>
          <h2 className="text-lg font-semibold text-gray-900">Dry-run plan</h2>
          <p className="mt-1 text-sm text-gray-600">
            What would happen if you hit Run right now. Nothing has been
            executed.
          </p>
        </div>
        <button
          onClick={onDismiss}
          className="text-xs text-gray-500 hover:text-gray-700 hover:underline"
        >
          Hide
        </button>
      </div>

      <div className="mt-4 grid grid-cols-1 gap-4 md:grid-cols-3">
        <Summary
          label="Will load"
          value={plan.tables_with_pk.length}
          hint="tables with a primary key"
        />
        <Summary
          label="Will skip"
          value={plan.tables_skipped.length}
          hint="tables without a primary key"
          muted={plan.tables_skipped.length === 0}
        />
        <Summary
          label="Deferred FKs"
          value={plan.deferred_constraints.length}
          hint="constraints held until commit"
          muted={plan.deferred_constraints.length === 0}
        />
      </div>

      {plan.load_order.length > 0 && (
        <div className="mt-6">
          <div className="text-sm font-semibold text-gray-900">Load order</div>
          <p className="mt-1 text-xs text-gray-500">
            Parents first so FK references resolve. Cycles use deferred
            constraints per the list above.
          </p>
          <ol className="mt-2 list-decimal space-y-0.5 pl-5 font-mono text-xs text-gray-800">
            {plan.load_order.map((t) => (
              <li key={t}>{t}</li>
            ))}
          </ol>
        </div>
      )}

      {plan.tables_skipped.length > 0 && (
        <div className="mt-6 rounded-md border border-amber-200 bg-amber-50 p-3 text-sm">
          <div className="font-semibold text-amber-900">Skipped tables</div>
          <p className="mt-0.5 text-xs text-amber-800">
            These have no primary key, so the keyset runner can&apos;t migrate
            them. Add a PK in the source, or exclude them from the migration.
          </p>
          <ul className="mt-2 space-y-0.5 font-mono text-xs text-amber-900">
            {plan.tables_skipped.map((t) => (
              <li key={t}>· {t}</li>
            ))}
          </ul>
        </div>
      )}

      {plan.create_table_ddl.length > 0 && (
        <div className="mt-6">
          <div className="text-sm font-semibold text-gray-900">
            CREATE TABLE statements ({plan.create_table_ddl.length})
          </div>
          <p className="mt-1 text-xs text-gray-500">
            These will run against the target before data load. Copy them and
            diff against your expectations before clicking Run.
          </p>
          <pre className="mt-2 max-h-[40vh] overflow-auto rounded-md bg-gray-900 p-4 font-mono text-xs text-green-200">
{plan.create_table_ddl.join('\n\n')}
          </pre>
        </div>
      )}

      {plan.type_mappings.length > 0 && (
        <details className="mt-6">
          <summary className="cursor-pointer text-sm font-semibold text-gray-900">
            Column type mappings ({plan.type_mappings.length})
          </summary>
          <div className="mt-2 max-h-[40vh] overflow-auto rounded-md border border-gray-200 bg-white">
            <table className="w-full text-xs">
              <thead className="sticky top-0 bg-gray-50 text-left">
                <tr>
                  <th className="px-3 py-2 font-semibold">Table</th>
                  <th className="px-3 py-2 font-semibold">Column</th>
                  <th className="px-3 py-2 font-semibold">Source type</th>
                  <th className="px-3 py-2 font-semibold">Postgres type</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-gray-100 font-mono">
                {plan.type_mappings.map((m, i) => (
                  <tr key={`${m.table}-${m.column}-${i}`}>
                    <td className="px-3 py-1.5">{m.table}</td>
                    <td className="px-3 py-1.5">{m.column}</td>
                    <td className="px-3 py-1.5">{m.source_type}</td>
                    <td className="px-3 py-1.5">{m.pg_type}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </details>
      )}
    </section>
  );
}


function Summary({
  label,
  value,
  hint,
  muted,
}: {
  label: string;
  value: number;
  hint: string;
  muted?: boolean;
}) {
  return (
    <div
      className={`rounded-md border bg-white p-3 shadow-sm ${
        muted ? 'border-gray-100 text-gray-400' : 'border-purple-100 text-gray-900'
      }`}
    >
      <div className="text-xs font-medium uppercase tracking-wide text-gray-500">
        {label}
      </div>
      <div className="mt-1 text-2xl font-bold">{value}</div>
      <div className="mt-0.5 text-xs text-gray-500">{hint}</div>
    </div>
  );
}

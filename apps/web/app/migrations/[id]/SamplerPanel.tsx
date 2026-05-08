'use client';

import { useState } from 'react';
import {
  SampleMismatchItem,
  SampleResponse,
  SampleResultItem,
  listSampleResults,
  runSample,
} from '@/app/lib/api';
import { useAuthStore } from '@/app/store/authStore';


type RunState =
  | { tag: 'idle' }
  | { tag: 'running' }
  | { tag: 'ok'; data: SampleResponse }
  | { tag: 'error'; msg: string };

const STATUS_CLASSES: Record<string, string> = {
  clean:             'bg-green-100 text-green-800',
  mismatches_found:  'bg-red-100 text-red-800',
};

const MISMATCH_LABELS: Record<string, string> = {
  value_mismatch:  'Value',
  missing_in_pg:   'Missing in PG',
  null_mismatch:   'NULL mismatch',
};

function StatusBadge({ status }: { status: string }) {
  const cls = STATUS_CLASSES[status] ?? 'bg-gray-100 text-gray-700';
  const label = status === 'clean' ? 'clean' : 'mismatches found';
  return (
    <span className={`inline-block rounded-full px-2 py-0.5 text-xs font-medium ${cls}`}>
      {label}
    </span>
  );
}


export default function SamplerPanel({ migrationId }: { migrationId: string }) {
  const { user } = useAuthStore();
  const canRun = user?.role === 'admin' || user?.role === 'operator';

  const [sampleSize, setSampleSize] = useState(100);
  const [runState, setRunState] = useState<RunState>({ tag: 'idle' });
  const [history, setHistory] = useState<SampleResultItem[] | null>(null);
  const [historyLoading, setHistoryLoading] = useState(false);

  async function run() {
    setRunState({ tag: 'running' });
    try {
      const data = await runSample(migrationId, sampleSize);
      setRunState({ tag: 'ok', data });
      setHistory(null);
    } catch (e: any) {
      setRunState({
        tag: 'error',
        msg: e?.response?.data?.detail || e?.message || 'Sampler failed.',
      });
    }
  }

  async function loadHistory() {
    if (history !== null) {
      setHistory(null);
      return;
    }
    setHistoryLoading(true);
    try {
      const items = await listSampleResults(migrationId);
      setHistory(items);
    } catch {
      setHistory([]);
    } finally {
      setHistoryLoading(false);
    }
  }

  return (
    <section className="mt-6 rounded-xl border border-gray-200 bg-white p-5">
      <div className="flex items-center justify-between">
        <div>
          <h2 className="text-lg font-semibold text-gray-900">Data Sampler</h2>
          <p className="mt-0.5 text-xs text-gray-500">
            Row-level Oracle → PostgreSQL comparison before cutover
          </p>
        </div>
        <div className="flex items-center gap-2">
          <button
            onClick={loadHistory}
            disabled={historyLoading}
            className="rounded-lg border border-gray-200 px-3 py-1.5 text-xs font-medium text-gray-600 hover:bg-gray-50 disabled:opacity-50"
          >
            {historyLoading ? 'Loading…' : history !== null ? 'Hide history' : 'History'}
          </button>
          {canRun && (
            <>
              <select
                value={sampleSize}
                onChange={(e) => setSampleSize(Number(e.target.value))}
                disabled={runState.tag === 'running'}
                className="rounded-lg border border-gray-200 px-2 py-1.5 text-xs text-gray-600 disabled:opacity-50"
              >
                <option value={50}>50 rows</option>
                <option value={100}>100 rows</option>
                <option value={500}>500 rows</option>
                <option value={1000}>1 000 rows</option>
              </select>
              <RunButton state={runState} onRun={run} />
            </>
          )}
        </div>
      </div>

      {runState.tag === 'idle' && (
        <p className="mt-3 text-sm text-gray-600">
          Samples rows from Oracle by primary key and compares them against
          PostgreSQL. Detects value corruption, missing rows, and NULL conversion
          errors before cutover. Tables without a primary key are skipped.
        </p>
      )}

      {runState.tag === 'running' && (
        <p className="mt-3 text-sm text-gray-500">Sampling Oracle rows and comparing…</p>
      )}

      {runState.tag === 'error' && (
        <div className="mt-3 rounded-lg border border-red-200 bg-red-50 p-3 text-sm text-red-700">
          {runState.msg}
        </div>
      )}

      {runState.tag === 'ok' && (
        <SampleResults data={runState.data} />
      )}

      {history !== null && (
        <HistoryList items={history} />
      )}
    </section>
  );
}


function RunButton({ state, onRun }: { state: RunState; onRun: () => void }) {
  const busy = state.tag === 'running';
  const label =
    state.tag === 'idle'    ? 'Run sampler' :
    state.tag === 'running' ? 'Sampling…' :
    'Re-run';
  return (
    <button
      onClick={onRun}
      disabled={busy}
      className="rounded-lg bg-orange-600 px-3 py-1.5 text-xs font-medium text-white hover:bg-orange-700 disabled:opacity-50"
    >
      {label}
    </button>
  );
}


function SampleResults({ data }: { data: SampleResponse }) {
  const { overall_status, tables_sampled, tables_skipped, mismatch_count, mismatches } = data;
  return (
    <div className="mt-3 space-y-3">
      <div className="flex flex-wrap items-center gap-3 text-sm">
        <div className="flex items-center gap-1.5">
          <span className="text-gray-500">Result:</span>
          <StatusBadge status={overall_status} />
        </div>
        <span className="text-gray-400">·</span>
        <span className="text-gray-500">
          {tables_sampled} table{tables_sampled !== 1 ? 's' : ''} sampled
        </span>
        {tables_skipped > 0 && (
          <>
            <span className="text-gray-400">·</span>
            <span className="text-gray-400">
              {tables_skipped} skipped (no PK)
            </span>
          </>
        )}
        {mismatch_count > 0 && (
          <>
            <span className="text-gray-400">·</span>
            <span className="text-red-600 font-medium">
              {mismatch_count} mismatch{mismatch_count !== 1 ? 'es' : ''}
            </span>
          </>
        )}
      </div>

      {mismatches.length === 0 ? (
        <p className="text-sm text-gray-600">
          All sampled rows match — no value corruption or missing rows detected.
        </p>
      ) : (
        <MismatchTable mismatches={mismatches} />
      )}
    </div>
  );
}


function MismatchTable({ mismatches }: { mismatches: SampleMismatchItem[] }) {
  return (
    <div className="overflow-x-auto rounded-lg border border-gray-200">
      <table className="min-w-full divide-y divide-gray-200 text-sm">
        <thead className="bg-gray-50">
          <tr>
            <th className="px-3 py-2 text-left text-xs font-medium uppercase tracking-wide text-gray-500">Type</th>
            <th className="px-3 py-2 text-left text-xs font-medium uppercase tracking-wide text-gray-500">Table</th>
            <th className="px-3 py-2 text-left text-xs font-medium uppercase tracking-wide text-gray-500">Column</th>
            <th className="px-3 py-2 text-left text-xs font-medium uppercase tracking-wide text-gray-500">Oracle</th>
            <th className="px-3 py-2 text-left text-xs font-medium uppercase tracking-wide text-gray-500">PG</th>
          </tr>
        </thead>
        <tbody className="divide-y divide-gray-100 bg-white">
          {mismatches.map((m, i) => (
            <MismatchRow key={i} mismatch={m} />
          ))}
        </tbody>
      </table>
    </div>
  );
}


function MismatchRow({ mismatch }: { mismatch: SampleMismatchItem }) {
  const [expanded, setExpanded] = useState(false);
  const pkStr = Object.entries(mismatch.pk_values)
    .map(([k, v]) => `${k}=${v}`)
    .join(', ');

  return (
    <>
      <tr
        className="cursor-pointer hover:bg-gray-50"
        onClick={() => setExpanded((v) => !v)}
      >
        <td className="px-3 py-2 text-xs text-gray-600">
          {MISMATCH_LABELS[mismatch.mismatch_type] ?? mismatch.mismatch_type}
        </td>
        <td className="px-3 py-2 font-mono text-xs text-gray-800">{mismatch.table}</td>
        <td className="px-3 py-2 font-mono text-xs text-gray-700">{mismatch.column}</td>
        <td className="px-3 py-2 font-mono text-xs text-gray-600 max-w-[180px] truncate">
          {mismatch.oracle_value ?? <span className="text-gray-400 italic">NULL</span>}
        </td>
        <td className="px-3 py-2 font-mono text-xs text-gray-600 max-w-[180px] truncate">
          {mismatch.pg_value ?? <span className="text-gray-400 italic">NULL</span>}
        </td>
      </tr>
      {expanded && (
        <tr className="bg-orange-50">
          <td colSpan={5} className="px-4 py-2 text-xs text-gray-600">
            <span className="font-medium">PK: </span>{pkStr}
          </td>
        </tr>
      )}
    </>
  );
}


function HistoryList({ items }: { items: SampleResultItem[] }) {
  if (items.length === 0) {
    return (
      <div className="mt-4 rounded-lg border border-gray-100 bg-gray-50 p-3 text-sm text-gray-500">
        No previous sample runs found.
      </div>
    );
  }

  return (
    <div className="mt-4 space-y-2">
      <p className="text-xs font-medium uppercase tracking-wide text-gray-400">Run history</p>
      {items.map((item) => (
        <div
          key={item.result_id}
          className="flex items-center gap-3 rounded-lg border border-gray-200 bg-gray-50 px-3 py-2 text-sm"
        >
          <StatusBadge status={item.overall_status} />
          <span className="text-gray-600">
            {new Date(item.created_at).toLocaleString()}
          </span>
          <span className="text-xs text-gray-400">
            · {item.tables_sampled} tables · {item.sample_size} rows/table
            {item.mismatch_count > 0 && (
              <span className="ml-1 text-red-600 font-medium">
                · {item.mismatch_count} mismatches
              </span>
            )}
          </span>
        </div>
      ))}
    </div>
  );
}

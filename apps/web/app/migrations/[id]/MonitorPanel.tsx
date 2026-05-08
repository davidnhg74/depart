'use client';

import { useState } from 'react';
import {
  MonitorFindingItem,
  MonitorResponse,
  MonitorSnapshotItem,
  listMonitorSnapshots,
  runMonitor,
} from '@/app/lib/api';
import { useAuthStore } from '@/app/store/authStore';


type RunState =
  | { tag: 'idle' }
  | { tag: 'running' }
  | { tag: 'ok'; data: MonitorResponse }
  | { tag: 'error'; msg: string };

const SEVERITY_CLASSES: Record<string, string> = {
  clean:   'bg-green-100 text-green-800',
  info:    'bg-blue-100 text-blue-800',
  warning: 'bg-amber-100 text-amber-800',
  error:   'bg-red-100 text-red-800',
};

const CHECK_LABELS: Record<string, string> = {
  row_drift:         'Row drift',
  dead_tuple_bloat:  'Bloat',
  cdc_lag:           'CDC lag',
};

function SeverityBadge({ severity }: { severity: string }) {
  const cls = SEVERITY_CLASSES[severity] ?? 'bg-gray-100 text-gray-700';
  return (
    <span className={`inline-block rounded-full px-2 py-0.5 text-xs font-medium ${cls}`}>
      {severity}
    </span>
  );
}


export default function MonitorPanel({ migrationId }: { migrationId: string }) {
  const { user } = useAuthStore();
  const canRun = user?.role === 'admin' || user?.role === 'operator';

  const [runState, setRunState] = useState<RunState>({ tag: 'idle' });
  const [history, setHistory] = useState<MonitorSnapshotItem[] | null>(null);
  const [historyLoading, setHistoryLoading] = useState(false);

  async function run() {
    setRunState({ tag: 'running' });
    try {
      const data = await runMonitor(migrationId);
      setRunState({ tag: 'ok', data });
      setHistory(null); // invalidate cache so next history load is fresh
    } catch (e: any) {
      setRunState({
        tag: 'error',
        msg: e?.response?.data?.detail || e?.message || 'Monitor check failed.',
      });
    }
  }

  async function loadHistory() {
    if (history !== null) {
      setHistory(null); // toggle off
      return;
    }
    setHistoryLoading(true);
    try {
      const snaps = await listMonitorSnapshots(migrationId);
      setHistory(snaps);
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
          <h2 className="text-lg font-semibold text-gray-900">Production Monitor</h2>
          <p className="mt-0.5 text-xs text-gray-500">
            Row drift · dead-tuple bloat · CDC replication lag
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
            <RunButton state={runState} onRun={run} />
          )}
        </div>
      </div>

      {runState.tag === 'idle' && (
        <p className="mt-3 text-sm text-gray-600">
          Snapshot the target database health: row-count drift vs the previous
          check, dead-tuple bloat, and CDC replication lag.
        </p>
      )}

      {runState.tag === 'running' && (
        <p className="mt-3 text-sm text-gray-500">Collecting metrics…</p>
      )}

      {runState.tag === 'error' && (
        <div className="mt-3 rounded-lg border border-red-200 bg-red-50 p-3 text-sm text-red-700">
          {runState.msg}
        </div>
      )}

      {runState.tag === 'ok' && (
        <MonitorResults data={runState.data} />
      )}

      {history !== null && (
        <HistoryList snapshots={history} />
      )}
    </section>
  );
}


function RunButton({ state, onRun }: { state: RunState; onRun: () => void }) {
  const busy = state.tag === 'running';
  const label =
    state.tag === 'idle'    ? 'Run check' :
    state.tag === 'running' ? 'Checking…' :
    'Re-run';

  return (
    <button
      onClick={onRun}
      disabled={busy}
      className="rounded-lg bg-teal-600 px-3 py-1.5 text-xs font-medium text-white hover:bg-teal-700 disabled:opacity-50"
    >
      {label}
    </button>
  );
}


function MonitorResults({ data }: { data: MonitorResponse }) {
  const { overall_severity, findings, tables_checked } = data;
  return (
    <div className="mt-3 space-y-3">
      <div className="flex flex-wrap items-center gap-3 text-sm">
        <div className="flex items-center gap-1.5">
          <span className="text-gray-500">Result:</span>
          <SeverityBadge severity={overall_severity} />
        </div>
        <span className="text-gray-400">·</span>
        <span className="text-gray-500">
          {tables_checked} table{tables_checked !== 1 ? 's' : ''} checked
        </span>
      </div>

      {findings.length === 0 ? (
        <p className="text-sm text-gray-600">
          No issues detected — row counts, bloat, and replication lag look normal.
        </p>
      ) : (
        <FindingsTable findings={findings} />
      )}
    </div>
  );
}


function FindingsTable({ findings }: { findings: MonitorFindingItem[] }) {
  return (
    <div className="overflow-x-auto rounded-lg border border-gray-200">
      <table className="min-w-full divide-y divide-gray-200 text-sm">
        <thead className="bg-gray-50">
          <tr>
            <th className="px-3 py-2 text-left text-xs font-medium uppercase tracking-wide text-gray-500">Severity</th>
            <th className="px-3 py-2 text-left text-xs font-medium uppercase tracking-wide text-gray-500">Check</th>
            <th className="px-3 py-2 text-left text-xs font-medium uppercase tracking-wide text-gray-500">Table</th>
            <th className="px-3 py-2 text-left text-xs font-medium uppercase tracking-wide text-gray-500">Finding</th>
          </tr>
        </thead>
        <tbody className="divide-y divide-gray-100 bg-white">
          {findings.map((f, i) => (
            <FindingRow key={i} finding={f} />
          ))}
        </tbody>
      </table>
    </div>
  );
}


function FindingRow({ finding }: { finding: MonitorFindingItem }) {
  const [expanded, setExpanded] = useState(false);
  return (
    <>
      <tr
        className="cursor-pointer hover:bg-gray-50"
        onClick={() => setExpanded((v) => !v)}
      >
        <td className="px-3 py-2">
          <SeverityBadge severity={finding.severity} />
        </td>
        <td className="px-3 py-2 text-xs text-gray-600">
          {CHECK_LABELS[finding.check_name] ?? finding.check_name}
        </td>
        <td className="px-3 py-2 font-mono text-xs text-gray-800">{finding.table ?? '—'}</td>
        <td className="px-3 py-2 text-xs text-gray-700">{finding.message}</td>
      </tr>
      {expanded && (
        <tr className="bg-teal-50">
          <td colSpan={4} className="px-4 py-2 text-xs text-gray-700">
            <span className="font-medium">Recommended action: </span>
            {finding.recommended_action}
          </td>
        </tr>
      )}
    </>
  );
}


function HistoryList({ snapshots }: { snapshots: MonitorSnapshotItem[] }) {
  if (snapshots.length === 0) {
    return (
      <div className="mt-4 rounded-lg border border-gray-100 bg-gray-50 p-3 text-sm text-gray-500">
        No previous snapshots found.
      </div>
    );
  }

  return (
    <div className="mt-4 space-y-2">
      <p className="text-xs font-medium uppercase tracking-wide text-gray-400">
        Snapshot history
      </p>
      {snapshots.map((snap) => (
        <SnapshotRow key={snap.snapshot_id} snap={snap} />
      ))}
    </div>
  );
}


function SnapshotRow({ snap }: { snap: MonitorSnapshotItem }) {
  const [expanded, setExpanded] = useState(false);
  const ts = new Date(snap.created_at).toLocaleString();

  return (
    <div className="rounded-lg border border-gray-200 bg-gray-50">
      <button
        onClick={() => setExpanded((v) => !v)}
        className="flex w-full items-center justify-between px-3 py-2 text-left text-sm"
      >
        <div className="flex items-center gap-2">
          <SeverityBadge severity={snap.overall_severity} />
          <span className="text-gray-600">{ts}</span>
          <span className="text-xs text-gray-400">
            · {snap.tables_checked} table{snap.tables_checked !== 1 ? 's' : ''}
            {snap.findings.length > 0 && ` · ${snap.findings.length} finding${snap.findings.length !== 1 ? 's' : ''}`}
          </span>
        </div>
        <span className="text-gray-400">{expanded ? '▲' : '▼'}</span>
      </button>
      {expanded && snap.findings.length > 0 && (
        <div className="border-t border-gray-200 px-3 pb-3 pt-2">
          <FindingsTable findings={snap.findings} />
        </div>
      )}
      {expanded && snap.findings.length === 0 && (
        <p className="border-t border-gray-200 px-3 py-2 text-xs text-gray-500">
          No findings — all checks passed.
        </p>
      )}
    </div>
  );
}

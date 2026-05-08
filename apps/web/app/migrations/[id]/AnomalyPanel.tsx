'use client';

import { useState } from 'react';
import { AnomalyCheckResponse, AnomalyFindingItem, checkAnomalies } from '@/app/lib/api';
import { useAuthStore } from '@/app/store/authStore';


type LoadState =
  | { tag: 'idle' }
  | { tag: 'checking' }
  | { tag: 'ok'; data: AnomalyCheckResponse }
  | { tag: 'error'; msg: string };

const TERMINAL = new Set(['completed', 'completed_with_warnings']);

const SEVERITY_CLASSES: Record<string, string> = {
  clean:   'bg-green-100 text-green-800',
  info:    'bg-blue-100 text-blue-800',
  warning: 'bg-amber-100 text-amber-800',
  error:   'bg-red-100 text-red-800',
};

const ANOMALY_LABELS: Record<string, string> = {
  null_rate_spike:      'High NULLs',
  cardinality_mismatch: 'Cardinality',
  range_violation:      'Range',
  distribution_skew:    'Distribution',
  unexpected_empty_table: 'Empty table',
  row_count_mismatch:   'Row count',
  other:                'Other',
};

function SeverityBadge({ severity }: { severity: string }) {
  const cls = SEVERITY_CLASSES[severity] ?? 'bg-gray-100 text-gray-700';
  return (
    <span className={`inline-block rounded-full px-2 py-0.5 text-xs font-medium ${cls}`}>
      {severity}
    </span>
  );
}


export default function AnomalyPanel({
  migrationId,
  status,
}: {
  migrationId: string;
  status: string;
}) {
  const { user } = useAuthStore();
  const canRun = user?.role === 'admin' || user?.role === 'operator';
  const isTerminal = TERMINAL.has(status);

  const [state, setState] = useState<LoadState>({ tag: 'idle' });

  if (!isTerminal || !canRun) return null;

  async function run() {
    setState({ tag: 'checking' });
    try {
      const data = await checkAnomalies(migrationId);
      setState({ tag: 'ok', data });
    } catch (e: any) {
      setState({
        tag: 'error',
        msg: e?.response?.data?.detail || e?.message || 'Anomaly check failed.',
      });
    }
  }

  return (
    <section className="mt-6 rounded-xl border border-gray-200 bg-white p-5">
      <div className="flex items-center justify-between">
        <div>
          <h2 className="text-lg font-semibold text-gray-900">Anomaly Check</h2>
          <p className="mt-0.5 text-xs text-gray-500">
            AI-powered post-migration data quality analysis
          </p>
        </div>
        <RunButton state={state} onRun={run} />
      </div>

      {state.tag === 'idle' && (
        <p className="mt-3 text-sm text-gray-600">
          Sample the target database distributions and use Claude to surface
          unexpected NULLs, cardinality collapses, or range violations before
          cutover.
        </p>
      )}

      {state.tag === 'checking' && (
        <p className="mt-3 text-sm text-gray-500">Sampling target database…</p>
      )}

      {state.tag === 'error' && (
        <div className="mt-3 rounded-lg border border-red-200 bg-red-50 p-3 text-sm text-red-700">
          {state.msg}
        </div>
      )}

      {state.tag === 'ok' && (
        <AnomalyResults data={state.data} />
      )}
    </section>
  );
}


function RunButton({ state, onRun }: { state: LoadState; onRun: () => void }) {
  const busy = state.tag === 'checking';
  const label =
    state.tag === 'idle' ? 'Run check' :
    state.tag === 'checking' ? 'Checking…' :
    'Re-run';

  return (
    <button
      onClick={onRun}
      disabled={busy}
      className="rounded-lg bg-purple-600 px-3 py-1.5 text-xs font-medium text-white hover:bg-purple-700 disabled:opacity-50"
    >
      {label}
    </button>
  );
}


function AnomalyResults({ data }: { data: AnomalyCheckResponse }) {
  const { overall_severity, findings, used_ai, tables_sampled } = data;

  return (
    <div className="mt-3 space-y-3">
      <div className="flex flex-wrap items-center gap-3 text-sm">
        <div className="flex items-center gap-1.5">
          <span className="text-gray-500">Result:</span>
          <SeverityBadge severity={overall_severity} />
        </div>
        <span className="text-gray-400">·</span>
        <span className="text-gray-500">{tables_sampled} table{tables_sampled !== 1 ? 's' : ''} sampled</span>
        <span className="text-gray-400">·</span>
        <span className="text-gray-500">{used_ai ? 'AI-powered' : 'Rule-based (no API key)'}</span>
      </div>

      {findings.length === 0 ? (
        <p className="text-sm text-gray-600">
          No anomalies detected — data distributions look normal.
        </p>
      ) : (
        <FindingsTable findings={findings} />
      )}
    </div>
  );
}


function FindingsTable({ findings }: { findings: AnomalyFindingItem[] }) {
  return (
    <div className="overflow-x-auto rounded-lg border border-gray-200">
      <table className="min-w-full divide-y divide-gray-200 text-sm">
        <thead className="bg-gray-50">
          <tr>
            <th className="px-3 py-2 text-left text-xs font-medium uppercase tracking-wide text-gray-500">Severity</th>
            <th className="px-3 py-2 text-left text-xs font-medium uppercase tracking-wide text-gray-500">Table</th>
            <th className="px-3 py-2 text-left text-xs font-medium uppercase tracking-wide text-gray-500">Column</th>
            <th className="px-3 py-2 text-left text-xs font-medium uppercase tracking-wide text-gray-500">Type</th>
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


function FindingRow({ finding }: { finding: AnomalyFindingItem }) {
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
        <td className="px-3 py-2 font-mono text-xs text-gray-800">{finding.table}</td>
        <td className="px-3 py-2 font-mono text-xs text-gray-500">{finding.column ?? '—'}</td>
        <td className="px-3 py-2 text-xs text-gray-600">
          {ANOMALY_LABELS[finding.anomaly_type] ?? finding.anomaly_type}
        </td>
        <td className="px-3 py-2 text-xs text-gray-700">{finding.message}</td>
      </tr>
      {expanded && (
        <tr className="bg-amber-50">
          <td colSpan={5} className="px-4 py-2 text-xs text-gray-700">
            <span className="font-medium">Recommended action: </span>
            {finding.recommended_action}
          </td>
        </tr>
      )}
    </>
  );
}

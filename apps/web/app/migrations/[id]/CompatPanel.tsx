'use client';

import { useState } from 'react';
import {
  CompatFindingItem,
  CompatScanItem,
  CompatScanResponse,
  listCompatScans,
  runCompatScan,
} from '@/app/lib/api';
import { useAuthStore } from '@/app/store/authStore';

type RunState =
  | { tag: 'idle' }
  | { tag: 'running' }
  | { tag: 'ok'; data: CompatScanResponse }
  | { tag: 'error'; msg: string };

const SEVERITY_CLASSES: Record<string, string> = {
  blocking: 'text-red-700 bg-red-50 border-red-200',
  advisory:  'text-amber-700 bg-amber-50 border-amber-200',
  info:      'text-blue-700 bg-blue-50 border-blue-200',
};

const SEVERITY_ICON: Record<string, string> = {
  blocking: '✗',
  advisory: '⚠',
  info:     'ℹ',
};

const SEVERITY_LABEL: Record<string, string> = {
  blocking: 'Blocking',
  advisory: 'Advisory',
  info:     'Info',
};

function ScoreMeter({ score }: { score: number }) {
  const color =
    score >= 80 ? 'bg-green-500' :
    score >= 50 ? 'bg-amber-400' :
                  'bg-red-500';
  return (
    <div className="flex items-center gap-2">
      <div className="h-2 w-24 rounded-full bg-gray-200 overflow-hidden">
        <div
          className={`h-full rounded-full transition-all ${color}`}
          style={{ width: `${score}%` }}
        />
      </div>
      <span className="text-sm font-semibold tabular-nums text-gray-700">{score}/100</span>
    </div>
  );
}

function FindingRow({ finding }: { finding: CompatFindingItem }) {
  const [expanded, setExpanded] = useState(false);
  const cls = SEVERITY_CLASSES[finding.severity] ?? SEVERITY_CLASSES.info;
  const icon = SEVERITY_ICON[finding.severity] ?? 'ℹ';
  const label = SEVERITY_LABEL[finding.severity] ?? finding.severity;

  return (
    <div
      className={`rounded-lg border px-3 py-2.5 ${cls} cursor-pointer`}
      onClick={() => setExpanded((v) => !v)}
    >
      <div className="flex items-start gap-2">
        <span className="mt-0.5 font-bold text-sm shrink-0">{icon}</span>
        <div className="flex-1 min-w-0">
          <div className="flex items-center justify-between gap-2 flex-wrap">
            <span className="text-xs font-bold uppercase tracking-wide font-mono">
              {finding.construct.replace(/_/g, ' ')}
            </span>
            <div className="flex items-center gap-2 shrink-0">
              <span className="text-xs opacity-70">{finding.count} occurrence{finding.count !== 1 ? 's' : ''}</span>
              <span className="text-xs font-medium capitalize opacity-75">{label}</span>
            </div>
          </div>
          <p className="mt-0.5 text-xs opacity-80">{finding.pg_equivalent}</p>
          {expanded && finding.locations.length > 0 && (
            <div className="mt-1.5 space-y-0.5">
              {finding.locations.map((loc) => (
                <span
                  key={loc}
                  className="mr-1.5 inline-block rounded bg-white/50 px-1.5 py-0.5 text-xs font-mono opacity-80"
                >
                  {loc}
                </span>
              ))}
            </div>
          )}
        </div>
      </div>
    </div>
  );
}

function ScanResult({ data }: { data: CompatScanResponse }) {
  const hasFindings = data.findings.length > 0;
  return (
    <div className="mt-3 space-y-3">
      <div className="flex items-center gap-4 flex-wrap">
        <div>
          <p className="text-xs font-medium uppercase tracking-wide text-gray-400 mb-1">Compatibility score</p>
          <ScoreMeter score={data.complexity_score} />
        </div>
        <div className="flex gap-3 text-xs flex-wrap">
          <span className="text-gray-500">{data.oracle_objects_scanned} objects scanned</span>
          {data.blocking_count > 0 && (
            <span className="text-red-600 font-medium">{data.blocking_count} blocking</span>
          )}
          {data.advisory_count > 0 && (
            <span className="text-amber-600 font-medium">{data.advisory_count} advisory</span>
          )}
          {data.info_count > 0 && (
            <span className="text-blue-500">{data.info_count} info</span>
          )}
        </div>
      </div>

      {data.complexity_score === 100 ? (
        <div className="mt-3 rounded-xl border border-green-300 bg-green-50 px-4 py-3 flex items-center gap-3">
          <span className="text-2xl">✓</span>
          <div>
            <p className="font-semibold text-green-800">Fully compatible</p>
            <p className="text-xs text-green-700 mt-0.5">
              No Oracle-specific SQL constructs found — application layer requires no changes.
            </p>
          </div>
        </div>
      ) : data.blocking_count > 0 ? (
        <div className="mt-3 rounded-xl border border-red-300 bg-red-50 px-4 py-3 flex items-center gap-3">
          <span className="text-2xl">✗</span>
          <div>
            <p className="font-semibold text-red-800">Application changes required</p>
            <p className="text-xs text-red-700 mt-0.5">
              {data.blocking_count} construct{data.blocking_count !== 1 ? 's' : ''} must be rewritten before cutover
            </p>
          </div>
        </div>
      ) : (
        <div className="mt-3 rounded-xl border border-amber-300 bg-amber-50 px-4 py-3 flex items-center gap-3">
          <span className="text-2xl">⚠</span>
          <div>
            <p className="font-semibold text-amber-800">Minor changes advisable</p>
            <p className="text-xs text-amber-700 mt-0.5">
              No blockers, but {data.advisory_count} construct{data.advisory_count !== 1 ? 's' : ''} should be reviewed
            </p>
          </div>
        </div>
      )}

      {hasFindings && (
        <div className="space-y-2 pt-1">
          <p className="text-xs text-gray-400">Click any finding to see affected objects</p>
          {data.findings.map((f) => (
            <FindingRow key={f.construct} finding={f} />
          ))}
        </div>
      )}
    </div>
  );
}

function HistoryList({ items }: { items: CompatScanItem[] }) {
  if (items.length === 0) {
    return (
      <div className="mt-4 rounded-lg border border-gray-100 bg-gray-50 p-3 text-sm text-gray-500">
        No previous compatibility scans.
      </div>
    );
  }
  return (
    <div className="mt-4 space-y-2">
      <p className="text-xs font-medium uppercase tracking-wide text-gray-400">Scan history</p>
      {items.map((item) => (
        <div
          key={item.snapshot_id}
          className="flex items-center gap-3 rounded-lg border border-gray-200 bg-gray-50 px-3 py-2 text-sm flex-wrap"
        >
          <span className={
            item.complexity_score === 100 ? 'text-green-600 font-bold' :
            item.blocking_count > 0 ? 'text-red-600 font-bold' :
            'text-amber-600 font-bold'
          }>
            {item.complexity_score === 100 ? '✓' : item.blocking_count > 0 ? '✗' : '⚠'}
          </span>
          <span className="text-gray-600">{new Date(item.created_at).toLocaleString()}</span>
          <span className="text-xs text-gray-400">
            · score {item.complexity_score}
            · {item.oracle_objects_scanned} objects
            {item.blocking_count > 0 && (
              <span className="ml-1 text-red-500 font-medium">· {item.blocking_count} blocking</span>
            )}
          </span>
        </div>
      ))}
    </div>
  );
}

export default function CompatPanel({ migrationId }: { migrationId: string }) {
  const { user } = useAuthStore();
  const canRun = user?.role === 'admin' || user?.role === 'operator';

  const [runState, setRunState] = useState<RunState>({ tag: 'idle' });
  const [history, setHistory] = useState<CompatScanItem[] | null>(null);
  const [historyLoading, setHistoryLoading] = useState(false);

  async function run() {
    setRunState({ tag: 'running' });
    try {
      const data = await runCompatScan(migrationId);
      setRunState({ tag: 'ok', data });
      setHistory(null);
    } catch (e: any) {
      setRunState({
        tag: 'error',
        msg: e?.response?.data?.detail || e?.message || 'Compatibility scan failed.',
      });
    }
  }

  async function loadHistory() {
    if (history !== null) { setHistory(null); return; }
    setHistoryLoading(true);
    try {
      setHistory(await listCompatScans(migrationId));
    } catch {
      setHistory([]);
    } finally {
      setHistoryLoading(false);
    }
  }

  return (
    <section className="mt-6 rounded-xl border border-gray-200 bg-white p-5">
      <div className="flex items-center justify-between flex-wrap gap-2">
        <div>
          <h2 className="text-lg font-semibold text-gray-900">Application SQL Compatibility</h2>
          <p className="mt-0.5 text-xs text-gray-500">
            Scans Oracle views, procedures, functions, and packages for constructs that require
            changes when migrating to PostgreSQL
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
            <button
              onClick={run}
              disabled={runState.tag === 'running'}
              className="rounded-lg bg-violet-600 px-3 py-1.5 text-xs font-medium text-white hover:bg-violet-700 disabled:opacity-50"
            >
              {runState.tag === 'running' ? 'Scanning…' :
               runState.tag === 'idle'    ? 'Scan compatibility' :
               'Re-scan'}
            </button>
          )}
        </div>
      </div>

      {runState.tag === 'idle' && (
        <p className="mt-3 text-sm text-gray-600">
          Analyzes Oracle-specific SQL constructs in your source schema — ROWNUM, CONNECT BY,
          NVL, DECODE, SYSDATE, outer-join (+) syntax, PL/SQL packages, and more. Produces a
          0–100 compatibility score and a remediation checklist for each construct found.
          Safe to run before data movement — reads Oracle system views only.
        </p>
      )}

      {runState.tag === 'running' && (
        <p className="mt-3 text-sm text-gray-500">Scanning Oracle schema for compatibility issues…</p>
      )}

      {runState.tag === 'error' && (
        <div className="mt-3 rounded-lg border border-red-200 bg-red-50 p-3 text-sm text-red-700">
          {runState.msg}
        </div>
      )}

      {runState.tag === 'ok' && <ScanResult data={runState.data} />}

      {history !== null && <HistoryList items={history} />}
    </section>
  );
}

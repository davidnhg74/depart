'use client';

import { useState } from 'react';
import {
  CutoverReadinessItem,
  CutoverReadinessResponse,
  ReadinessSignal,
  listCutoverReadiness,
  runCutoverReadiness,
} from '@/app/lib/api';
import { useAuthStore } from '@/app/store/authStore';

type RunState =
  | { tag: 'idle' }
  | { tag: 'running' }
  | { tag: 'ok'; data: CutoverReadinessResponse }
  | { tag: 'error'; msg: string };

const STATUS_ICON: Record<string, string> = {
  ok:       '✓',
  advisory: '⚠',
  blocking: '✗',
  not_run:  '–',
};

const STATUS_CLASSES: Record<string, string> = {
  ok:       'text-green-700 bg-green-50 border-green-200',
  advisory: 'text-amber-700 bg-amber-50 border-amber-200',
  blocking: 'text-red-700 bg-red-50 border-red-200',
  not_run:  'text-gray-500 bg-gray-50 border-gray-200',
};

const ICON_CLASSES: Record<string, string> = {
  ok:       'text-green-600',
  advisory: 'text-amber-500',
  blocking: 'text-red-600',
  not_run:  'text-gray-400',
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
      <span className="text-sm font-semibold tabular-nums text-gray-700">{score}</span>
    </div>
  );
}

function VerdictBanner({ data }: { data: CutoverReadinessResponse }) {
  const { ready_to_cut, blocking_count, advisory_count, not_run_count } = data;
  if (ready_to_cut) {
    return (
      <div className="mt-4 rounded-xl border border-green-300 bg-green-50 px-4 py-3 flex items-center gap-3">
        <span className="text-2xl">✓</span>
        <div>
          <p className="font-semibold text-green-800">Ready to cut over</p>
          {advisory_count > 0 && (
            <p className="text-xs text-green-700 mt-0.5">
              {advisory_count} advisory item{advisory_count !== 1 ? 's' : ''} — review before proceeding
            </p>
          )}
          {not_run_count > 0 && (
            <p className="text-xs text-green-600 mt-0.5">
              {not_run_count} check{not_run_count !== 1 ? 's' : ''} not yet run — accepted risk
            </p>
          )}
        </div>
      </div>
    );
  }
  return (
    <div className="mt-4 rounded-xl border border-red-300 bg-red-50 px-4 py-3 flex items-center gap-3">
      <span className="text-2xl">✗</span>
      <div>
        <p className="font-semibold text-red-800">Not ready — cutover blocked</p>
        <p className="text-xs text-red-700 mt-0.5">
          {blocking_count} blocking issue{blocking_count !== 1 ? 's' : ''} must be resolved before cutting over
        </p>
      </div>
    </div>
  );
}

function SignalRow({ signal }: { signal: ReadinessSignal }) {
  const [expanded, setExpanded] = useState(false);
  const cls = STATUS_CLASSES[signal.status] ?? STATUS_CLASSES.not_run;
  const iconCls = ICON_CLASSES[signal.status] ?? ICON_CLASSES.not_run;
  const icon = STATUS_ICON[signal.status] ?? '–';

  return (
    <div
      className={`rounded-lg border px-3 py-2.5 ${cls} ${signal.detail ? 'cursor-pointer' : ''}`}
      onClick={() => signal.detail && setExpanded((v) => !v)}
    >
      <div className="flex items-start gap-2">
        <span className={`mt-0.5 font-bold text-sm shrink-0 ${iconCls}`}>{icon}</span>
        <div className="flex-1 min-w-0">
          <div className="flex items-center justify-between gap-2">
            <span className="text-xs font-semibold uppercase tracking-wide opacity-60">
              {signal.label}
            </span>
            <span className="text-xs font-medium capitalize opacity-75 shrink-0">
              {signal.status.replace('_', ' ')}
            </span>
          </div>
          <p className="mt-0.5 text-sm">{signal.summary}</p>
          {expanded && signal.detail && (
            <p className="mt-1 text-xs opacity-75">{signal.detail}</p>
          )}
        </div>
      </div>
    </div>
  );
}

function ReadinessResult({ data }: { data: CutoverReadinessResponse }) {
  return (
    <div className="mt-3 space-y-3">
      <div className="flex items-center gap-4">
        <div>
          <p className="text-xs font-medium uppercase tracking-wide text-gray-400 mb-1">Readiness score</p>
          <ScoreMeter score={data.score} />
        </div>
        <div className="flex gap-3 text-xs">
          {data.blocking_count > 0 && (
            <span className="text-red-600 font-medium">{data.blocking_count} blocking</span>
          )}
          {data.advisory_count > 0 && (
            <span className="text-amber-600 font-medium">{data.advisory_count} advisory</span>
          )}
          {data.not_run_count > 0 && (
            <span className="text-gray-400">{data.not_run_count} not run</span>
          )}
        </div>
      </div>

      <VerdictBanner data={data} />

      <div className="space-y-2 pt-1">
        {data.signals.map((s) => (
          <SignalRow key={s.layer} signal={s} />
        ))}
      </div>
    </div>
  );
}

function HistoryList({ items }: { items: CutoverReadinessItem[] }) {
  if (items.length === 0) {
    return (
      <div className="mt-4 rounded-lg border border-gray-100 bg-gray-50 p-3 text-sm text-gray-500">
        No previous readiness assessments.
      </div>
    );
  }
  return (
    <div className="mt-4 space-y-2">
      <p className="text-xs font-medium uppercase tracking-wide text-gray-400">Assessment history</p>
      {items.map((item) => (
        <div
          key={item.snapshot_id}
          className="flex items-center gap-3 rounded-lg border border-gray-200 bg-gray-50 px-3 py-2 text-sm"
        >
          <span className={item.ready_to_cut ? 'text-green-600 font-bold' : 'text-red-600 font-bold'}>
            {item.ready_to_cut ? '✓' : '✗'}
          </span>
          <span className="text-gray-600">{new Date(item.created_at).toLocaleString()}</span>
          <span className="text-xs text-gray-400">
            · score {item.score}
            {item.blocking_count > 0 && (
              <span className="ml-1 text-red-500 font-medium">· {item.blocking_count} blocking</span>
            )}
          </span>
        </div>
      ))}
    </div>
  );
}

export default function CutoverPanel({ migrationId }: { migrationId: string }) {
  const { user } = useAuthStore();
  const canRun = user?.role === 'admin' || user?.role === 'operator';

  const [runState, setRunState] = useState<RunState>({ tag: 'idle' });
  const [history, setHistory] = useState<CutoverReadinessItem[] | null>(null);
  const [historyLoading, setHistoryLoading] = useState(false);

  async function run() {
    setRunState({ tag: 'running' });
    try {
      const data = await runCutoverReadiness(migrationId);
      setRunState({ tag: 'ok', data });
      setHistory(null);
    } catch (e: any) {
      setRunState({
        tag: 'error',
        msg: e?.response?.data?.detail || e?.message || 'Readiness check failed.',
      });
    }
  }

  async function loadHistory() {
    if (history !== null) { setHistory(null); return; }
    setHistoryLoading(true);
    try {
      setHistory(await listCutoverReadiness(migrationId));
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
          <h2 className="text-lg font-semibold text-gray-900">Cutover Readiness</h2>
          <p className="mt-0.5 text-xs text-gray-500">
            Go / No-Go verdict before switching production traffic to PostgreSQL
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
              className="rounded-lg bg-indigo-600 px-3 py-1.5 text-xs font-medium text-white hover:bg-indigo-700 disabled:opacity-50"
            >
              {runState.tag === 'running' ? 'Checking…' :
               runState.tag === 'idle'    ? 'Check readiness' :
               'Re-check'}
            </button>
          )}
        </div>
      </div>

      {runState.tag === 'idle' && (
        <p className="mt-3 text-sm text-gray-600">
          Aggregates Layer 6 (AI anomaly), Layer 7 (production monitor), and Layer 8 (row sampler)
          results together with migration status and CDC lag to produce a go/no-go verdict.
          Run all preceding checks first for the most accurate assessment.
        </p>
      )}

      {runState.tag === 'running' && (
        <p className="mt-3 text-sm text-gray-500">Evaluating readiness signals…</p>
      )}

      {runState.tag === 'error' && (
        <div className="mt-3 rounded-lg border border-red-200 bg-red-50 p-3 text-sm text-red-700">
          {runState.msg}
        </div>
      )}

      {runState.tag === 'ok' && <ReadinessResult data={runState.data} />}

      {history !== null && <HistoryList items={history} />}
    </section>
  );
}

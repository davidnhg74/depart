'use client';

import { useState } from 'react';
import {
  CodeConversionResponse,
  CodeConversionRunItem,
  ConversionObjectResult,
  getCodeConversionRun,
  listCodeConversionRuns,
  runCodeConversion,
} from '@/app/lib/api';
import { useAuthStore } from '@/app/store/authStore';

type RunState =
  | { tag: 'idle' }
  | { tag: 'running' }
  | { tag: 'ok'; data: CodeConversionResponse }
  | { tag: 'error'; msg: string };

const CONFIDENCE_CLASSES: Record<string, string> = {
  high:   'text-green-700 bg-green-50 border-green-200',
  medium: 'text-amber-700 bg-amber-50 border-amber-200',
  low:    'text-red-700 bg-red-50 border-red-200',
};

const CONFIDENCE_DOT: Record<string, string> = {
  high:   'bg-green-500',
  medium: 'bg-amber-400',
  low:    'bg-red-500',
};

function ObjectCard({ obj }: { obj: ConversionObjectResult }) {
  const [tab, setTab] = useState<'converted' | 'original'>('converted');
  const [open, setOpen] = useState(false);
  const cls = CONFIDENCE_CLASSES[obj.confidence] ?? CONFIDENCE_CLASSES.low;
  const dot = CONFIDENCE_DOT[obj.confidence] ?? CONFIDENCE_DOT.low;

  return (
    <div className={`rounded-lg border ${cls} overflow-hidden`}>
      <button
        className="w-full px-3 py-2.5 text-left flex items-center gap-2"
        onClick={() => setOpen((v) => !v)}
      >
        <span className={`w-2 h-2 rounded-full shrink-0 ${dot}`} />
        <span className="flex-1 font-mono text-sm font-semibold">
          {obj.object_type}: {obj.object_name}
        </span>
        {obj.error ? (
          <span className="text-xs font-medium text-red-600 shrink-0">Error</span>
        ) : (
          <span className="text-xs font-medium capitalize opacity-75 shrink-0">
            {obj.confidence} confidence
          </span>
        )}
        <span className="text-xs opacity-50 shrink-0">{open ? '▲' : '▼'}</span>
      </button>

      {open && (
        <div className="border-t border-current/20 bg-white/60">
          {obj.error ? (
            <div className="p-3 text-sm text-red-700 font-mono">{obj.error}</div>
          ) : (
            <>
              {obj.review_notes && (
                <div className="px-3 pt-2.5 pb-1 text-xs opacity-80">
                  <span className="font-semibold">Review notes: </span>
                  {obj.review_notes}
                </div>
              )}
              {obj.patterns_applied.length > 0 && (
                <div className="px-3 py-1 flex flex-wrap gap-1">
                  {obj.patterns_applied.map((p) => (
                    <span
                      key={p}
                      className="inline-block rounded bg-white/70 px-1.5 py-0.5 text-xs font-mono opacity-75"
                    >
                      {p}
                    </span>
                  ))}
                </div>
              )}
              <div className="flex border-t border-current/10 text-xs">
                <button
                  className={`px-3 py-1.5 font-medium transition-colors ${tab === 'converted' ? 'bg-white/80 underline' : 'opacity-60 hover:opacity-80'}`}
                  onClick={() => setTab('converted')}
                >
                  PostgreSQL
                </button>
                <button
                  className={`px-3 py-1.5 font-medium transition-colors ${tab === 'original' ? 'bg-white/80 underline' : 'opacity-60 hover:opacity-80'}`}
                  onClick={() => setTab('original')}
                >
                  Oracle (original)
                </button>
              </div>
              <pre className="max-h-96 overflow-auto bg-gray-900 text-green-200 text-xs p-3 font-mono whitespace-pre-wrap leading-relaxed">
                {tab === 'converted'
                  ? (obj.converted_code || '(no output)')
                  : obj.oracle_source}
              </pre>
              {tab === 'converted' && obj.converted_code && (
                <div className="px-3 py-1.5 border-t border-current/10">
                  <button
                    className="text-xs font-medium opacity-70 hover:opacity-100"
                    onClick={() => navigator.clipboard.writeText(obj.converted_code!)}
                  >
                    Copy SQL
                  </button>
                </div>
              )}
            </>
          )}
        </div>
      )}
    </div>
  );
}

function ConversionResult({ data }: { data: CodeConversionResponse }) {
  const highCount   = data.results.filter((r) => r.confidence === 'high').length;
  const mediumCount = data.results.filter((r) => r.confidence === 'medium').length;
  const lowCount    = data.results.filter((r) => r.confidence === 'low' && !r.error).length;

  return (
    <div className="mt-3 space-y-3">
      <div className="flex items-center gap-4 flex-wrap text-xs">
        <div>
          <p className="font-medium uppercase tracking-wide text-gray-400 mb-1">Results</p>
          <div className="flex gap-2">
            <span className="text-gray-600">{data.objects_found} objects in schema</span>
            <span className="text-gray-400">·</span>
            <span className="text-gray-600">{data.objects_attempted} converted</span>
          </div>
        </div>
        <div className="flex gap-2">
          {highCount > 0 && (
            <span className="text-green-600 font-medium">{highCount} high confidence</span>
          )}
          {mediumCount > 0 && (
            <span className="text-amber-600 font-medium">{mediumCount} medium</span>
          )}
          {lowCount > 0 && (
            <span className="text-red-500 font-medium">{lowCount} low</span>
          )}
          {data.objects_failed > 0 && (
            <span className="text-red-700 font-medium">{data.objects_failed} failed</span>
          )}
        </div>
      </div>

      {data.objects_found > data.objects_attempted && (
        <div className="rounded-lg border border-amber-200 bg-amber-50 px-3 py-2 text-xs text-amber-700">
          {data.objects_found - data.objects_attempted} objects not converted in this run
          (limit={data.objects_attempted}). Run again with a higher limit or in batches.
        </div>
      )}

      <div className="space-y-2">
        {data.results.map((obj) => (
          <ObjectCard key={`${obj.object_type}:${obj.object_name}`} obj={obj} />
        ))}
      </div>
    </div>
  );
}

function HistoryList({
  items,
  migrationId,
  onLoad,
}: {
  items: CodeConversionRunItem[];
  migrationId: string;
  onLoad: (data: CodeConversionResponse) => void;
}) {
  const [loadingId, setLoadingId] = useState<string | null>(null);

  if (items.length === 0) {
    return (
      <div className="mt-4 rounded-lg border border-gray-100 bg-gray-50 p-3 text-sm text-gray-500">
        No previous conversion runs.
      </div>
    );
  }

  async function loadRun(runId: string) {
    setLoadingId(runId);
    try {
      const data = await getCodeConversionRun(migrationId, runId);
      onLoad(data);
    } catch {
      // ignore
    } finally {
      setLoadingId(null);
    }
  }

  return (
    <div className="mt-4 space-y-2">
      <p className="text-xs font-medium uppercase tracking-wide text-gray-400">Run history</p>
      {items.map((item) => (
        <div
          key={item.run_id}
          className="flex items-center gap-3 rounded-lg border border-gray-200 bg-gray-50 px-3 py-2 text-sm flex-wrap"
        >
          <span className="text-gray-600">{new Date(item.created_at).toLocaleString()}</span>
          <span className="text-xs text-gray-400">
            · {item.objects_converted}/{item.objects_attempted} converted
            {item.objects_failed > 0 && (
              <span className="ml-1 text-red-500 font-medium">· {item.objects_failed} failed</span>
            )}
          </span>
          <button
            disabled={loadingId === item.run_id}
            onClick={() => loadRun(item.run_id)}
            className="ml-auto text-xs font-medium text-indigo-600 hover:underline disabled:opacity-50"
          >
            {loadingId === item.run_id ? 'Loading…' : 'View results'}
          </button>
        </div>
      ))}
    </div>
  );
}

export default function CodeConversionPanel({ migrationId }: { migrationId: string }) {
  const { user } = useAuthStore();
  const canRun = user?.role === 'admin' || user?.role === 'operator';

  const [runState, setRunState] = useState<RunState>({ tag: 'idle' });
  const [limit, setLimit] = useState(10);
  const [history, setHistory] = useState<CodeConversionRunItem[] | null>(null);
  const [historyLoading, setHistoryLoading] = useState(false);

  async function run() {
    setRunState({ tag: 'running' });
    try {
      const data = await runCodeConversion(migrationId, limit);
      setRunState({ tag: 'ok', data });
      setHistory(null);
    } catch (e: any) {
      setRunState({
        tag: 'error',
        msg: e?.response?.data?.detail || e?.message || 'Code conversion failed.',
      });
    }
  }

  async function loadHistory() {
    if (history !== null) { setHistory(null); return; }
    setHistoryLoading(true);
    try {
      setHistory(await listCodeConversionRuns(migrationId));
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
          <h2 className="text-lg font-semibold text-gray-900">PL/SQL Code Conversion</h2>
          <p className="mt-0.5 text-xs text-gray-500">
            Converts Oracle stored procedures, functions, triggers, and packages to
            PostgreSQL PL/pgSQL using Claude AI
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
                value={limit}
                onChange={(e) => setLimit(Number(e.target.value))}
                disabled={runState.tag === 'running'}
                className="rounded-lg border border-gray-200 px-2 py-1.5 text-xs text-gray-600 bg-white disabled:opacity-50"
              >
                <option value={5}>5 objects</option>
                <option value={10}>10 objects</option>
                <option value={25}>25 objects</option>
                <option value={50}>50 objects</option>
              </select>
              <button
                onClick={run}
                disabled={runState.tag === 'running'}
                className="rounded-lg bg-emerald-600 px-3 py-1.5 text-xs font-medium text-white hover:bg-emerald-700 disabled:opacity-50"
              >
                {runState.tag === 'running' ? 'Converting…' :
                 runState.tag === 'idle'    ? 'Convert PL/SQL' :
                 'Re-convert'}
              </button>
            </>
          )}
        </div>
      </div>

      {runState.tag === 'idle' && (
        <p className="mt-3 text-sm text-gray-600">
          Fetches stored procedures, functions, triggers, and packages from the Oracle source
          schema and converts each to PostgreSQL PL/pgSQL. Claude provides converted code,
          confidence rating (high/medium/low), and review notes explaining what changed.
          Requires a configured Anthropic API key in Settings.
        </p>
      )}

      {runState.tag === 'running' && (
        <div className="mt-3 space-y-1">
          <p className="text-sm text-gray-500">Converting PL/SQL objects via Claude AI…</p>
          <p className="text-xs text-gray-400">
            This may take 30–120 seconds depending on the number and complexity of objects.
          </p>
        </div>
      )}

      {runState.tag === 'error' && (
        <div className="mt-3 rounded-lg border border-red-200 bg-red-50 p-3 text-sm text-red-700">
          {runState.msg}
          {runState.msg.includes('Anthropic API key') && (
            <a href="/settings" className="ml-2 underline font-medium">
              Configure in Settings →
            </a>
          )}
        </div>
      )}

      {runState.tag === 'ok' && (
        <ConversionResult data={runState.data} />
      )}

      {history !== null && (
        <HistoryList
          items={history}
          migrationId={migrationId}
          onLoad={(data) => setRunState({ tag: 'ok', data })}
        />
      )}
    </section>
  );
}

'use client';

/**
 * /assess — public, no-auth, paste-DDL → complexity report.
 *
 * This is the top-of-funnel for depart.cloud. The whole design
 * optimizes for "time to insight < 5 seconds from landing page click."
 * EDB Migration Portal and Ispirer both gate this behind a signup;
 * we deliberately do not.
 *
 * The page is three states driven by a single `phase` discriminant:
 *   input    — textarea + "Try HR sample" + Analyze button
 *   running  — streaming-style progress indicator
 *   results  — score card, inventory, risk list, next-step CTAs
 *
 * Results never auto-persist. A real "save assessment" flow can land
 * later behind a soft email gate; today the URL-less report IS the
 * product hook.
 */

import { useEffect, useState } from 'react';

import SelfHostedGuard from '@/app/components/SelfHostedGuard';
import { apiBaseUrl } from '@/app/lib/api';

import { HR_SAMPLE } from './sample';


type ConversionExample = {
  tag: string;
  title: string;
  oracle: string;
  postgres: string;
  reasoning: string;
  confidence: 'high' | 'medium' | 'needs-review';
};

type Risk = { tag: string; tier: 'A' | 'B' | 'C'; label: string; guidance: string; count: number };

type AssessResult = {
  score: number;
  total_lines: number;
  auto_convertible_lines: number;
  needs_review_lines: number;
  must_rewrite_lines: number;
  effort_estimate_days: number;
  estimated_cost: number;
  objects_by_kind: Record<string, number>;
  construct_counts: Record<string, number>;
  top_constructs: string[];
  risks: Risk[];
};

type Phase =
  | { kind: 'input' }
  | { kind: 'running' }
  | { kind: 'results'; data: AssessResult; ddl: string };

export default function AssessPage() {
  return (
    <SelfHostedGuard>
      <AssessContent />
    </SelfHostedGuard>
  );
}

function AssessContent() {
  const [ddl, setDdl] = useState('');
  const [phase, setPhase] = useState<Phase>({ kind: 'input' });
  const [error, setError] = useState<string>('');

  async function runAssessment(source: string) {
    setError('');
    setPhase({ kind: 'running' });
    try {
      const resp = await fetch(`${apiBaseUrl()}/api/v1/assess`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ ddl: source }),
      });
      if (!resp.ok) {
        const detail = await resp.text();
        throw new Error(detail || `HTTP ${resp.status}`);
      }
      const data = (await resp.json()) as AssessResult;
      setPhase({ kind: 'results', data, ddl: source });
    } catch (e: unknown) {
      const msg = e instanceof Error ? e.message : 'Unknown error';
      setError(msg);
      setPhase({ kind: 'input' });
    }
  }

  return (
    <main className="min-h-screen bg-gradient-to-br from-purple-50 to-blue-50">
      <div className="container mx-auto max-w-5xl px-4 py-12">
        <Header />

        {error && <ErrorBanner message={error} onDismiss={() => setError('')} />}

        {phase.kind === 'input' && (
          <InputPane
            ddl={ddl}
            onDdlChange={setDdl}
            onAnalyze={() => runAssessment(ddl)}
            onTrySample={() => {
              setDdl(HR_SAMPLE);
              runAssessment(HR_SAMPLE);
            }}
          />
        )}

        {phase.kind === 'running' && <RunningPane />}

        {phase.kind === 'results' && (
          <ResultsPane
            result={phase.data}
            ddl={phase.ddl}
            onReset={() => {
              setDdl('');
              setPhase({ kind: 'input' });
            }}
          />
        )}
      </div>
    </main>
  );
}

/* ─── Sections ───────────────────────────────────────────────────────────── */

function Header() {
  return (
    <div className="mb-8 text-center">
      <h1 className="text-4xl font-bold text-gray-900">
        Assess your Oracle → Postgres migration
      </h1>
      <p className="mt-3 text-lg text-gray-600">
        Paste your DDL. Get a complexity score, a risk list, and an effort estimate. No signup.
      </p>
      <p className="mt-2 text-sm text-gray-500">
        🔒 We parse in-process only. Nothing is stored unless you save it.
      </p>
    </div>
  );
}

function ErrorBanner({ message, onDismiss }: { message: string; onDismiss: () => void }) {
  return (
    <div className="mb-6 rounded-lg border border-red-200 bg-red-50 p-4">
      <p className="font-medium text-red-700">Assessment failed</p>
      <p className="mt-1 text-sm text-red-600">{message}</p>
      <button onClick={onDismiss} className="mt-2 text-xs text-red-500 hover:text-red-700">
        Dismiss
      </button>
    </div>
  );
}

function InputPane({
  ddl,
  onDdlChange,
  onAnalyze,
  onTrySample,
}: {
  ddl: string;
  onDdlChange: (v: string) => void;
  onAnalyze: () => void;
  onTrySample: () => void;
}) {
  const hasContent = ddl.trim().length > 0;
  return (
    <div className="rounded-xl border border-gray-200 bg-white p-6 shadow-sm">
      <label
        htmlFor="ddl-input"
        className="mb-2 block text-sm font-semibold text-gray-700"
      >
        Paste Oracle DDL / PL-SQL
      </label>
      <textarea
        id="ddl-input"
        value={ddl}
        onChange={(e) => onDdlChange(e.target.value)}
        placeholder="CREATE TABLE employees (...);&#10;CREATE OR REPLACE PROCEDURE ...;"
        className="h-72 w-full resize-y rounded-md border border-gray-300 bg-gray-50 p-3 font-mono text-sm focus:border-purple-400 focus:outline-none focus:ring-1 focus:ring-purple-400"
      />
      <div className="mt-4 flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between">
        <button
          onClick={onTrySample}
          className="text-sm text-purple-600 underline-offset-2 hover:underline"
        >
          🎯 Try the HR sample schema
        </button>
        <button
          onClick={onAnalyze}
          disabled={!hasContent}
          className="rounded-md bg-purple-600 px-6 py-2 font-semibold text-white shadow-sm transition hover:bg-purple-700 disabled:cursor-not-allowed disabled:bg-gray-300"
        >
          Analyze →
        </button>
      </div>
    </div>
  );
}

function RunningPane() {
  const steps = [
    'Parsing DDL with ANTLR...',
    'Building dialect-agnostic IR...',
    'Scoring complexity...',
    'Flagging risky constructs...',
  ];
  return (
    <div className="rounded-xl border border-gray-200 bg-white p-8 shadow-sm">
      <p className="mb-6 text-lg font-semibold text-gray-800">Running assessment...</p>
      <ul className="space-y-2 text-sm text-gray-600">
        {steps.map((s) => (
          <li key={s} className="flex items-center gap-2">
            <span className="inline-block h-2 w-2 animate-pulse rounded-full bg-purple-500" />
            {s}
          </li>
        ))}
      </ul>
    </div>
  );
}

function ResultsPane({
  result,
  onReset,
}: {
  result: AssessResult;
  ddl: string;
  onReset: () => void;
}) {
  return (
    <div className="space-y-6">
      <ScoreCard result={result} />
      <InventoryCards objects={result.objects_by_kind} />
      <RiskList risks={result.risks} />
      <NextSteps onReset={onReset} />
    </div>
  );
}

function ScoreCard({ result }: { result: AssessResult }) {
  const { score, effort_estimate_days, auto_convertible_lines, total_lines } = result;
  const scoreClass =
    score < 40 ? 'score-low' : score < 70 ? 'score-medium' : 'score-high';
  const label = score < 40 ? 'Low' : score < 70 ? 'Medium' : 'High';
  const autoPct =
    total_lines > 0 ? Math.round((auto_convertible_lines / total_lines) * 100) : 0;

  return (
    <div className="rounded-xl border border-gray-200 bg-white p-6 shadow-sm">
      <div className="flex flex-col items-start gap-6 sm:flex-row sm:items-center">
        <div className={`score-badge ${scoreClass} !h-28 !w-28 !text-3xl`}>{score}</div>
        <div className="flex-1 space-y-2">
          <h2 className="text-2xl font-bold text-gray-900">
            Migration complexity: <span className="capitalize">{label}</span>
          </h2>
          <div className="grid grid-cols-1 gap-2 text-sm text-gray-700 sm:grid-cols-3">
            <Stat label="Estimated effort" value={`${effort_estimate_days.toFixed(1)} engineer-days`} />
            <Stat label="Auto-convertible" value={`${autoPct}% of lines`} />
            <Stat label="Total lines analyzed" value={total_lines.toLocaleString()} />
          </div>
        </div>
      </div>
    </div>
  );
}

function Stat({ label, value }: { label: string; value: string }) {
  return (
    <div>
      <div className="text-xs uppercase tracking-wide text-gray-500">{label}</div>
      <div className="mt-1 font-semibold text-gray-900">{value}</div>
    </div>
  );
}

function InventoryCards({ objects }: { objects: Record<string, number> }) {
  const entries = Object.entries(objects).sort((a, b) => b[1] - a[1]);
  if (entries.length === 0) return null;
  return (
    <div className="rounded-xl border border-gray-200 bg-white p-6 shadow-sm">
      <h3 className="mb-4 text-lg font-semibold text-gray-900">Schema inventory</h3>
      <div className="grid grid-cols-2 gap-3 sm:grid-cols-4">
        {entries.map(([kind, n]) => (
          <div
            key={kind}
            className="rounded-md border border-gray-100 bg-gray-50 p-3 text-center"
          >
            <div className="text-2xl font-bold text-purple-700">{n}</div>
            <div className="mt-1 text-xs uppercase tracking-wide text-gray-500">
              {kind.toLowerCase()}
            </div>
          </div>
        ))}
      </div>
    </div>
  );
}

function RiskList({ risks }: { risks: Risk[] }) {
  if (risks.length === 0) {
    return (
      <div className="rounded-xl border border-green-200 bg-green-50 p-6">
        <h3 className="text-lg font-semibold text-green-900">
          ✓ No Tier B or C constructs detected
        </h3>
        <p className="mt-2 text-sm text-green-800">
          Everything in this sample maps 1:1 to Postgres. You can migrate this schema
          with the free open-source runner alone.
        </p>
      </div>
    );
  }
  return (
    <div className="rounded-xl border border-gray-200 bg-white p-6 shadow-sm">
      <h3 className="mb-4 text-lg font-semibold text-gray-900">
        Risks & rewrites needed ({risks.length})
      </h3>
      <ul className="divide-y divide-gray-100">
        {risks.map((r) => (
          <RiskItem key={r.tag} risk={r} />
        ))}
      </ul>
    </div>
  );
}

function RiskItem({ risk }: { risk: Risk }) {
  const [open, setOpen] = useState(false);
  return (
    <li className="py-4">
      <button
        type="button"
        onClick={() => setOpen(!open)}
        className="flex w-full items-start gap-4 text-left"
        aria-expanded={open}
      >
        <TierBadge tier={risk.tier} />
        <div className="flex-1">
          <div className="flex items-baseline justify-between gap-2">
            <span className="font-semibold text-gray-900">{risk.label}</span>
            <span className="text-xs text-gray-500">
              {risk.count} {risk.count === 1 ? 'occurrence' : 'occurrences'}
            </span>
          </div>
          <p className="mt-1 text-sm text-gray-600">{risk.guidance}</p>
          <div className="mt-2 text-xs font-semibold text-purple-600">
            {open ? '▼ Hide conversion example' : '▶ Show conversion example'}
          </div>
        </div>
      </button>
      {open && <ConversionPanel tag={risk.tag} />}
    </li>
  );
}

function ConversionPanel({ tag }: { tag: string }) {
  const [state, setState] = useState<
    | { kind: 'loading' }
    | { kind: 'error'; message: string }
    | { kind: 'loaded'; example: ConversionExample }
  >({ kind: 'loading' });

  useEffect(() => {
    let cancelled = false;
    (async () => {
      try {
        const resp = await fetch(`${apiBaseUrl()}/api/v1/convert/${tag}`);
        if (!resp.ok) {
          const detail = resp.status === 404
            ? 'No canonical example yet — paid tier runs AI conversion on your actual code.'
            : `HTTP ${resp.status}`;
          throw new Error(detail);
        }
        const example = (await resp.json()) as ConversionExample;
        if (!cancelled) setState({ kind: 'loaded', example });
      } catch (e: unknown) {
        if (cancelled) return;
        const msg = e instanceof Error ? e.message : 'Unknown error';
        setState({ kind: 'error', message: msg });
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [tag]);

  if (state.kind === 'loading') {
    return (
      <div className="mt-4 rounded-md border border-gray-100 bg-gray-50 p-4 text-sm text-gray-500">
        Loading canonical conversion...
      </div>
    );
  }
  if (state.kind === 'error') {
    return (
      <div className="mt-4 rounded-md border border-amber-200 bg-amber-50 p-4 text-sm text-amber-800">
        {state.message}
      </div>
    );
  }

  const { example } = state;
  return (
    <div className="mt-4 rounded-lg border border-purple-100 bg-purple-50/40 p-4">
      <div className="mb-3 flex items-center justify-between">
        <div className="font-semibold text-gray-900">{example.title}</div>
        <ConfidenceBadge level={example.confidence} />
      </div>
      <div className="grid grid-cols-1 gap-3 lg:grid-cols-2">
        <CodeBlock label="Oracle" code={example.oracle} accent="red" />
        <CodeBlock label="Postgres" code={example.postgres} accent="green" />
      </div>
      <p className="mt-3 text-sm text-gray-700">{example.reasoning}</p>
      <p className="mt-3 text-xs italic text-gray-500">
        Canonical reference example. Use the box below to run live AI conversion on your
        actual snippet.
      </p>
      <LiveConvertBox tag={tag} />
    </div>
  );
}


/* ─── Live BYOK convert ─────────────────────────────────────────────────── */

function LiveConvertBox({ tag }: { tag: string }) {
  const [configured, setConfigured] = useState<boolean | null>(null);
  const [snippet, setSnippet] = useState('');
  const [context, setContext] = useState('');
  const [state, setState] = useState<
    | { kind: 'idle' }
    | { kind: 'running' }
    | { kind: 'error'; message: string }
    | { kind: 'loaded'; example: ConversionExample }
  >({ kind: 'idle' });

  useEffect(() => {
    let cancelled = false;
    (async () => {
      try {
        const r = await fetch(`${apiBaseUrl()}/api/v1/settings`);
        if (!r.ok) return;
        const body = (await r.json()) as { anthropic_key_configured: boolean };
        if (!cancelled) setConfigured(body.anthropic_key_configured);
      } catch {
        if (!cancelled) setConfigured(false);
      }
    })();
    return () => {
      cancelled = true;
    };
  }, []);

  async function run() {
    setState({ kind: 'running' });
    try {
      const resp = await fetch(`${apiBaseUrl()}/api/v1/convert/${tag}`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ snippet, context: context || null }),
      });
      if (!resp.ok) {
        const msg = resp.status === 412
          ? 'No Anthropic key configured on this instance. Go to /settings/instance to add one.'
          : `HTTP ${resp.status}: ${await resp.text()}`;
        throw new Error(msg);
      }
      const example = (await resp.json()) as ConversionExample;
      setState({ kind: 'loaded', example });
    } catch (e) {
      setState({
        kind: 'error',
        message: e instanceof Error ? e.message : 'Unknown error',
      });
    }
  }

  if (configured === null) return null;  // first-paint, don't flash

  if (!configured) {
    return (
      <div className="mt-4 rounded-md border border-gray-200 bg-white p-3 text-xs text-gray-600">
        💡 Configure an Anthropic API key at{' '}
        <a href="/settings/instance" className="text-purple-700 underline">
          /settings/instance
        </a>{' '}
        to run live AI conversion on your actual snippet. (BYOK — your key stays on this
        instance.)
      </div>
    );
  }

  return (
    <div className="mt-4 rounded-lg border border-purple-200 bg-white p-4">
      <div className="mb-3 text-sm font-semibold text-gray-900">
        Convert your actual code
      </div>
      <label
        htmlFor={`snippet-${tag}`}
        className="block text-xs font-medium text-gray-600"
      >
        Oracle snippet
      </label>
      <textarea
        id={`snippet-${tag}`}
        value={snippet}
        onChange={(e) => setSnippet(e.target.value)}
        placeholder="Paste your actual Oracle snippet for this construct..."
        className="mt-1 h-28 w-full resize-y rounded-md border border-gray-300 bg-gray-50 p-2 font-mono text-xs focus:border-purple-400 focus:outline-none focus:ring-1 focus:ring-purple-400"
        disabled={state.kind === 'running'}
      />
      <label
        htmlFor={`context-${tag}`}
        className="mt-3 block text-xs font-medium text-gray-600"
      >
        Enclosing context (optional — e.g. enclosing procedure signature)
      </label>
      <textarea
        id={`context-${tag}`}
        value={context}
        onChange={(e) => setContext(e.target.value)}
        className="mt-1 h-16 w-full resize-y rounded-md border border-gray-300 bg-gray-50 p-2 font-mono text-xs focus:border-purple-400 focus:outline-none focus:ring-1 focus:ring-purple-400"
        disabled={state.kind === 'running'}
      />
      <button
        onClick={() => void run()}
        disabled={state.kind === 'running' || !snippet.trim()}
        className="mt-3 rounded-md bg-purple-600 px-4 py-2 text-sm font-semibold text-white transition hover:bg-purple-700 disabled:cursor-not-allowed disabled:bg-gray-300"
      >
        {state.kind === 'running' ? 'Running Claude...' : 'Convert with Claude →'}
      </button>

      {state.kind === 'error' && (
        <div className="mt-3 rounded-md border border-amber-200 bg-amber-50 p-3 text-xs text-amber-800">
          {state.message}
        </div>
      )}

      {state.kind === 'loaded' && (
        <div className="mt-4 border-t border-gray-100 pt-4">
          <div className="mb-3 flex items-center justify-between">
            <div className="text-sm font-semibold text-gray-900">
              Claude&apos;s conversion
            </div>
            <ConfidenceBadge level={state.example.confidence} />
          </div>
          <div className="grid grid-cols-1 gap-3 lg:grid-cols-2">
            <CodeBlock label="Your Oracle" code={state.example.oracle} accent="red" />
            <CodeBlock label="Postgres" code={state.example.postgres} accent="green" />
          </div>
          <p className="mt-3 text-sm text-gray-700">{state.example.reasoning}</p>
        </div>
      )}
    </div>
  );
}

function CodeBlock({
  label,
  code,
  accent,
}: {
  label: string;
  code: string;
  accent: 'red' | 'green';
}) {
  const accentClass =
    accent === 'red'
      ? 'border-red-200 bg-red-50/60 text-red-900'
      : 'border-green-200 bg-green-50/60 text-green-900';
  return (
    <div className={`overflow-hidden rounded-md border ${accentClass}`}>
      <div className="border-b border-current/10 px-3 py-1 text-xs font-semibold uppercase tracking-wide opacity-70">
        {label}
      </div>
      <pre className="overflow-x-auto p-3 text-xs leading-relaxed">
        <code>{code}</code>
      </pre>
    </div>
  );
}

function ConfidenceBadge({ level }: { level: ConversionExample['confidence'] }) {
  const styles: Record<ConversionExample['confidence'], string> = {
    high: 'bg-green-100 text-green-800 border-green-200',
    medium: 'bg-yellow-100 text-yellow-800 border-yellow-300',
    'needs-review': 'bg-red-100 text-red-800 border-red-300',
  };
  const labels: Record<ConversionExample['confidence'], string> = {
    high: 'High confidence',
    medium: 'Medium confidence',
    'needs-review': 'Needs architectural review',
  };
  return (
    <span
      className={`inline-flex whitespace-nowrap rounded-md border px-2 py-0.5 text-xs font-semibold ${styles[level]}`}
    >
      {labels[level]}
    </span>
  );
}

function TierBadge({ tier }: { tier: Risk['tier'] }) {
  const styles: Record<Risk['tier'], string> = {
    A: 'bg-green-100 text-green-800 border-green-200',
    B: 'bg-yellow-100 text-yellow-800 border-yellow-300',
    C: 'bg-red-100 text-red-800 border-red-300',
  };
  const labels: Record<Risk['tier'], string> = {
    A: 'Tier A · auto',
    B: 'Tier B · review',
    C: 'Tier C · rewrite',
  };
  return (
    <span
      className={`inline-flex whitespace-nowrap rounded-md border px-2 py-0.5 text-xs font-semibold ${styles[tier]}`}
    >
      {labels[tier]}
    </span>
  );
}

function NextSteps({ onReset }: { onReset: () => void }) {
  return (
    <div className="rounded-xl border border-purple-200 bg-purple-50 p-6">
      <h3 className="text-lg font-semibold text-purple-900">What&apos;s next?</h3>
      <ul className="mt-3 space-y-2 text-sm text-purple-800">
        <li>
          📄 <strong>Email me the full PDF runbook</strong> — with per-object conversion
          guidance and load-order plan. (Coming soon — free.)
        </li>
        <li>
          🚀 <strong>Download the on-prem runner</strong> — Docker image, keyset-paginated
          COPY, Merkle-verified correctness. Data stays in your VPC. (Paid per-project.)
        </li>
        <li>
          📅 <strong>Talk to a migration engineer</strong> — 30 minutes, free, no pitch.
        </li>
      </ul>
      <button
        onClick={onReset}
        className="mt-5 text-sm text-purple-700 underline-offset-2 hover:underline"
      >
        ← Assess another schema
      </button>
    </div>
  );
}

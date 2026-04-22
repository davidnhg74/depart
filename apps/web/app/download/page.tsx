/**
 * /download — the product's entry point.
 *
 * The self-hosted product lives here, not /signup. The page prioritizes
 * "get depart running on your machine in two commands" over funnel-y
 * registration flows. Three install methods listed: compose (dev/demo),
 * single Docker image (staging), and OVA/tarball (air-gapped prod).
 *
 * Everything below the first fold is about reassuring compliance /
 * security reviewers: network flow, what depart can and cannot see,
 * license verification.
 */

import Link from 'next/link';


export default function DownloadPage() {
  return (
    <main className="min-h-screen bg-gray-50">
      <div className="container mx-auto max-w-5xl px-4 py-16">
        <Header />
        <QuickStart />
        <Architecture />
        <InstallMethods />
        <Requirements />
        <Verification />
        <NextSteps />
      </div>
    </main>
  );
}

/* ─── Sections ─────────────────────────────────────────────────────────── */

function Header() {
  return (
    <div className="mb-10 text-center">
      <h1 className="text-4xl font-bold text-gray-900 md:text-5xl">
        Download depart
      </h1>
      <p className="mx-auto mt-4 max-w-2xl text-lg text-gray-600">
        depart ships as a self-hosted bundle. The web UI, API, parser, AI gateway, and
        data-movement runner all run on your machine. Nothing phones home.
      </p>
    </div>
  );
}

function QuickStart() {
  return (
    <section className="mb-12 rounded-xl border border-purple-200 bg-white p-8 shadow-sm">
      <h2 className="text-2xl font-bold text-gray-900">Two-command quick start</h2>
      <p className="mt-2 text-gray-600">
        Works on macOS, Linux, and Windows with Docker Desktop or any Docker daemon.
      </p>
      <pre className="mt-6 overflow-x-auto rounded-lg bg-gray-900 p-6 font-mono text-sm text-green-200">
{`# 1. Clone the repo
git clone https://github.com/davidnhg74/depart.git
cd depart

# 2. Boot the whole stack
docker compose up -d

# That's it. Open the UI:
open http://localhost:3000`}
      </pre>
      <p className="mt-4 text-sm text-gray-500">
        First boot pulls the Postgres + API + web images and runs the schema migrations.
        Takes ~2 minutes on a warm cache, ~5 minutes on first install.
      </p>
    </section>
  );
}

function Architecture() {
  return (
    <section className="mb-12 rounded-xl border border-gray-200 bg-white p-8 shadow-sm">
      <h2 className="text-2xl font-bold text-gray-900">What runs where</h2>
      <p className="mt-2 text-gray-600">
        Every box below lives inside your firewall. The only outbound connection depart ever
        makes is AI conversion calls to the provider you choose, using the API key you
        provide.
      </p>

      <pre className="mt-6 overflow-x-auto rounded-lg border border-gray-200 bg-gray-50 p-6 font-mono text-xs leading-relaxed text-gray-800">
{`┌──────────────── your infrastructure ────────────────┐
│                                                     │
│  ┌─ depart bundle (Docker compose) ──┐              │
│  │                                   │              │
│  │  Next.js UI ── localhost:3000     │              │
│  │  FastAPI   ── localhost:8000      │              │
│  │  Postgres  ── internal (metadata) │              │
│  │                                   │              │
│  │  Embedded: parser, AI gateway,    │              │
│  │            data-movement runner   │              │
│  └─────────┬─────────────────────────┘              │
│            │                                        │
│            ▼                                        │
│  ┌─ your Oracle (read-only) ─┐                      │
│  │  ALL_TABLES, ALL_TAB_COLS │                      │
│  │  SELECT for row streaming │                      │
│  └───────────────────────────┘                      │
│            │                                        │
│            ▼                                        │
│  ┌─ your Postgres target ─┐                         │
│  │  CREATE TABLE (DDL gen)│                         │
│  │  COPY (binary)         │                         │
│  └────────────────────────┘                         │
│                                                     │
└──────────────────────────────────────────┬──────────┘
                                           │ only outbound:
                                           ▼ (optional) BYOK AI
                              ┌─ Anthropic / OpenAI / local model ─┐
                              │  key stays in your local config    │
                              │  we never see it                   │
                              └────────────────────────────────────┘`}
      </pre>

      <div className="mt-6 grid grid-cols-1 gap-4 md:grid-cols-2">
        <InfoBox
          title="What depart can see"
          items={[
            'Only what runs on your host',
            'Your Oracle via the read-only user you create',
            'Your Postgres via the admin user you provide',
          ]}
          tone="neutral"
        />
        <InfoBox
          title="What depart cannot see"
          items={[
            'Anything we could see — because nothing phones home',
            'Your DDL, your data, your connection strings',
            'Your AI provider keys (they sit in local config)',
          ]}
          tone="good"
        />
      </div>
    </section>
  );
}

function InfoBox({
  title,
  items,
  tone,
}: {
  title: string;
  items: string[];
  tone: 'good' | 'neutral';
}) {
  const border = tone === 'good' ? 'border-green-200 bg-green-50' : 'border-gray-200 bg-gray-50';
  const heading = tone === 'good' ? 'text-green-900' : 'text-gray-900';
  return (
    <div className={`rounded-lg border p-4 ${border}`}>
      <div className={`font-semibold ${heading}`}>{title}</div>
      <ul className="mt-2 space-y-1 text-sm text-gray-700">
        {items.map((i) => (
          <li key={i} className="flex gap-2">
            <span className="text-gray-400">•</span>
            <span>{i}</span>
          </li>
        ))}
      </ul>
    </div>
  );
}

function InstallMethods() {
  const methods = [
    {
      title: 'Docker Compose',
      blurb: 'Dev, demo, most production use.',
      command: 'git clone ... && docker compose up -d',
      status: 'Available now',
    },
    {
      title: 'Single Docker image',
      blurb: 'Staging, CI pipelines, Kubernetes.',
      command: 'docker run -p 3000:3000 -p 8000:8000 depart/depart:latest',
      status: 'Shipping with v0.2',
    },
    {
      title: 'OVA / tarball',
      blurb: 'Air-gapped enterprise installs. All deps vendored.',
      command: '# unpack on the target, run installer.sh',
      status: 'Enterprise tier',
    },
  ];
  return (
    <section className="mb-12 rounded-xl border border-gray-200 bg-white p-8 shadow-sm">
      <h2 className="text-2xl font-bold text-gray-900">Install methods</h2>
      <div className="mt-6 space-y-4">
        {methods.map((m) => (
          <div
            key={m.title}
            className="rounded-lg border border-gray-100 bg-gray-50 p-5"
          >
            <div className="flex flex-wrap items-baseline justify-between gap-2">
              <h3 className="text-lg font-semibold text-gray-900">{m.title}</h3>
              <span className="text-xs font-semibold text-purple-700">{m.status}</span>
            </div>
            <p className="mt-1 text-sm text-gray-600">{m.blurb}</p>
            <pre className="mt-3 overflow-x-auto rounded border border-gray-200 bg-white p-3 font-mono text-xs text-gray-800">
{m.command}
            </pre>
          </div>
        ))}
      </div>
    </section>
  );
}

function Requirements() {
  return (
    <section className="mb-12 rounded-xl border border-gray-200 bg-white p-8 shadow-sm">
      <h2 className="text-2xl font-bold text-gray-900">Requirements</h2>
      <div className="mt-6 grid grid-cols-1 gap-6 md:grid-cols-3">
        <ReqBox title="Host" items={['Docker 24+', '4 GB RAM', '5 GB disk', 'Linux/macOS/Win']} />
        <ReqBox title="Source" items={['Oracle 11g+', 'Read-only user', 'SELECT on ALL_*', 'Network reach']} />
        <ReqBox title="Target" items={['Postgres 13+', 'Superuser / CREATE', 'Network reach', 'RDS/Aurora/on-prem']} />
      </div>
    </section>
  );
}

function ReqBox({ title, items }: { title: string; items: string[] }) {
  return (
    <div className="rounded-lg border border-gray-100 bg-gray-50 p-4">
      <div className="font-semibold text-gray-900">{title}</div>
      <ul className="mt-2 space-y-1 text-sm text-gray-700">
        {items.map((i) => (
          <li key={i}>· {i}</li>
        ))}
      </ul>
    </div>
  );
}

function Verification() {
  return (
    <section className="mb-12 rounded-xl border border-gray-200 bg-white p-8 shadow-sm">
      <h2 className="text-2xl font-bold text-gray-900">Verify your install</h2>
      <p className="mt-2 text-gray-600">
        After <code className="rounded bg-gray-100 px-1">docker compose up</code> finishes:
      </p>
      <ol className="mt-4 space-y-3 text-sm text-gray-700">
        <li>
          <strong>1. Health check:</strong>{' '}
          <code className="rounded bg-gray-100 px-2 py-0.5 text-xs">
            curl http://localhost:8000/health
          </code>{' '}
          should return <code className="rounded bg-gray-100 px-1 text-xs">{`{"status":"ok"}`}</code>
        </li>
        <li>
          <strong>2. Open the UI:</strong>{' '}
          <a href="http://localhost:3000" className="text-purple-600 underline">
            http://localhost:3000
          </a>{' '}
          — the landing page should render.
        </li>
        <li>
          <strong>3. Run an assessment:</strong> go to{' '}
          <Link href="/assess" className="text-purple-600 underline">
            /assess
          </Link>
          , click &ldquo;Try the HR sample,&rdquo; confirm you see a risk list.
        </li>
        <li>
          <strong>4. (Optional) Add your AI key:</strong> drop your Anthropic API key into{' '}
          <code className="rounded bg-gray-100 px-1 text-xs">.env</code> — AI conversion previews
          will expand to live conversion of your actual snippets.
        </li>
      </ol>
    </section>
  );
}

function NextSteps() {
  return (
    <section className="rounded-xl border border-purple-200 bg-purple-50 p-8">
      <h2 className="text-2xl font-bold text-purple-900">Next steps</h2>
      <ul className="mt-4 space-y-3 text-sm text-purple-900">
        <li>
          📘{' '}
          <a href="https://github.com/davidnhg74/depart/blob/main/docs/getting-started.md" className="underline">
            Getting started
          </a>{' '}
          — first-migration walkthrough against the HR sample schema.
        </li>
        <li>
          🔐{' '}
          <Link href="/contact" className="underline">
            Buy a Pro license
          </Link>{' '}
          — $25k–$75k per project, offline-verified, unlocks full AI conversion and runbook PDF.
        </li>
        <li>
          💬{' '}
          <a href="https://github.com/davidnhg74/depart/issues" className="underline">
            Open an issue
          </a>{' '}
          — grammar gaps, type-mapping requests, feature asks.
        </li>
      </ul>
    </section>
  );
}

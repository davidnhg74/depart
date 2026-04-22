/**
 * depart.cloud landing page.
 *
 * The pitch in one scroll:
 *   Hero → Problem → Three Pillars → How It Works →
 *   What's in the Box → Pricing → Comparison → FAQ → Final CTA
 *
 * Primary CTA is /assess — the no-auth, paste-DDL funnel. Every
 * secondary CTA (pricing, signup) is a follow-up after the user has
 * seen the assessment deliver value. This reverses the Ispirer/EDB
 * pattern of gating value behind signup.
 */

import Link from 'next/link';


export default function LandingPage() {
  return (
    <div className="w-full">
      <Hero />
      <ProblemSection />
      <ThreePillars />
      <HowItWorks />
      <WhatsInTheBox />
      <Pricing />
      <Comparison />
      <FAQ />
      <FinalCTA />
      <Footer />
    </div>
  );
}

/* ─── Hero ──────────────────────────────────────────────────────────────── */

function Hero() {
  return (
    <section className="bg-gradient-to-br from-purple-900 via-purple-800 to-blue-900 text-white">
      <div className="container mx-auto px-4 py-24 md:py-32">
        <div className="mx-auto max-w-4xl text-center">
          <h1 className="text-4xl font-bold leading-tight md:text-6xl">
            Oracle → Postgres migration that never leaves your network.
          </h1>
          <p className="mx-auto mt-6 max-w-2xl text-lg text-purple-100 md:text-xl">
            depart is a <strong>self-hosted</strong> migration platform. Download the Docker
            image, run it inside your VPC, point it at your Oracle and Postgres. Nothing phones
            home. AI conversion uses your own Anthropic API key.
          </p>
          <p className="mt-4 text-lg font-semibold text-purple-200">
            Air-gap friendly. Audit-safe. Open source at the core.
          </p>

          <div className="mt-10 flex flex-wrap justify-center gap-4">
            <Link
              href="/download"
              className="rounded-lg bg-white px-8 py-4 font-bold text-purple-700 shadow-lg transition hover:bg-gray-100"
            >
              Download depart →
            </Link>
            <Link
              href="/assess"
              className="rounded-lg border border-white/30 bg-white/10 px-8 py-4 font-bold text-white transition hover:bg-white/20"
            >
              Try the online demo
            </Link>
          </div>

          <div className="mt-12 flex flex-wrap items-center justify-center gap-x-6 gap-y-2 text-sm text-purple-200">
            <span>🐳 Docker + OVA + tarball</span>
            <span>·</span>
            <span>🔒 Runs in your VPC</span>
            <span>·</span>
            <span>🧠 Bring your own AI key</span>
            <span>·</span>
            <span>📖 MIT at the core</span>
          </div>
        </div>
      </div>
    </section>
  );
}

/* ─── Problem ───────────────────────────────────────────────────────────── */

function ProblemSection() {
  return (
    <section className="bg-gray-50 py-20">
      <div className="container mx-auto max-w-4xl px-4">
        <h2 className="text-center text-3xl font-bold text-gray-900 md:text-4xl">
          Oracle is a tax. PostgreSQL is the way out.
        </h2>
        <div className="mt-10 grid grid-cols-1 gap-6 md:grid-cols-3">
          <Stat
            value="70–90%"
            label="TCO savings over Oracle per-core licensing"
          />
          <Stat
            value="2026"
            label="is the year Oracle audits get aggressive — and announcing a migration often triggers one"
          />
          <Stat
            value="73%"
            label="of enterprise SaaS already runs on Postgres. The readiness question is dead."
          />
        </div>
        <p className="mx-auto mt-10 max-w-3xl text-center text-gray-600">
          But the tools to get there are old. <strong>Ora2Pg</strong> caps at 4 billion rows per
          table and needs ~25% manual PL/SQL correction. <strong>EDB Migration Portal</strong>
          locks you into EDB Postgres Advanced Server. <strong>AWS SCT</strong> locks you into
          AWS. <strong>Ispirer</strong> won&apos;t show you pricing.
        </p>
      </div>
    </section>
  );
}

function Stat({ value, label }: { value: string; label: string }) {
  return (
    <div className="rounded-lg border border-gray-200 bg-white p-6 text-center shadow-sm">
      <div className="text-3xl font-bold text-purple-700">{value}</div>
      <div className="mt-2 text-sm text-gray-600">{label}</div>
    </div>
  );
}

/* ─── Three Pillars ─────────────────────────────────────────────────────── */

function ThreePillars() {
  return (
    <section className="py-20">
      <div className="container mx-auto max-w-5xl px-4">
        <h2 className="text-center text-3xl font-bold text-gray-900 md:text-4xl">
          A migration platform built for 2026.
        </h2>

        <div className="mt-12 grid grid-cols-1 gap-8 md:grid-cols-3">
          <Pillar
            icon="🔒"
            title="Runs entirely in your infra"
            body="Single Docker image (or OVA, or tarball). Everything — UI, parser, AI gateway, runner — inside your firewall. Air-gapped installs supported. No telemetry by default. We cannot see your DDL, your data, or even know you're running it."
          />
          <Pillar
            icon="🧠"
            title="AI conversion, your key"
            body="Bring your own Anthropic API key (or OpenAI, or a local model). The key lives in your local config. Claude calls go from your server to theirs — we are not in that path. Every conversion shows a diff with reasoning before anything gets applied."
          />
          <Pillar
            icon="🎯"
            title="Target-neutral"
            body="Postgres is Postgres. No upsell to EDB Advanced Server, no lock-in to AWS. Works against RDS, Aurora, Azure Database, Cloud SQL, Crunchy Bridge, Supabase, self-hosted — whatever you already run."
          />
        </div>
      </div>
    </section>
  );
}

function Pillar({ icon, title, body }: { icon: string; title: string; body: string }) {
  return (
    <div className="rounded-xl border border-gray-200 bg-white p-8 shadow-sm">
      <div className="text-4xl">{icon}</div>
      <h3 className="mt-4 text-xl font-bold text-gray-900">{title}</h3>
      <p className="mt-3 text-gray-600">{body}</p>
    </div>
  );
}

/* ─── How It Works ──────────────────────────────────────────────────────── */

function HowItWorks() {
  return (
    <section id="how-it-works" className="bg-gray-50 py-20">
      <div className="container mx-auto max-w-4xl px-4">
        <h2 className="text-center text-3xl font-bold text-gray-900 md:text-4xl">
          Three steps. No surprises.
        </h2>

        <div className="mt-12 space-y-8">
          <Step
            n={1}
            title="Install"
            body="docker compose up. The UI comes up on localhost:3000, the API on localhost:8000. No accounts, no cloud signup, no outbound calls. Takes about 2 minutes on a fresh host."
          />
          <Step
            n={2}
            title="Assess & plan"
            body="Point depart at a read-only Oracle account. It introspects the schema, scores complexity, flags risky constructs (MERGE, CONNECT BY, autonomous txns), and generates a table-by-table load plan — parents first, cycles with deferred constraints, sequence catch-up, rollback points."
          />
          <Step
            n={3}
            title="Run & verify"
            body="The runner streams rows via keyset-paginated COPY while Merkle verification runs behind it. Checkpointed and resumable — if a batch dies, --migration-id <id> picks up from the last verified keyset cursor. When it finishes, every row is hash-verified on both sides."
          />
        </div>
      </div>
    </section>
  );
}

function Step({ n, title, body }: { n: number; title: string; body: string }) {
  return (
    <div className="flex items-start gap-6 rounded-xl border border-gray-200 bg-white p-6 shadow-sm">
      <div className="flex h-12 w-12 flex-shrink-0 items-center justify-center rounded-md bg-purple-600 text-xl font-bold text-white">
        {n}
      </div>
      <div>
        <h3 className="text-xl font-bold text-gray-900">{title}</h3>
        <p className="mt-2 text-gray-600">{body}</p>
      </div>
    </div>
  );
}

/* ─── What's In The Box ─────────────────────────────────────────────────── */

function WhatsInTheBox() {
  const items = [
    {
      title: 'ANTLR-backed parser',
      body: 'with dialect-agnostic IR. Handles PL/SQL, PL/pgSQL, T-SQL.',
    },
    {
      title: 'Keyset pagination',
      body: 'with composite PK support. Oracle ROWNUM / OFFSET death spirals do not happen here.',
    },
    {
      title: 'Binary COPY protocol',
      body: 'for target-side writes. ~200K rows/sec on commodity hardware.',
    },
    {
      title: 'Merkle-tree row verification',
      body: 'at every batch. Mismatches flagged per-table, not silently swallowed.',
    },
    {
      title: 'Sequence catch-up',
      body: 'constraint deferral for FK cycles, resumable-from-last-checkpoint.',
    },
    {
      title: 'Open source under MIT',
      body: 'Read the code. Run it without us. Fork it if we disappear.',
    },
  ];
  return (
    <section className="py-20">
      <div className="container mx-auto max-w-5xl px-4">
        <h2 className="text-center text-3xl font-bold text-gray-900 md:text-4xl">
          Built on solid primitives.
        </h2>
        <div className="mt-12 grid grid-cols-1 gap-6 md:grid-cols-2">
          {items.map((it) => (
            <div
              key={it.title}
              className="rounded-lg border border-gray-100 bg-gray-50 p-6"
            >
              <div className="font-semibold text-gray-900">{it.title}</div>
              <div className="mt-1 text-sm text-gray-600">{it.body}</div>
            </div>
          ))}
        </div>
      </div>
    </section>
  );
}

/* ─── Pricing ───────────────────────────────────────────────────────────── */

function Pricing() {
  return (
    <section id="pricing" className="bg-gray-50 py-20">
      <div className="container mx-auto max-w-6xl px-4">
        <h2 className="text-center text-3xl font-bold text-gray-900 md:text-4xl">
          Pricing that matches how you buy.
        </h2>
        <p className="mx-auto mt-3 max-w-2xl text-center text-gray-600">
          Migrations are capex events and enterprises want to own their tools. We ship signed
          license files that run offline. No subscription, no phone-home, no vendor lock-in.
        </p>

        <div className="mt-12 grid grid-cols-1 gap-4 md:grid-cols-4">
          <PlanCard
            name="OSS Core"
            price="Free"
            blurb="Parser + runner + CLI. MIT."
            features={[
              'ANTLR parser + IR',
              'Data-movement runner',
              'Merkle verification',
              'CLI, Docker image',
            ]}
            cta={{ href: 'https://github.com/davidnhg74/depart', label: 'View on GitHub' }}
          />
          <PlanCard
            name="Community"
            price="Free"
            blurb="Full self-hosted install. No license."
            features={[
              'Everything in OSS Core',
              'Web UI (localhost)',
              'Assessment + risk list',
              'Canonical AI examples',
            ]}
            cta={{ href: '/download', label: 'Download →' }}
          />
          <PlanCard
            name="Pro"
            price="$25k–$75k"
            blurb="Per-project license. 90 days. Offline."
            highlight
            features={[
              'Everything in Community',
              'AI conversion on your actual code (BYOK)',
              'Runbook PDF generator',
              'Priority grammar fixes + email support',
            ]}
            cta={{ href: '/contact', label: 'Buy a license' }}
          />
          <PlanCard
            name="Enterprise"
            price="Custom"
            blurb="Site license + air-gap install."
            features={[
              'Everything in Pro',
              'Multi-project, unlimited',
              'Air-gap installer + SSO',
              'Dedicated support + SLA',
            ]}
            cta={{ href: '/contact', label: 'Contact sales' }}
          />
        </div>

        <p className="mx-auto mt-8 max-w-2xl text-center text-sm text-gray-500">
          Pro + Enterprise ship as signed offline license files (JWT). Verified locally, valid
          for the stated term, no network check required. A single successful migration
          typically recoups its license cost in Oracle savings within 90 days.
        </p>
      </div>
    </section>
  );
}

function PlanCard({
  name,
  price,
  blurb,
  features,
  cta,
  highlight = false,
}: {
  name: string;
  price: string;
  blurb: string;
  features: string[];
  cta: { href: string; label: string };
  highlight?: boolean;
}) {
  const base = highlight
    ? 'bg-purple-600 text-white border-purple-600 md:scale-105'
    : 'bg-white text-gray-900 border-gray-200';
  const subtle = highlight ? 'text-purple-100' : 'text-gray-500';
  const ctaClass = highlight
    ? 'bg-white text-purple-700 hover:bg-gray-100'
    : 'bg-purple-600 text-white hover:bg-purple-700';
  return (
    <div className={`rounded-xl border-2 p-6 shadow-sm ${base}`}>
      {highlight && (
        <div className="mb-3 inline-block rounded-full bg-purple-900/40 px-2 py-0.5 text-xs font-bold uppercase tracking-wide">
          Hero SKU
        </div>
      )}
      <h3 className="text-xl font-bold">{name}</h3>
      <div className="mt-3 text-2xl font-bold">{price}</div>
      <div className={`mt-1 text-sm ${subtle}`}>{blurb}</div>
      <ul className={`mt-5 space-y-2 text-sm ${highlight ? 'text-purple-50' : 'text-gray-700'}`}>
        {features.map((f) => (
          <li key={f} className="flex items-start gap-2">
            <span className={highlight ? 'text-purple-200' : 'text-purple-600'}>✓</span>
            <span>{f}</span>
          </li>
        ))}
      </ul>
      <Link
        href={cta.href}
        className={`mt-6 block rounded-md px-4 py-2 text-center font-semibold transition ${ctaClass}`}
      >
        {cta.label}
      </Link>
    </div>
  );
}

/* ─── Comparison ────────────────────────────────────────────────────────── */

function Comparison() {
  const rows: { label: string; values: [boolean | string, boolean | string, boolean | string, boolean | string] }[] = [
    { label: 'Fully self-hosted install', values: [true, true, 'K8s only', false] },
    { label: 'Air-gap friendly', values: [true, true, false, false] },
    { label: 'Open source core', values: [true, true, false, false] },
    { label: 'AI-assisted PL/SQL conversion', values: [true, false, 'Limited', false] },
    { label: 'Bring your own AI key (BYOK)', values: [true, false, false, false] },
    { label: 'Merkle row verification', values: [true, false, false, false] },
    { label: 'Target-neutral (any Postgres)', values: [true, true, 'EDB only', 'AWS only'] },
    { label: 'Transparent pricing', values: [true, 'free', 'free', 'AWS metering'] },
  ];
  const cols = ['depart', 'Ora2Pg', 'EDB Portal', 'AWS SCT'];
  return (
    <section className="py-20">
      <div className="container mx-auto max-w-5xl px-4">
        <h2 className="text-center text-3xl font-bold text-gray-900 md:text-4xl">
          How depart compares.
        </h2>
        <div className="mt-10 overflow-x-auto">
          <table className="w-full border-collapse text-sm">
            <thead>
              <tr className="border-b-2 border-gray-200">
                <th className="p-3 text-left font-semibold text-gray-700"></th>
                {cols.map((c, i) => (
                  <th
                    key={c}
                    className={`p-3 font-semibold ${
                      i === 0 ? 'text-purple-700' : 'text-gray-700'
                    }`}
                  >
                    {c}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {rows.map((r) => (
                <tr key={r.label} className="border-b border-gray-100">
                  <td className="p-3 font-medium text-gray-900">{r.label}</td>
                  {r.values.map((v, i) => (
                    <td
                      key={i}
                      className={`p-3 text-center ${i === 0 ? 'bg-purple-50' : ''}`}
                    >
                      {typeof v === 'boolean' ? (
                        v ? <span className="text-green-600">✓</span> : <span className="text-gray-300">—</span>
                      ) : (
                        <span className="text-sm text-gray-600">{v}</span>
                      )}
                    </td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </section>
  );
}

/* ─── FAQ ───────────────────────────────────────────────────────────────── */

function FAQ() {
  const items = [
    {
      q: 'Will Oracle sue us for using this?',
      a: "No. Reading your own Oracle schema via ALL_* views is something your DBA does every day. We don't decompile Oracle binaries, bypass licensing, or ship any Oracle IP.",
    },
    {
      q: 'How is this different from Ora2Pg?',
      a: "Ora2Pg is a Perl script that dumps schema. We have a parser that builds an IR, an AI that fixes what the parser flags, a runner that verifies every row, and a dashboard you can hand to your CTO. We also don't have a 4-billion-row table limit.",
    },
    {
      q: 'Do we need a new Postgres SKU from EDB?',
      a: 'No. depart targets any Postgres 13+. Plain RDS, Aurora, CloudSQL, Crunchy, Supabase, self-hosted — all equivalent.',
    },
    {
      q: 'Is AI conversion actually safe for production code?',
      a: 'Every generated conversion is shown as a diff against the original with reasoning. You approve each change. Nothing runs unreviewed. The AI call goes from your server to Anthropic (or whichever provider you choose) using your API key — we are not in that path.',
    },
    {
      q: 'Does depart phone home? Can it run fully air-gapped?',
      a: 'No phone-home. The product image runs entirely inside your network; license verification is offline (signed JWT checked locally). Air-gap installs work — we ship a separate installer bundle with all dependencies vendored.',
    },
    {
      q: "What if we hit a construct you don't handle?",
      a: 'The parser falls back to a permissive scanner and logs the unsupported construct. The runbook flags it as "manual." You can also open an issue — our ANTLR grammar is open source, and we merge grammar PRs from the community.',
    },
    {
      q: 'How does the license check work offline?',
      a: 'When you buy a Pro license we send you a signed JWT with the project ID, seat count, and expiry. depart verifies the signature against our public key bundled with the image. No network call. If your license expires, AI conversion and PDF generation stop working — the OSS core (parser, runner) keeps running forever.',
    },
    {
      q: 'Can we start with a free install and upgrade later?',
      a: "Yes. Run the Community tier forever if you only need assessment + the open-source runner. Drop a Pro license file into the install directory when you're ready for AI conversion and the runbook PDF — no reinstall needed.",
    },
  ];
  return (
    <section className="bg-gray-50 py-20">
      <div className="container mx-auto max-w-3xl px-4">
        <h2 className="text-center text-3xl font-bold text-gray-900 md:text-4xl">FAQ</h2>
        <div className="mt-10 space-y-4">
          {items.map((it) => (
            <details
              key={it.q}
              className="group rounded-lg border border-gray-200 bg-white p-5 open:shadow-sm"
            >
              <summary className="cursor-pointer list-none font-semibold text-gray-900 group-open:text-purple-700">
                {it.q}
              </summary>
              <p className="mt-3 text-sm text-gray-700">{it.a}</p>
            </details>
          ))}
        </div>
      </div>
    </section>
  );
}

/* ─── Final CTA ─────────────────────────────────────────────────────────── */

function FinalCTA() {
  return (
    <section className="bg-gradient-to-br from-purple-700 to-blue-700 py-20 text-white">
      <div className="container mx-auto max-w-3xl px-4 text-center">
        <h2 className="text-3xl font-bold md:text-4xl">
          Stop paying Oracle. Start this afternoon.
        </h2>
        <p className="mx-auto mt-4 max-w-xl text-purple-100">
          Two commands. The whole platform runs on your laptop or a VM in your VPC.
        </p>
        <pre className="mx-auto mt-8 max-w-xl rounded-lg bg-black/30 p-4 text-left font-mono text-sm">
{`git clone https://github.com/davidnhg74/depart
cd depart && docker compose up`}
        </pre>
        <div className="mt-8 flex flex-wrap justify-center gap-4">
          <Link
            href="/download"
            className="inline-block rounded-lg bg-white px-8 py-4 font-bold text-purple-700 shadow-lg transition hover:bg-gray-100"
          >
            Download →
          </Link>
          <Link
            href="/assess"
            className="inline-block rounded-lg border border-white/30 bg-white/10 px-8 py-4 font-bold text-white transition hover:bg-white/20"
          >
            Try the online demo
          </Link>
        </div>
      </div>
    </section>
  );
}

/* ─── Footer ────────────────────────────────────────────────────────────── */

function Footer() {
  return (
    <footer className="bg-gray-900 py-12 text-gray-400">
      <div className="container mx-auto px-4">
        <div className="grid grid-cols-2 gap-8 md:grid-cols-4">
          <div>
            <h4 className="mb-3 font-bold text-white">Product</h4>
            <ul className="space-y-2 text-sm">
              <li>
                <Link href="/assess" className="hover:text-white">
                  Assess
                </Link>
              </li>
              <li>
                <Link href="/features" className="hover:text-white">
                  Features
                </Link>
              </li>
              <li>
                <Link href="#pricing" className="hover:text-white">
                  Pricing
                </Link>
              </li>
            </ul>
          </div>
          <div>
            <h4 className="mb-3 font-bold text-white">Open source</h4>
            <ul className="space-y-2 text-sm">
              <li>
                <a href="https://github.com/davidnhg74/depart" className="hover:text-white">
                  GitHub
                </a>
              </li>
              <li>
                <a href="https://github.com/davidnhg74/depart/blob/main/CONTRIBUTING.md" className="hover:text-white">
                  Contribute
                </a>
              </li>
            </ul>
          </div>
          <div>
            <h4 className="mb-3 font-bold text-white">Company</h4>
            <ul className="space-y-2 text-sm">
              <li>
                <Link href="/contact" className="hover:text-white">
                  Contact
                </Link>
              </li>
            </ul>
          </div>
          <div>
            <h4 className="mb-3 font-bold text-white">Legal</h4>
            <ul className="space-y-2 text-sm">
              <li>
                <Link href="/privacy" className="hover:text-white">
                  Privacy
                </Link>
              </li>
              <li>
                <Link href="/terms" className="hover:text-white">
                  Terms
                </Link>
              </li>
            </ul>
          </div>
        </div>
        <div className="mt-10 border-t border-gray-800 pt-6 text-center text-sm">
          depart — open-source at{' '}
          <a href="https://github.com/davidnhg74/depart" className="hover:text-white">
            github.com/davidnhg74/depart
          </a>
          . Built by a team that&apos;s done this migration before.
        </div>
      </div>
    </footer>
  );
}

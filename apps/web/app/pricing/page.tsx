import Link from 'next/link';

export default function PricingPage() {
  return (
    <div className="w-full">
      {/* Hero */}
      <section className="py-20 bg-gradient-to-br from-purple-900 to-blue-900 text-white">
        <div className="container mx-auto px-4 text-center">
          <h1 className="text-5xl font-bold mb-6">Pricing that matches how you buy.</h1>
          <p className="text-xl text-purple-100 max-w-3xl mx-auto">
            Migrations are capex events and enterprises want to own their tools.
            Hafen ships as signed license files that run inside your VPC — offline,
            no subscription, no phone-home, no vendor lock-in.
          </p>
        </div>
      </section>

      {/* Pricing Cards */}
      <section className="py-20">
        <div className="container mx-auto px-4">
          <div className="grid grid-cols-1 md:grid-cols-4 gap-4 max-w-7xl mx-auto">
            {/* OSS Core */}
            <div className="bg-white border-2 border-gray-200 rounded-lg p-8 flex flex-col">
              <h3 className="text-2xl font-bold text-gray-900">OSS Core</h3>
              <div className="text-3xl font-bold text-purple-600 my-4">Free</div>
              <p className="text-gray-600 mb-6">Parser + runner + CLI. MIT.</p>
              <ul className="space-y-3 mb-8 text-gray-700 text-sm flex-grow">
                <li className="flex items-start gap-3">
                  <span className="text-purple-600 font-bold flex-shrink-0 mt-0.5">✓</span>
                  <span>ANTLR parser + intermediate representation</span>
                </li>
                <li className="flex items-start gap-3">
                  <span className="text-purple-600 font-bold flex-shrink-0 mt-0.5">✓</span>
                  <span>Data-movement runner with checkpoint/resume</span>
                </li>
                <li className="flex items-start gap-3">
                  <span className="text-purple-600 font-bold flex-shrink-0 mt-0.5">✓</span>
                  <span>Merkle verification of every copied table</span>
                </li>
                <li className="flex items-start gap-3">
                  <span className="text-purple-600 font-bold flex-shrink-0 mt-0.5">✓</span>
                  <span>CLI + Docker image</span>
                </li>
              </ul>
              <a
                href="https://github.com/davidnhg74/hafen"
                className="block w-full px-4 py-3 border-2 border-purple-600 text-purple-600 font-bold text-center rounded-lg hover:bg-purple-50"
              >
                View on GitHub
              </a>
            </div>

            {/* Community */}
            <div className="bg-white border-2 border-gray-200 rounded-lg p-8 flex flex-col">
              <h3 className="text-2xl font-bold text-gray-900">Community</h3>
              <div className="text-3xl font-bold text-purple-600 my-4">Free</div>
              <p className="text-gray-600 mb-6">Full self-hosted install. No license.</p>
              <ul className="space-y-3 mb-8 text-gray-700 text-sm flex-grow">
                <li className="flex items-start gap-3">
                  <span className="text-purple-600 font-bold flex-shrink-0 mt-0.5">✓</span>
                  <span>Everything in OSS Core</span>
                </li>
                <li className="flex items-start gap-3">
                  <span className="text-purple-600 font-bold flex-shrink-0 mt-0.5">✓</span>
                  <span>Web UI, deployed in your VPC</span>
                </li>
                <li className="flex items-start gap-3">
                  <span className="text-purple-600 font-bold flex-shrink-0 mt-0.5">✓</span>
                  <span>Assessment + risk scoring</span>
                </li>
                <li className="flex items-start gap-3">
                  <span className="text-purple-600 font-bold flex-shrink-0 mt-0.5">✓</span>
                  <span>Canonical AI conversion examples</span>
                </li>
              </ul>
              <Link
                href="/download"
                className="block w-full px-4 py-3 border-2 border-purple-600 text-purple-600 font-bold text-center rounded-lg hover:bg-purple-50"
              >
                Download →
              </Link>
            </div>

            {/* Pro — highlighted */}
            <div className="bg-purple-600 text-white border-2 border-purple-600 rounded-lg p-8 flex flex-col shadow-lg md:scale-105">
              <h3 className="text-2xl font-bold">Pro</h3>
              <div className="text-3xl font-bold my-4">$75k–$150k<span className="text-lg font-normal opacity-80">/yr</span></div>
              <p className="text-purple-100 mb-6">Annual site license. Unlimited projects. Offline.</p>
              <ul className="space-y-3 mb-8 text-sm flex-grow">
                <li className="flex items-start gap-3">
                  <span className="font-bold flex-shrink-0 mt-0.5">✓</span>
                  <span>Everything in Community</span>
                </li>
                <li className="flex items-start gap-3">
                  <span className="font-bold flex-shrink-0 mt-0.5">✓</span>
                  <span>AI conversion on your actual code (BYOK)</span>
                </li>
                <li className="flex items-start gap-3">
                  <span className="font-bold flex-shrink-0 mt-0.5">✓</span>
                  <span>Runbook PDF generator</span>
                </li>
                <li className="flex items-start gap-3">
                  <span className="font-bold flex-shrink-0 mt-0.5">✓</span>
                  <span>Scheduled + recurring migrations</span>
                </li>
                <li className="flex items-start gap-3">
                  <span className="font-bold flex-shrink-0 mt-0.5">✓</span>
                  <span>Webhook notifications (Slack/HTTP)</span>
                </li>
                <li className="flex items-start gap-3">
                  <span className="font-bold flex-shrink-0 mt-0.5">✓</span>
                  <span>Per-column PII masking</span>
                </li>
                <li className="flex items-start gap-3">
                  <span className="font-bold flex-shrink-0 mt-0.5">✓</span>
                  <span>Priority grammar fixes + email support</span>
                </li>
              </ul>
              <Link
                href="/contact"
                className="block w-full px-4 py-3 bg-white text-purple-600 font-bold text-center rounded-lg hover:bg-purple-50"
              >
                Buy a license
              </Link>
            </div>

            {/* Enterprise */}
            <div className="bg-white border-2 border-gray-200 rounded-lg p-8 flex flex-col">
              <h3 className="text-2xl font-bold text-gray-900">Enterprise</h3>
              <div className="text-3xl font-bold text-purple-600 my-4">Custom</div>
              <p className="text-gray-600 mb-6">Site license + air-gap install.</p>
              <ul className="space-y-3 mb-8 text-gray-700 text-sm flex-grow">
                <li className="flex items-start gap-3">
                  <span className="text-purple-600 font-bold flex-shrink-0 mt-0.5">✓</span>
                  <span>Everything in Pro</span>
                </li>
                <li className="flex items-start gap-3">
                  <span className="text-purple-600 font-bold flex-shrink-0 mt-0.5">✓</span>
                  <span>Multi-project, unlimited migrations</span>
                </li>
                <li className="flex items-start gap-3">
                  <span className="text-purple-600 font-bold flex-shrink-0 mt-0.5">✓</span>
                  <span>Air-gap installer + SSO (OIDC/SAML)</span>
                </li>
                <li className="flex items-start gap-3">
                  <span className="text-purple-600 font-bold flex-shrink-0 mt-0.5">✓</span>
                  <span>Audit trail with hash-chain verification</span>
                </li>
                <li className="flex items-start gap-3">
                  <span className="text-purple-600 font-bold flex-shrink-0 mt-0.5">✓</span>
                  <span>Dedicated support + SLA</span>
                </li>
              </ul>
              <Link
                href="/contact"
                className="block w-full px-4 py-3 border-2 border-purple-600 text-purple-600 font-bold text-center rounded-lg hover:bg-purple-50"
              >
                Contact sales
              </Link>
            </div>
          </div>

          <p className="mx-auto mt-12 max-w-3xl text-center text-sm text-gray-500">
            Pro and Enterprise ship as signed offline license files (JWT). Verified locally
            against a bundled public key — no network check required. Annual site licenses
            cover unlimited projects across your organization. A typical migration recoups
            its annual license cost in avoided Oracle support fees within the first quarter.
          </p>
        </div>
      </section>

      {/* FAQ */}
      <section className="py-16 bg-gray-50">
        <div className="container mx-auto max-w-3xl px-4">
          <h2 className="text-3xl font-bold text-gray-900 text-center mb-10">
            Frequently asked
          </h2>
          <div className="space-y-6">
            <div>
              <h3 className="text-lg font-bold text-gray-900">
                Why per-project, not per-month?
              </h3>
              <p className="mt-2 text-gray-700">
                Migrations are one-time projects, not ongoing services. Buying a SaaS
                subscription for something you&apos;ll finish in 90 days wastes money and
                creates an awkward procurement story (&ldquo;we still pay for the migration
                tool we finished using&rdquo;). Pay once for the term you need.
              </p>
            </div>
            <div>
              <h3 className="text-lg font-bold text-gray-900">
                What happens when the 90-day term ends?
              </h3>
              <p className="mt-2 text-gray-700">
                The Pro features disable; Community features (parser, runner, CLI, web UI,
                assessment) keep working indefinitely. Migrations you already ran are done
                — the license term is about Pro features during the active project.
                Extensions are straightforward if you need them.
              </p>
            </div>
            <div>
              <h3 className="text-lg font-bold text-gray-900">
                Is there really no phone-home?
              </h3>
              <p className="mt-2 text-gray-700">
                Correct. License verification is 100% offline. The JWT is signed with our
                private key; your install verifies it against a public key bundled in the
                image. We never see your DDL, your DSNs, or that you&apos;re running. That
                design is why regulated customers (finance, healthcare, government)
                approve us.
              </p>
            </div>
            <div>
              <h3 className="text-lg font-bold text-gray-900">
                What databases does it support?
              </h3>
              <p className="mt-2 text-gray-700">
                Oracle → PostgreSQL today. SQL Server, DB2, and Snowflake sources are on
                the roadmap. If you have a specific source and a real project,{' '}
                <Link href="/contact" className="text-purple-600 underline">
                  talk to us
                </Link>
                {' '}— we prioritize based on committed projects.
              </p>
            </div>
          </div>
        </div>
      </section>

      {/* CTA */}
      <section className="py-16 bg-gradient-to-br from-purple-900 to-blue-900 text-white">
        <div className="container mx-auto max-w-4xl px-4 text-center">
          <h2 className="text-3xl font-bold mb-4">
            Start free. Buy when the ROI shows up.
          </h2>
          <p className="text-purple-100 mb-8">
            Download Community, run an assessment on your real schema, and see what the
            conversion looks like before you ever pay us.
          </p>
          <div className="flex flex-col sm:flex-row gap-4 justify-center">
            <Link
              href="/download"
              className="px-8 py-3 bg-white text-purple-600 font-bold rounded-lg hover:bg-purple-50"
            >
              Download Community
            </Link>
            <Link
              href="/contact"
              className="px-8 py-3 border-2 border-white font-bold rounded-lg hover:bg-white/10"
            >
              Talk to sales
            </Link>
          </div>
        </div>
      </section>
    </div>
  );
}

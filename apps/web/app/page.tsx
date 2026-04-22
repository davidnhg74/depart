import Link from 'next/link';

export default function LandingPage() {
  return (
    <div className="w-full">
      {/* Hero Section */}
      <section className="min-h-screen bg-gradient-to-br from-purple-900 via-purple-800 to-blue-900 text-white flex items-center">
        <div className="container mx-auto px-4 py-20">
          <div className="max-w-3xl mx-auto text-center">
            <h1 className="text-5xl md:text-6xl font-bold mb-6">
              Escape Oracle Licensing with AI-Powered Migration
            </h1>
            <p className="text-xl md:text-2xl text-purple-100 mb-8">
              Automatically convert Oracle PL/SQL to PostgreSQL. Replace $50k+ in consulting with an intelligent migration engine.
            </p>
            <div className="flex gap-4 justify-center flex-wrap">
              <Link
                href="/signup"
                className="px-8 py-4 bg-purple-500 hover:bg-purple-600 text-white font-bold rounded-lg transition"
              >
                Start Free Trial (14 days)
              </Link>
              <Link
                href="/features"
                className="px-8 py-4 bg-white/20 hover:bg-white/30 text-white font-bold rounded-lg border border-white/30 transition"
              >
                Learn More
              </Link>
            </div>

            {/* Stats */}
            <div className="grid grid-cols-3 gap-8 mt-16">
              <div>
                <div className="text-4xl font-bold">100x</div>
                <div className="text-purple-200">Cheaper than Informatica</div>
              </div>
              <div>
                <div className="text-4xl font-bold">200+</div>
                <div className="text-purple-200">Hours saved per migration</div>
              </div>
              <div>
                <div className="text-4xl font-bold">$50k+</div>
                <div className="text-purple-200">Consulting costs replaced</div>
              </div>
            </div>
          </div>
        </div>
      </section>

      {/* Problem Section */}
      <section className="py-20 bg-gray-50">
        <div className="container mx-auto px-4">
          <div className="max-w-3xl mx-auto">
            <h2 className="text-4xl font-bold text-gray-900 text-center mb-12">The Oracle Migration Problem</h2>

            <div className="grid grid-cols-1 md:grid-cols-2 gap-8">
              <div className="bg-red-50 p-8 rounded-lg border border-red-200">
                <div className="text-3xl mb-4">💰</div>
                <h3 className="text-xl font-bold text-gray-900 mb-2">Expensive Consulting</h3>
                <p className="text-gray-700">Database migration consulting costs $150-300/hr, totaling $50k-600k+ per migration.</p>
              </div>

              <div className="bg-red-50 p-8 rounded-lg border border-red-200">
                <div className="text-3xl mb-4">⏰</div>
                <h3 className="text-xl font-bold text-gray-900 mb-2">Time-Consuming</h3>
                <p className="text-gray-700">Manual PL/SQL conversion takes 200-500 hours. Mistakes in conversion cause runtime failures.</p>
              </div>

              <div className="bg-red-50 p-8 rounded-lg border border-red-200">
                <div className="text-3xl mb-4">🔴</div>
                <h3 className="text-xl font-bold text-gray-900 mb-2">Oracle Lock-in</h3>
                <p className="text-gray-700">Oracle licensing costs $10k-50k/year. Legacy PL/SQL code keeps you trapped.</p>
              </div>

              <div className="bg-red-50 p-8 rounded-lg border border-red-200">
                <div className="text-3xl mb-4">🐛</div>
                <h3 className="text-xl font-bold text-gray-900 mb-2">Hidden Complexity</h3>
                <p className="text-gray-700">Semantic differences between Oracle and PostgreSQL cause bugs in production.</p>
              </div>
            </div>
          </div>
        </div>
      </section>

      {/* Solution Section */}
      <section className="py-20">
        <div className="container mx-auto px-4">
          <div className="max-w-3xl mx-auto">
            <h2 className="text-4xl font-bold text-gray-900 text-center mb-12">Meet Depart: Your AI Migration Assistant</h2>

            <div className="space-y-8">
              <div className="flex gap-6">
                <div className="flex-shrink-0">
                  <div className="flex items-center justify-center h-12 w-12 rounded-md bg-purple-600 text-white">1</div>
                </div>
                <div>
                  <h3 className="text-xl font-bold text-gray-900">Connect Your Databases</h3>
                  <p className="text-gray-700 mt-2">Input your Oracle and PostgreSQL connection details. We validate both connections before analyzing.</p>
                </div>
              </div>

              <div className="flex gap-6">
                <div className="flex-shrink-0">
                  <div className="flex items-center justify-center h-12 w-12 rounded-md bg-purple-600 text-white">2</div>
                </div>
                <div>
                  <h3 className="text-xl font-bold text-gray-900">AI-Powered Analysis</h3>
                  <p className="text-gray-700 mt-2">Our neural network analyzes your Oracle schema, identifies complexity, and flags conversion risks.</p>
                </div>
              </div>

              <div className="flex gap-6">
                <div className="flex-shrink-0">
                  <div className="flex items-center justify-center h-12 w-12 rounded-md bg-purple-600 text-white">3</div>
                </div>
                <div>
                  <h3 className="text-xl font-bold text-gray-900">Automatic Conversion</h3>
                  <p className="text-gray-700 mt-2">Convert PL/SQL procedures, functions, and triggers to PostgreSQL with semantic accuracy.</p>
                </div>
              </div>

              <div className="flex gap-6">
                <div className="flex-shrink-0">
                  <div className="flex items-center justify-center h-12 w-12 rounded-md bg-purple-600 text-white">4</div>
                </div>
                <div>
                  <h3 className="text-xl font-bold text-gray-900">Migration Workflow</h3>
                  <p className="text-gray-700 mt-2">DBA-guided migration planning with benchmarking, testing, and safe cutover.</p>
                </div>
              </div>
            </div>
          </div>
        </div>
      </section>

      {/* Features Section */}
      <section className="py-20 bg-gray-50">
        <div className="container mx-auto px-4">
          <h2 className="text-4xl font-bold text-gray-900 text-center mb-12">Powerful Features</h2>

          <div className="grid grid-cols-1 md:grid-cols-3 gap-8">
            <div className="bg-white p-8 rounded-lg shadow">
              <div className="text-4xl mb-4">🔍</div>
              <h3 className="text-xl font-bold text-gray-900 mb-2">Complexity Analysis</h3>
              <p className="text-gray-700">Identify risky patterns, dead code, and migration blockers before conversion.</p>
            </div>

            <div className="bg-white p-8 rounded-lg shadow">
              <div className="text-4xl mb-4">🤖</div>
              <h3 className="text-xl font-bold text-gray-900 mb-2">Neural Code Conversion</h3>
              <p className="text-gray-700">LLM-powered conversion that understands PL/SQL semantics and PostgreSQL idioms.</p>
            </div>

            <div className="bg-white p-8 rounded-lg shadow">
              <div className="text-4xl mb-4">📊</div>
              <h3 className="text-xl font-bold text-gray-900 mb-2">DBA Workflow</h3>
              <p className="text-gray-700">Multi-phase migration planning with approvals, testing, and rollback plans.</p>
            </div>

            <div className="bg-white p-8 rounded-lg shadow">
              <div className="text-4xl mb-4">✅</div>
              <h3 className="text-xl font-bold text-gray-900 mb-2">Semantic Validation</h3>
              <p className="text-gray-700">Test converted code against both databases to catch differences early.</p>
            </div>

            <div className="bg-white p-8 rounded-lg shadow">
              <div className="text-4xl mb-4">📈</div>
              <h3 className="text-xl font-bold text-gray-900 mb-2">Performance Benchmarking</h3>
              <p className="text-gray-700">Capture Oracle baseline metrics and compare PostgreSQL performance pre-cutover.</p>
            </div>

            <div className="bg-white p-8 rounded-lg shadow">
              <div className="text-4xl mb-4">🔑</div>
              <h3 className="text-xl font-bold text-gray-900 mb-2">API Keys for CI/CD</h3>
              <p className="text-gray-700">Integrate migrations into your deployment pipeline with secure API access.</p>
            </div>
          </div>
        </div>
      </section>

      {/* Pricing Section */}
      <section className="py-20">
        <div className="container mx-auto px-4">
          <h2 className="text-4xl font-bold text-gray-900 text-center mb-4">Simple, Transparent Pricing</h2>
          <p className="text-xl text-gray-600 text-center mb-12">Start free. Scale as you grow.</p>

          <div className="grid grid-cols-1 md:grid-cols-4 gap-6 max-w-5xl mx-auto">
            {/* Trial */}
            <div className="bg-white border-2 border-gray-200 rounded-lg p-8">
              <h3 className="text-2xl font-bold text-gray-900">Free Trial</h3>
              <div className="text-3xl font-bold text-purple-600 my-4">$0</div>
              <p className="text-gray-600 mb-6">14 days, no credit card</p>
              <ul className="space-y-3 mb-8 text-gray-700 text-sm">
                <li className="flex items-center gap-2">
                  <span className="text-purple-600">✓</span> 1 database
                </li>
                <li className="flex items-center gap-2">
                  <span className="text-purple-600">✓</span> 3 migrations/month
                </li>
                <li className="flex items-center gap-2">
                  <span className="text-purple-600">✓</span> 10 LLM conversions
                </li>
              </ul>
              <Link
                href="/signup"
                className="block w-full px-4 py-3 bg-purple-600 text-white font-bold text-center rounded-lg hover:bg-purple-700"
              >
                Start Trial
              </Link>
            </div>

            {/* Starter */}
            <div className="bg-white border-2 border-gray-200 rounded-lg p-8">
              <h3 className="text-2xl font-bold text-gray-900">Starter</h3>
              <div className="text-3xl font-bold text-purple-600 my-4">$249</div>
              <p className="text-gray-600 mb-6">/month</p>
              <ul className="space-y-3 mb-8 text-gray-700 text-sm">
                <li className="flex items-center gap-2">
                  <span className="text-purple-600">✓</span> 5 databases
                </li>
                <li className="flex items-center gap-2">
                  <span className="text-purple-600">✓</span> 25 migrations/month
                </li>
                <li className="flex items-center gap-2">
                  <span className="text-purple-600">✓</span> 100 LLM conversions
                </li>
                <li className="flex items-center gap-2">
                  <span className="text-purple-600">✓</span> Email support
                </li>
              </ul>
              <Link
                href="/signup"
                className="block w-full px-4 py-3 bg-gray-200 text-gray-900 font-bold text-center rounded-lg hover:bg-gray-300"
              >
                Get Started
              </Link>
            </div>

            {/* Professional */}
            <div className="bg-purple-600 text-white rounded-lg p-8 md:scale-105">
              <div className="bg-purple-700 text-white text-xs font-bold px-3 py-1 rounded-full inline-block mb-4">
                MOST POPULAR
              </div>
              <h3 className="text-2xl font-bold">Professional</h3>
              <div className="text-3xl font-bold my-4">$599</div>
              <p className="text-purple-100 mb-6">/month</p>
              <ul className="space-y-3 mb-8 text-purple-100 text-sm">
                <li className="flex items-center gap-2">
                  <span className="text-purple-300">✓</span> 20 databases
                </li>
                <li className="flex items-center gap-2">
                  <span className="text-purple-300">✓</span> 100 migrations/month
                </li>
                <li className="flex items-center gap-2">
                  <span className="text-purple-300">✓</span> 500 LLM conversions
                </li>
                <li className="flex items-center gap-2">
                  <span className="text-purple-300">✓</span> Priority email + Slack
                </li>
              </ul>
              <Link
                href="/signup"
                className="block w-full px-4 py-3 bg-white text-purple-600 font-bold text-center rounded-lg hover:bg-gray-100"
              >
                Start Free Trial
              </Link>
            </div>

            {/* Enterprise */}
            <div className="bg-white border-2 border-gray-200 rounded-lg p-8">
              <h3 className="text-2xl font-bold text-gray-900">Enterprise</h3>
              <div className="text-3xl font-bold text-purple-600 my-4">Custom</div>
              <p className="text-gray-600 mb-6">Contact us</p>
              <ul className="space-y-3 mb-8 text-gray-700 text-sm">
                <li className="flex items-center gap-2">
                  <span className="text-purple-600">✓</span> 100+ databases
                </li>
                <li className="flex items-center gap-2">
                  <span className="text-purple-600">✓</span> Unlimited migrations
                </li>
                <li className="flex items-center gap-2">
                  <span className="text-purple-600">✓</span> Unlimited conversions
                </li>
                <li className="flex items-center gap-2">
                  <span className="text-purple-600">✓</span> Dedicated CSM + SLA
                </li>
              </ul>
              <Link
                href="/contact"
                className="block w-full px-4 py-3 bg-gray-200 text-gray-900 font-bold text-center rounded-lg hover:bg-gray-300"
              >
                Contact Sales
              </Link>
            </div>
          </div>
        </div>
      </section>

      {/* CTA Section */}
      <section className="py-20 bg-gradient-to-br from-purple-600 to-blue-600 text-white">
        <div className="container mx-auto px-4 text-center">
          <h2 className="text-4xl font-bold mb-6">Ready to escape Oracle?</h2>
          <p className="text-xl text-purple-100 mb-8 max-w-2xl mx-auto">
            Start your free 14-day trial today. No credit card required. Convert your first schema in minutes.
          </p>
          <Link
            href="/signup"
            className="inline-block px-8 py-4 bg-white text-purple-600 font-bold rounded-lg hover:bg-gray-100 transition"
          >
            Start Your Free Trial
          </Link>
        </div>
      </section>

      {/* Footer */}
      <footer className="bg-gray-900 text-gray-400 py-12">
        <div className="container mx-auto px-4">
          <div className="grid grid-cols-4 gap-8 mb-8">
            <div>
              <h4 className="text-white font-bold mb-4">Product</h4>
              <ul className="space-y-2 text-sm">
                <li>
                  <Link href="/features" className="hover:text-white">
                    Features
                  </Link>
                </li>
                <li>
                  <Link href="/pricing" className="hover:text-white">
                    Pricing
                  </Link>
                </li>
              </ul>
            </div>
            <div>
              <h4 className="text-white font-bold mb-4">Company</h4>
              <ul className="space-y-2 text-sm">
                <li>
                  <Link href="/contact" className="hover:text-white">
                    Contact
                  </Link>
                </li>
              </ul>
            </div>
            <div>
              <h4 className="text-white font-bold mb-4">Legal</h4>
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
            <div>
              <h4 className="text-white font-bold mb-4">Built by</h4>
              <p className="text-sm">DBAs escaping Oracle</p>
            </div>
          </div>

          <div className="border-t border-gray-800 pt-8 text-center text-sm">
            <p>© 2024 Depart. All rights reserved.</p>
            <p className="mt-2">
              AI-powered Oracle to PostgreSQL migration. Built by Oracle DBAs, for teams migrating off legacy systems.
            </p>
          </div>
        </div>
      </footer>
    </div>
  );
}

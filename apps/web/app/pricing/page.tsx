import Link from 'next/link';

export default function PricingPage() {
  return (
    <div className="w-full">
      {/* Hero */}
      <section className="py-20 bg-gradient-to-br from-purple-900 to-blue-900 text-white">
        <div className="container mx-auto px-4 text-center">
          <h1 className="text-5xl font-bold mb-6">Simple, Transparent Pricing</h1>
          <p className="text-xl text-purple-100">
            Start free. Scale as you grow. Cancel anytime.
          </p>
        </div>
      </section>

      {/* Pricing Cards */}
      <section className="py-20">
        <div className="container mx-auto px-4">
          <div className="grid grid-cols-1 md:grid-cols-4 gap-6 max-w-6xl mx-auto">
            {/* Trial */}
            <div className="bg-white border-2 border-gray-200 rounded-lg p-8">
              <h3 className="text-2xl font-bold text-gray-900">Free Trial</h3>
              <div className="text-3xl font-bold text-purple-600 my-4">$0</div>
              <p className="text-gray-600 mb-6">14 days, no credit card</p>
              <ul className="space-y-4 mb-8 text-gray-700 text-sm">
                <li className="flex items-start gap-3">
                  <span className="text-purple-600 font-bold flex-shrink-0 mt-0.5">✓</span>
                  <span>1 database</span>
                </li>
                <li className="flex items-start gap-3">
                  <span className="text-purple-600 font-bold flex-shrink-0 mt-0.5">✓</span>
                  <span>3 migrations/month</span>
                </li>
                <li className="flex items-start gap-3">
                  <span className="text-purple-600 font-bold flex-shrink-0 mt-0.5">✓</span>
                  <span>10 LLM conversions</span>
                </li>
                <li className="flex items-start gap-3">
                  <span className="text-purple-600 font-bold flex-shrink-0 mt-0.5">✓</span>
                  <span>Community support</span>
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
              <p className="text-gray-600 mb-6">/month, billed annually</p>
              <ul className="space-y-4 mb-8 text-gray-700 text-sm">
                <li className="flex items-start gap-3">
                  <span className="text-purple-600 font-bold flex-shrink-0 mt-0.5">✓</span>
                  <span>5 databases</span>
                </li>
                <li className="flex items-start gap-3">
                  <span className="text-purple-600 font-bold flex-shrink-0 mt-0.5">✓</span>
                  <span>25 migrations/month</span>
                </li>
                <li className="flex items-start gap-3">
                  <span className="text-purple-600 font-bold flex-shrink-0 mt-0.5">✓</span>
                  <span>100 LLM conversions</span>
                </li>
                <li className="flex items-start gap-3">
                  <span className="text-purple-600 font-bold flex-shrink-0 mt-0.5">✓</span>
                  <span>Email support</span>
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
            <div className="bg-purple-600 text-white rounded-lg p-8 shadow-xl">
              <div className="bg-purple-700 text-white text-xs font-bold px-3 py-1 rounded-full inline-block mb-4">
                MOST POPULAR
              </div>
              <h3 className="text-2xl font-bold">Professional</h3>
              <div className="text-3xl font-bold my-4">$599</div>
              <p className="text-purple-100 mb-6">/month, billed annually</p>
              <ul className="space-y-4 mb-8 text-purple-100 text-sm">
                <li className="flex items-start gap-3">
                  <span className="text-purple-300 font-bold flex-shrink-0 mt-0.5">✓</span>
                  <span>20 databases</span>
                </li>
                <li className="flex items-start gap-3">
                  <span className="text-purple-300 font-bold flex-shrink-0 mt-0.5">✓</span>
                  <span>100 migrations/month</span>
                </li>
                <li className="flex items-start gap-3">
                  <span className="text-purple-300 font-bold flex-shrink-0 mt-0.5">✓</span>
                  <span>500 LLM conversions</span>
                </li>
                <li className="flex items-start gap-3">
                  <span className="text-purple-300 font-bold flex-shrink-0 mt-0.5">✓</span>
                  <span>Priority email + Slack</span>
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
              <p className="text-gray-600 mb-6">Contact sales</p>
              <ul className="space-y-4 mb-8 text-gray-700 text-sm">
                <li className="flex items-start gap-3">
                  <span className="text-purple-600 font-bold flex-shrink-0 mt-0.5">✓</span>
                  <span>100+ databases</span>
                </li>
                <li className="flex items-start gap-3">
                  <span className="text-purple-600 font-bold flex-shrink-0 mt-0.5">✓</span>
                  <span>Unlimited migrations</span>
                </li>
                <li className="flex items-start gap-3">
                  <span className="text-purple-600 font-bold flex-shrink-0 mt-0.5">✓</span>
                  <span>Unlimited conversions</span>
                </li>
                <li className="flex items-start gap-3">
                  <span className="text-purple-600 font-bold flex-shrink-0 mt-0.5">✓</span>
                  <span>Dedicated CSM + SLA</span>
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

      {/* FAQ */}
      <section className="py-20 bg-gray-50">
        <div className="container mx-auto px-4 max-w-3xl">
          <h2 className="text-3xl font-bold text-gray-900 text-center mb-12">Frequently Asked Questions</h2>

          <div className="space-y-8">
            <div>
              <h3 className="text-xl font-bold text-gray-900 mb-2">Can I cancel anytime?</h3>
              <p className="text-gray-700">Yes. Cancel your subscription anytime without penalties. You'll lose access at the end of your billing period.</p>
            </div>

            <div>
              <h3 className="text-xl font-bold text-gray-900 mb-2">Do you offer monthly billing?</h3>
              <p className="text-gray-700">
                Annual billing gets you the best price. Monthly billing is available for Professional and Enterprise plans—contact sales.
              </p>
            </div>

            <div>
              <h3 className="text-xl font-bold text-gray-900 mb-2">What happens after my free trial expires?</h3>
              <p className="text-gray-700">
                After 14 days, your trial expires and you lose access. You can upgrade to a paid plan anytime to continue.
              </p>
            </div>

            <div>
              <h3 className="text-xl font-bold text-gray-900 mb-2">Can I change plans?</h3>
              <p className="text-gray-700">
                Yes. Upgrade or downgrade anytime from your billing dashboard. Changes take effect at the start of your next billing period.
              </p>
            </div>

            <div>
              <h3 className="text-xl font-bold text-gray-900 mb-2">Is my data secure?</h3>
              <p className="text-gray-700">
                All data is encrypted in transit and at rest. We never store your database credentials and use short-lived tokens for connections.
              </p>
            </div>
          </div>
        </div>
      </section>

      {/* CTA */}
      <section className="py-20 bg-purple-600 text-white">
        <div className="container mx-auto px-4 text-center">
          <h2 className="text-4xl font-bold mb-6">Start Your Free Trial Today</h2>
          <p className="text-xl text-purple-100 mb-8">14 days. No credit card. Full access.</p>
          <Link
            href="/signup"
            className="inline-block px-8 py-4 bg-white text-purple-600 font-bold rounded-lg hover:bg-gray-100 transition"
          >
            Start Free Trial
          </Link>
        </div>
      </section>
    </div>
  );
}

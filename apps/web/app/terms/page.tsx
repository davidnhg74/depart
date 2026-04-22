export default function TermsPage() {
  return (
    <div className="min-h-screen bg-gray-50 py-12 px-4">
      <div className="max-w-3xl mx-auto bg-white rounded-lg shadow p-8">
        <h1 className="text-4xl font-bold text-gray-900 mb-2">Terms of Service</h1>
        <p className="text-gray-600 mb-8">Last updated: April 2026</p>

        <div className="prose prose-lg max-w-none space-y-8 text-gray-700">
          <section>
            <h2 className="text-2xl font-bold text-gray-900 mb-4">1. Agreement to Terms</h2>
            <p>
              By accessing and using Depart's website and services, you accept and agree to be bound by the terms and provision
              of this agreement. If you do not agree to abide by the above, please do not use this service.
            </p>
          </section>

          <section>
            <h2 className="text-2xl font-bold text-gray-900 mb-4">2. Service Description</h2>
            <p>
              Depart provides an AI-powered platform for analyzing Oracle databases and converting PL/SQL code to PostgreSQL.
              Services are provided "as is" and "as available."
            </p>
          </section>

          <section>
            <h2 className="text-2xl font-bold text-gray-900 mb-4">3. User Accounts</h2>
            <p>To use our services, you must:</p>
            <ul className="list-disc list-inside space-y-2 ml-4">
              <li>Create an account with accurate, complete information</li>
              <li>Maintain the confidentiality of your password and account</li>
              <li>Accept responsibility for all activities under your account</li>
              <li>Be at least 18 years old</li>
              <li>Not impersonate or misrepresent your identity</li>
            </ul>
          </section>

          <section>
            <h2 className="text-2xl font-bold text-gray-900 mb-4">4. Free Trial and Billing</h2>
            <ul className="list-disc list-inside space-y-2 ml-4">
              <li>
                <strong>Free Trial:</strong> 14-day trial includes full platform access. Trial expires after 14 days and you lose
                access unless you upgrade.
              </li>
              <li>
                <strong>Billing:</strong> Paid subscriptions renew automatically. Cancel anytime from your account settings.
              </li>
              <li>
                <strong>Refunds:</strong> No refunds for partial months. All sales are final except where required by law.
              </li>
              <li>
                <strong>Price Changes:</strong> We may change prices with 30 days' notice. Changes apply to renewals, not current
                subscriptions.
              </li>
            </ul>
          </section>

          <section>
            <h2 className="text-2xl font-bold text-gray-900 mb-4">5. Usage Limits and Fair Use</h2>
            <p>Your plan includes specific limits on databases, migrations, and LLM conversions per month. We reserve the right to:</p>
            <ul className="list-disc list-inside space-y-2 ml-4">
              <li>Enforce usage limits and suspend service if exceeded</li>
              <li>Disable accounts used for abuse, spam, or illegal activity</li>
              <li>Rate-limit API requests to prevent service degradation</li>
            </ul>
          </section>

          <section>
            <h2 className="text-2xl font-bold text-gray-900 mb-4">6. User Responsibilities</h2>
            <p>You agree NOT to:</p>
            <ul className="list-disc list-inside space-y-2 ml-4">
              <li>Use the service for illegal purposes or in violation of laws</li>
              <li>Share your credentials or account with others</li>
              <li>Attempt to access unauthorized parts of the system</li>
              <li>Reverse-engineer, decompile, or attempt to derive source code</li>
              <li>Use the service to develop competing products</li>
              <li>Transmit viruses, malware, or harmful code</li>
              <li>Spam, abuse, harass, or threaten other users</li>
            </ul>
          </section>

          <section>
            <h2 className="text-2xl font-bold text-gray-900 mb-4">7. Intellectual Property</h2>
            <p>
              Depart's platform, including all software, designs, and code, is owned by Depart and protected by copyright and
              intellectual property laws. You may not copy, modify, or redistribute our platform without permission.
            </p>
            <p className="mt-4">
              You retain ownership of your data. By using our service, you grant us a license to process, analyze, and improve
              our services using your data (in anonymized form).
            </p>
          </section>

          <section>
            <h2 className="text-2xl font-bold text-gray-900 mb-4">8. Disclaimers</h2>
            <p>
              The service is provided "as is" without warranties. We do NOT guarantee that:
            </p>
            <ul className="list-disc list-inside space-y-2 ml-4">
              <li>Converted code will be 100% bug-free</li>
              <li>The service will be error-free or uninterrupted</li>
              <li>Data will never be lost</li>
              <li>Migrations will succeed without manual intervention</li>
            </ul>
            <p className="mt-4">
              You are responsible for testing converted code and validating results in your environment before production use.
            </p>
          </section>

          <section>
            <h2 className="text-2xl font-bold text-gray-900 mb-4">9. Limitation of Liability</h2>
            <p>
              To the maximum extent permitted by law, Depart is not liable for indirect, incidental, special, or consequential
              damages arising from your use of or inability to use the service, including lost profits, data loss, or business
              interruption.
            </p>
            <p className="mt-4">
              Our total liability to you shall not exceed the amount you paid us in the 12 months preceding the claim.
            </p>
          </section>

          <section>
            <h2 className="text-2xl font-bold text-gray-900 mb-4">10. Termination</h2>
            <p>We may terminate or suspend your account:</p>
            <ul className="list-disc list-inside space-y-2 ml-4">
              <li>If you violate these terms</li>
              <li>If you violate laws or regulations</li>
              <li>If your account is inactive for 180+ days</li>
              <li>Without notice if we reasonably believe you pose a security risk</li>
            </ul>
            <p className="mt-4">Upon termination, your access is revoked and your data is deleted after 30 days.</p>
          </section>

          <section>
            <h2 className="text-2xl font-bold text-gray-900 mb-4">11. Indemnification</h2>
            <p>
              You agree to indemnify and hold harmless Depart, its officers, and employees from any claims, damages, or costs
              arising from your violation of these terms or misuse of the service.
            </p>
          </section>

          <section>
            <h2 className="text-2xl font-bold text-gray-900 mb-4">12. Governing Law</h2>
            <p>
              These terms are governed by and construed in accordance with the laws of Delaware, USA, without regard to its
              conflict of law provisions.
            </p>
          </section>

          <section>
            <h2 className="text-2xl font-bold text-gray-900 mb-4">13. Contact</h2>
            <p>
              For questions about these terms, contact us at{' '}
              <a href="mailto:legal@depart.io" className="text-purple-600 hover:text-purple-700">
                legal@depart.io
              </a>
            </p>
          </section>

          <section>
            <h2 className="text-2xl font-bold text-gray-900 mb-4">14. Changes to Terms</h2>
            <p>
              We may update these terms at any time. Continued use of the service constitutes acceptance of new terms. We will
              notify you of significant changes via email.
            </p>
          </section>
        </div>
      </div>
    </div>
  );
}

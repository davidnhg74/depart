export default function PrivacyPage() {
  return (
    <div className="min-h-screen bg-gray-50 py-12 px-4">
      <div className="max-w-3xl mx-auto bg-white rounded-lg shadow p-8">
        <h1 className="text-4xl font-bold text-gray-900 mb-2">Privacy Policy</h1>
        <p className="text-gray-600 mb-8">Last updated: April 2026</p>

        <div className="prose prose-lg max-w-none space-y-8 text-gray-700">
          <section>
            <h2 className="text-2xl font-bold text-gray-900 mb-4">1. Introduction</h2>
            <p>
              Hafen ("we," "us," "our," or "Company") is committed to protecting your privacy. This Privacy Policy explains how
              we collect, use, disclose, and safeguard your information when you visit our website and use our services.
            </p>
          </section>

          <section>
            <h2 className="text-2xl font-bold text-gray-900 mb-4">2. Information We Collect</h2>
            <p>We collect information you provide directly:</p>
            <ul className="list-disc list-inside space-y-2 ml-4">
              <li>Account information: name, email, password, company</li>
              <li>Database credentials for analysis (stored temporarily, never persisted)</li>
              <li>Usage data: migrations created, conversions run, features used</li>
              <li>Payment information: processed securely through Stripe</li>
              <li>Communication data: support tickets, contact form submissions</li>
            </ul>
          </section>

          <section>
            <h2 className="text-2xl font-bold text-gray-900 mb-4">3. Database Credentials</h2>
            <p>
              Your Oracle and PostgreSQL connection credentials are used ONLY to analyze and migrate your schemas. We:
            </p>
            <ul className="list-disc list-inside space-y-2 ml-4">
              <li>Never store database credentials after your session ends</li>
              <li>Use encrypted connections (SSL/TLS) for all database communications</li>
              <li>Never log or retain your passwords</li>
              <li>Delete temporary connection tokens immediately after use</li>
            </ul>
          </section>

          <section>
            <h2 className="text-2xl font-bold text-gray-900 mb-4">4. How We Use Your Information</h2>
            <p>We use information to:</p>
            <ul className="list-disc list-inside space-y-2 ml-4">
              <li>Provide and improve our services</li>
              <li>Authenticate your account and process transactions</li>
              <li>Send service updates, technical notices, and support messages</li>
              <li>Respond to your inquiries and support requests</li>
              <li>Analyze usage patterns to improve product functionality</li>
              <li>Detect and prevent fraud and security issues</li>
            </ul>
          </section>

          <section>
            <h2 className="text-2xl font-bold text-gray-900 mb-4">5. Data Sharing</h2>
            <p>
              We do NOT sell or share your personal information with third parties, except:
            </p>
            <ul className="list-disc list-inside space-y-2 ml-4">
              <li>Service providers: Stripe (payments), Resend (email), AWS (hosting)</li>
              <li>Legal requirements: if required by law or legal process</li>
              <li>Business transfers: in the event of acquisition or bankruptcy</li>
            </ul>
          </section>

          <section>
            <h2 className="text-2xl font-bold text-gray-900 mb-4">6. Data Retention</h2>
            <p>
              We retain your account information while your account is active. You can request deletion of your account and
              associated data anytime. We retain anonymized usage data for up to 2 years for analytics.
            </p>
          </section>

          <section>
            <h2 className="text-2xl font-bold text-gray-900 mb-4">7. Security</h2>
            <p>
              We implement industry-standard security measures including encryption, secure authentication, and regular security
              audits. However, no method of transmission over the internet is 100% secure.
            </p>
          </section>

          <section>
            <h2 className="text-2xl font-bold text-gray-900 mb-4">8. Your Rights</h2>
            <p>You have the right to:</p>
            <ul className="list-disc list-inside space-y-2 ml-4">
              <li>Access your personal data</li>
              <li>Correct inaccurate information</li>
              <li>Request deletion of your account and data</li>
              <li>Opt out of marketing communications</li>
              <li>Export your data in a standard format</li>
            </ul>
          </section>

          <section>
            <h2 className="text-2xl font-bold text-gray-900 mb-4">9. Contact Us</h2>
            <p>
              For privacy questions or to exercise your rights, email us at{' '}
              <a href="mailto:privacy@hafen.ai" className="text-purple-600 hover:text-purple-700">
                privacy@hafen.ai
              </a>
            </p>
          </section>

          <section>
            <h2 className="text-2xl font-bold text-gray-900 mb-4">10. Policy Changes</h2>
            <p>
              We may update this policy occasionally. We will notify you of significant changes via email or by posting the new
              policy on our site.
            </p>
          </section>
        </div>
      </div>
    </div>
  );
}

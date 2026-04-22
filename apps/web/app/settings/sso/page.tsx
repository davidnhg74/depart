'use client';

/**
 * /settings/sso — admin-only OIDC + SAML configuration.
 *
 * Tabbed by protocol: the operator picks OIDC or SAML and fills in
 * the corresponding fields. Enable + default_role + auto_provision
 * are global across protocols since only one can be active per install.
 *
 * Secrets / certs are write-only from the UI's perspective — the
 * backend returns `*_set` booleans instead of the values. An empty
 * field on save means "leave unchanged" so admins can PATCH a
 * surrounding field without retyping everything.
 */

import { useEffect, useState } from 'react';

import SelfHostedGuard from '@/app/components/SelfHostedGuard';
import {
  apiBaseUrl,
  getSsoConfig,
  SsoConfig,
  testSsoDiscovery,
  updateSsoConfig,
} from '@/app/lib/api';
import { useAuthStore } from '@/app/store/authStore';


export default function SsoSettingsPage() {
  return (
    <SelfHostedGuard>
      <AdminOnly>
        <SsoContent />
      </AdminOnly>
    </SelfHostedGuard>
  );
}


function AdminOnly({ children }: { children: React.ReactNode }) {
  const { user } = useAuthStore();
  if (!user) return null;
  if (user.role !== 'admin') {
    return (
      <main className="min-h-screen bg-gray-50">
        <div className="container mx-auto max-w-2xl px-4 py-20">
          <div className="rounded-xl border border-amber-200 bg-amber-50 p-8">
            <h1 className="text-2xl font-bold text-amber-900">Admins only</h1>
            <p className="mt-3 text-amber-800">
              SSO configuration carries credentials that unlock login for
              your entire org. Admin role required.
            </p>
          </div>
        </div>
      </main>
    );
  }
  return <>{children}</>;
}


function SsoContent() {
  const [cfg, setCfg] = useState<SsoConfig | null>(null);
  const [loadErr, setLoadErr] = useState('');

  const [protocol, setProtocol] = useState<'oidc' | 'saml'>('oidc');
  const [enabled, setEnabled] = useState(false);
  const [defaultRole, setDefaultRole] = useState<'operator' | 'viewer'>('viewer');
  const [autoProvision, setAutoProvision] = useState(true);

  // OIDC
  const [issuer, setIssuer] = useState('');
  const [clientId, setClientId] = useState('');
  const [clientSecret, setClientSecret] = useState('');

  // SAML
  const [samlEntityId, setSamlEntityId] = useState('');
  const [samlSsoUrl, setSamlSsoUrl] = useState('');
  const [samlCert, setSamlCert] = useState('');

  const [saving, setSaving] = useState(false);
  const [message, setMessage] = useState<{ kind: 'ok' | 'err'; text: string } | null>(
    null,
  );

  const [testResult, setTestResult] = useState<null | {
    ok: boolean;
    authorization_endpoint?: string;
    error?: string;
  }>(null);

  async function refresh() {
    try {
      const c = await getSsoConfig();
      setCfg(c);
      setProtocol((c.protocol as 'oidc' | 'saml') || 'oidc');
      setEnabled(c.enabled);
      const role =
        c.default_role === 'admin' ? 'viewer' : c.default_role || 'viewer';
      setDefaultRole(role as 'operator' | 'viewer');
      setAutoProvision(c.auto_provision);
      setIssuer(c.issuer || '');
      setClientId(c.client_id || '');
      setSamlEntityId(c.saml_entity_id || '');
      setSamlSsoUrl(c.saml_sso_url || '');
    } catch (e: any) {
      setLoadErr(e?.response?.data?.detail || e?.message || 'Failed to load.');
    }
  }

  useEffect(() => {
    void refresh();
  }, []);

  async function save() {
    setSaving(true);
    setMessage(null);
    try {
      const next = await updateSsoConfig({
        protocol,
        enabled,
        default_role: defaultRole,
        auto_provision: autoProvision,
        issuer: protocol === 'oidc' ? issuer : undefined,
        client_id: protocol === 'oidc' ? clientId : undefined,
        client_secret: protocol === 'oidc' ? clientSecret : undefined,
        saml_entity_id: protocol === 'saml' ? samlEntityId : undefined,
        saml_sso_url: protocol === 'saml' ? samlSsoUrl : undefined,
        saml_x509_cert: protocol === 'saml' ? samlCert : undefined,
      });
      setCfg(next);
      setClientSecret('');
      setSamlCert('');
      setMessage({ kind: 'ok', text: 'SSO configuration saved.' });
    } catch (e: any) {
      setMessage({
        kind: 'err',
        text: e?.response?.data?.detail || e?.message || 'Save failed.',
      });
    } finally {
      setSaving(false);
    }
  }

  async function runDiscoveryTest() {
    setTestResult(null);
    try {
      const r = await testSsoDiscovery();
      setTestResult({ ok: r.ok, authorization_endpoint: r.authorization_endpoint });
    } catch (e: any) {
      setTestResult({
        ok: false,
        error: e?.response?.data?.detail || e?.message || 'Test failed.',
      });
    }
  }

  const base = typeof window !== 'undefined' ? window.location.origin : '';
  const acsUrl = `${base}/api/v1/auth/saml/acs`;
  const metadataUrl = `${apiBaseUrl()}/api/v1/auth/saml/metadata`;

  return (
    <main className="min-h-screen bg-gray-50">
      <div className="container mx-auto max-w-3xl px-4 py-12">
        <h1 className="text-3xl font-bold text-gray-900">SSO</h1>
        <p className="mt-2 text-gray-600">
          Connect your identity provider so employees can log in with their
          corporate account. Pick a protocol and fill in the details from your
          IdP&apos;s admin console.
        </p>

        {loadErr && (
          <div className="mt-6 rounded-lg border border-red-200 bg-red-50 p-4 text-sm text-red-700">
            {loadErr}
          </div>
        )}

        {/* Protocol tabs */}
        <div className="mt-8 flex gap-2 border-b border-gray-200">
          <TabButton
            label="OIDC"
            active={protocol === 'oidc'}
            onClick={() => setProtocol('oidc')}
          />
          <TabButton
            label="SAML"
            active={protocol === 'saml'}
            onClick={() => setProtocol('saml')}
          />
        </div>

        <section className="mt-6 space-y-6 rounded-b-xl rounded-tr-xl border border-gray-200 bg-white p-8 shadow-sm">
          {protocol === 'oidc' ? (
            <>
              <Field label="Issuer" hint="Base URL of your IdP. We append /.well-known/openid-configuration.">
                <input
                  type="url"
                  value={issuer}
                  onChange={(e) => setIssuer(e.target.value)}
                  placeholder="https://your-tenant.okta.com"
                  className="w-full rounded-md border border-gray-300 px-3 py-2 font-mono text-sm"
                />
              </Field>
              <div className="grid grid-cols-1 gap-6 md:grid-cols-2">
                <Field label="Client ID">
                  <input
                    type="text"
                    value={clientId}
                    onChange={(e) => setClientId(e.target.value)}
                    className="w-full rounded-md border border-gray-300 px-3 py-2 font-mono text-sm"
                  />
                </Field>
                <Field
                  label="Client secret"
                  hint={
                    cfg?.client_secret_set
                      ? 'Stored. Leave blank to keep; type to replace.'
                      : 'Not set yet.'
                  }
                >
                  <input
                    type="password"
                    value={clientSecret}
                    onChange={(e) => setClientSecret(e.target.value)}
                    placeholder={cfg?.client_secret_set ? '•••' : ''}
                    className="w-full rounded-md border border-gray-300 px-3 py-2 font-mono text-sm"
                  />
                </Field>
              </div>
              <p className="rounded-md border border-blue-200 bg-blue-50 p-3 text-xs text-blue-900">
                Configure your IdP to allow redirect URI{' '}
                <code className="rounded bg-white px-1">
                  {base}/api/v1/auth/sso/callback
                </code>
                .
              </p>
            </>
          ) : (
            <>
              <Field
                label="IdP entity ID (issuer)"
                hint="Usually shown as 'Issuer' or 'Entity ID' in your IdP's SAML app."
              >
                <input
                  type="text"
                  value={samlEntityId}
                  onChange={(e) => setSamlEntityId(e.target.value)}
                  placeholder="https://sts.windows.net/<tenant>/"
                  className="w-full rounded-md border border-gray-300 px-3 py-2 font-mono text-sm"
                />
              </Field>
              <Field
                label="IdP single-sign-on URL"
                hint="HTTP-Redirect binding. Shown as 'Login URL' or 'SSO URL' in the IdP."
              >
                <input
                  type="url"
                  value={samlSsoUrl}
                  onChange={(e) => setSamlSsoUrl(e.target.value)}
                  placeholder="https://login.microsoftonline.com/<tenant>/saml2"
                  className="w-full rounded-md border border-gray-300 px-3 py-2 font-mono text-sm"
                />
              </Field>
              <Field
                label="IdP X.509 certificate (PEM)"
                hint={
                  cfg?.saml_x509_cert_set
                    ? 'Stored. Leave blank to keep; paste to replace.'
                    : 'Paste the signing cert from your IdP (PEM format, with BEGIN/END lines).'
                }
              >
                <textarea
                  value={samlCert}
                  onChange={(e) => setSamlCert(e.target.value)}
                  rows={6}
                  placeholder={
                    cfg?.saml_x509_cert_set
                      ? '••• (stored)'
                      : '-----BEGIN CERTIFICATE-----\nMIIB…\n-----END CERTIFICATE-----'
                  }
                  className="w-full rounded-md border border-gray-300 px-3 py-2 font-mono text-xs"
                />
              </Field>
              <div className="rounded-md border border-blue-200 bg-blue-50 p-3 text-xs text-blue-900">
                <p>Configure your IdP with:</p>
                <ul className="ml-4 mt-1 list-disc space-y-0.5">
                  <li>
                    ACS URL:{' '}
                    <code className="rounded bg-white px-1">{acsUrl}</code>
                  </li>
                  <li>
                    Entity ID:{' '}
                    <code className="rounded bg-white px-1">
                      {base}/api/v1/auth/saml/metadata
                    </code>
                  </li>
                  <li>NameID format: emailAddress</li>
                </ul>
                <p className="mt-2">
                  Or give it our metadata XML:{' '}
                  <a
                    href={metadataUrl}
                    target="_blank"
                    rel="noreferrer"
                    className="underline"
                  >
                    download metadata
                  </a>
                </p>
              </div>
            </>
          )}

          <div className="border-t border-gray-100 pt-6">
            <div className="grid grid-cols-1 gap-6 md:grid-cols-2">
              <Field label="Default role for new SSO users">
                <select
                  value={defaultRole}
                  onChange={(e) =>
                    setDefaultRole(e.target.value as 'operator' | 'viewer')
                  }
                  className="w-full rounded-md border border-gray-300 px-3 py-2 text-sm"
                >
                  <option value="viewer">viewer</option>
                  <option value="operator">operator</option>
                </select>
              </Field>
              <Field label="Auto-provision new users">
                <label className="flex items-center gap-2 text-sm text-gray-700">
                  <input
                    type="checkbox"
                    checked={autoProvision}
                    onChange={(e) => setAutoProvision(e.target.checked)}
                    className="h-4 w-4 rounded border-gray-300 text-purple-600"
                  />
                  Create a local account on first SSO login (email match)
                </label>
              </Field>
            </div>

            <Field label="Enabled">
              <label className="mt-4 flex items-center gap-2 text-sm text-gray-700">
                <input
                  type="checkbox"
                  checked={enabled}
                  onChange={(e) => setEnabled(e.target.checked)}
                  className="h-4 w-4 rounded border-gray-300 text-purple-600"
                />
                Show &quot;Log in with SSO&quot; button on the /login page
              </label>
            </Field>
          </div>

          <div className="flex items-center gap-3">
            <button
              onClick={save}
              disabled={saving}
              className="rounded-md bg-purple-600 px-6 py-2 font-semibold text-white shadow-sm transition hover:bg-purple-700 disabled:bg-gray-300"
            >
              {saving ? 'Saving…' : 'Save'}
            </button>
            {protocol === 'oidc' && (
              <button
                onClick={runDiscoveryTest}
                disabled={!cfg?.issuer && !issuer}
                className="rounded-md border border-gray-300 bg-white px-5 py-2 text-sm font-semibold text-gray-700 shadow-sm hover:bg-gray-50 disabled:opacity-50"
              >
                Test discovery
              </button>
            )}
          </div>

          {message && (
            <p
              className={`text-sm ${
                message.kind === 'ok' ? 'text-green-700' : 'text-red-700'
              }`}
            >
              {message.text}
            </p>
          )}

          {testResult && (
            <div
              className={`rounded-md border p-3 text-sm ${
                testResult.ok
                  ? 'border-green-200 bg-green-50 text-green-900'
                  : 'border-red-200 bg-red-50 text-red-800'
              }`}
            >
              {testResult.ok ? (
                <>
                  ✓ Discovery succeeded.{' '}
                  <span className="font-mono text-xs">
                    authorize: {testResult.authorization_endpoint}
                  </span>
                </>
              ) : (
                <>✗ {testResult.error}</>
              )}
            </div>
          )}
        </section>
      </div>
    </main>
  );
}


function TabButton({
  label,
  active,
  onClick,
}: {
  label: string;
  active: boolean;
  onClick: () => void;
}) {
  return (
    <button
      onClick={onClick}
      className={`rounded-t-md border border-b-0 px-4 py-2 text-sm font-semibold transition ${
        active
          ? 'border-gray-200 bg-white text-purple-700'
          : 'border-transparent bg-transparent text-gray-500 hover:text-gray-700'
      }`}
    >
      {label}
    </button>
  );
}


function Field({
  label,
  hint,
  children,
}: {
  label: string;
  hint?: string;
  children: React.ReactNode;
}) {
  return (
    <div>
      <label className="block text-sm font-medium text-gray-700">{label}</label>
      {hint && <p className="mt-0.5 text-xs text-gray-500">{hint}</p>}
      <div className="mt-1.5">{children}</div>
    </div>
  );
}

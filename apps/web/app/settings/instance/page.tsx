'use client';

/**
 * /settings/instance — local instance configuration for self-hosted depart.
 *
 * Exposes the BYOK Anthropic key today. Once the license verifier
 * lands, the LicensePanel below becomes interactive — tier, expiry,
 * features, and JWT upload. Keeping both on the same page because the
 * operator's mental model is "where do I configure this box?" — split
 * pages would fragment that.
 *
 * No auth: self-hosted, localhost. Same reasoning as the backend.
 */

import { useEffect, useState } from 'react';

import SelfHostedGuard from '@/app/components/SelfHostedGuard';
import { apiBaseUrl, rotateEncryptionKey } from '@/app/lib/api';


type SettingsStatus = {
  anthropic_key_masked: string | null;
  anthropic_key_configured: boolean;
  license_configured: boolean;
  encryption_key_configured: boolean;
};

type LicenseStatus = {
  valid: boolean;
  tier: 'community' | 'pro' | 'enterprise' | string;
  features: string[];
  expires_at: string | null;
  subject: string | null;
  project: string | null;
  reason: string | null;
};


export default function InstanceSettingsPage() {
  return (
    <SelfHostedGuard>
      <InstanceSettingsContent />
    </SelfHostedGuard>
  );
}

function InstanceSettingsContent() {
  const [status, setStatus] = useState<SettingsStatus | null>(null);
  const [license, setLicense] = useState<LicenseStatus | null>(null);
  const [loadErr, setLoadErr] = useState<string>('');

  async function refresh() {
    try {
      const [s, l] = await Promise.all([
        fetch(`${apiBaseUrl()}/api/v1/settings`).then((r) => r.json()),
        fetch(`${apiBaseUrl()}/api/v1/license`).then((r) => r.json()),
      ]);
      setStatus(s);
      setLicense(l);
    } catch (e) {
      setLoadErr(e instanceof Error ? e.message : 'Unknown error');
    }
  }

  useEffect(() => {
    void refresh();
  }, []);

  return (
    <main className="min-h-screen bg-gray-50">
      <div className="container mx-auto max-w-3xl px-4 py-12">
        <Header />

        {loadErr && (
          <div className="mb-6 rounded-lg border border-red-200 bg-red-50 p-4 text-sm text-red-700">
            Failed to load settings: {loadErr}. Is the API running at {apiBaseUrl()}?
          </div>
        )}

        <AnthropicKeySection status={status} onUpdated={setStatus} />
        <LicenseSection license={license} onUpdated={(l) => {
          setLicense(l);
          void refresh();  // pull settings too so license_configured updates
        }} />
        <EncryptionSection status={status} />
      </div>
    </main>
  );
}


/* ─── Sections ─────────────────────────────────────────────────────────── */

function Header() {
  return (
    <div className="mb-10">
      <h1 className="text-3xl font-bold text-gray-900">Instance settings</h1>
      <p className="mt-2 text-gray-600">
        Local-only configuration for this depart instance. Values live in your Postgres
        metadata DB; nothing is synced to any cloud service.
      </p>
    </div>
  );
}


function AnthropicKeySection({
  status,
  onUpdated,
}: {
  status: SettingsStatus | null;
  onUpdated: (s: SettingsStatus) => void;
}) {
  const [input, setInput] = useState('');
  const [saving, setSaving] = useState(false);
  const [message, setMessage] = useState<{ kind: 'ok' | 'err'; text: string } | null>(null);

  async function putKey(value: string) {
    setSaving(true);
    setMessage(null);
    try {
      const r = await fetch(`${apiBaseUrl()}/api/v1/settings/anthropic-key`, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ api_key: value }),
      });
      if (!r.ok) throw new Error(`HTTP ${r.status}`);
      const body = (await r.json()) as SettingsStatus;
      onUpdated(body);
      setInput('');
      setMessage({ kind: 'ok', text: value ? 'API key saved.' : 'API key cleared.' });
    } catch (e) {
      setMessage({ kind: 'err', text: e instanceof Error ? e.message : 'Save failed.' });
    } finally {
      setSaving(false);
    }
  }

  return (
    <section className="mb-8 rounded-xl border border-gray-200 bg-white p-8 shadow-sm">
      <div className="flex items-baseline justify-between">
        <h2 className="text-xl font-bold text-gray-900">Anthropic API key (BYOK)</h2>
        {status?.anthropic_key_configured ? (
          <span className="inline-flex items-center gap-1 rounded-md bg-green-100 px-2 py-0.5 text-xs font-semibold text-green-800">
            <span>✓</span> Configured
          </span>
        ) : (
          <span className="inline-flex items-center gap-1 rounded-md bg-gray-100 px-2 py-0.5 text-xs font-semibold text-gray-600">
            Not configured
          </span>
        )}
      </div>

      <p className="mt-3 text-sm text-gray-600">
        Needed for live AI conversion on your actual code. The key stays on this instance —
        we never see it. Claude calls go from your server directly to Anthropic.
      </p>

      {status?.anthropic_key_masked && (
        <div className="mt-4 rounded-md bg-gray-50 p-3 font-mono text-sm text-gray-700">
          Current: <span className="font-semibold">{status.anthropic_key_masked}</span>
        </div>
      )}

      <div className="mt-4">
        <label
          htmlFor="anthropic-key-input"
          className="block text-sm font-medium text-gray-700"
        >
          {status?.anthropic_key_configured ? 'Replace key' : 'Set key'}
        </label>
        <input
          id="anthropic-key-input"
          type="password"
          value={input}
          onChange={(e) => setInput(e.target.value)}
          placeholder="sk-ant-api03-..."
          className="mt-2 w-full rounded-md border border-gray-300 bg-white px-3 py-2 font-mono text-sm focus:border-purple-400 focus:outline-none focus:ring-1 focus:ring-purple-400"
          disabled={saving}
        />
        <div className="mt-3 flex items-center gap-3">
          <button
            onClick={() => void putKey(input)}
            disabled={saving || !input}
            className="rounded-md bg-purple-600 px-4 py-2 font-semibold text-white transition hover:bg-purple-700 disabled:cursor-not-allowed disabled:bg-gray-300"
          >
            {saving ? 'Saving...' : 'Save'}
          </button>
          {status?.anthropic_key_configured && (
            <button
              onClick={() => void putKey('')}
              disabled={saving}
              className="text-sm text-gray-500 underline-offset-2 hover:underline disabled:opacity-50"
            >
              Clear
            </button>
          )}
        </div>
        {message && (
          <p
            className={`mt-3 text-sm ${
              message.kind === 'ok' ? 'text-green-700' : 'text-red-700'
            }`}
          >
            {message.text}
          </p>
        )}
      </div>
    </section>
  );
}


function LicenseSection({
  license,
  onUpdated,
}: {
  license: LicenseStatus | null;
  onUpdated: (l: LicenseStatus) => void;
}) {
  const [input, setInput] = useState('');
  const [saving, setSaving] = useState(false);
  const [message, setMessage] = useState<{ kind: 'ok' | 'err'; text: string } | null>(null);

  async function putLicense(value: string) {
    setSaving(true);
    setMessage(null);
    try {
      const r = await fetch(`${apiBaseUrl()}/api/v1/license`, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ jwt: value }),
      });
      if (!r.ok) throw new Error(`HTTP ${r.status}`);
      const body = (await r.json()) as LicenseStatus;
      onUpdated(body);
      setInput('');
      if (body.valid) {
        setMessage({ kind: 'ok', text: `License accepted. Tier: ${body.tier}.` });
      } else if (value === '') {
        setMessage({ kind: 'ok', text: 'License cleared — reverted to Community.' });
      } else {
        setMessage({
          kind: 'err',
          text: `License stored but not valid: ${body.reason || 'unknown reason'}`,
        });
      }
    } catch (e) {
      setMessage({ kind: 'err', text: e instanceof Error ? e.message : 'Save failed.' });
    } finally {
      setSaving(false);
    }
  }

  const tier = license?.tier ?? 'community';
  const valid = license?.valid ?? false;

  return (
    <section className="rounded-xl border border-gray-200 bg-white p-8 shadow-sm">
      <div className="flex items-baseline justify-between">
        <h2 className="text-xl font-bold text-gray-900">License</h2>
        <TierBadge valid={valid} tier={tier} />
      </div>

      <p className="mt-3 text-sm text-gray-600">
        Upload a signed license JWT to unlock Pro features (live AI conversion, PDF runbook
        generator, priority support). Verification is local — no network check, works in
        air-gapped environments.
      </p>

      {license && (
        <div className="mt-4 space-y-1 rounded-md bg-gray-50 p-3 text-sm text-gray-700">
          {!valid && license.reason && (
            <div>
              <span className="text-gray-500">Status:</span>{' '}
              <span className="text-red-700">{license.reason}</span>
            </div>
          )}
          {valid && (
            <>
              <div>
                <span className="text-gray-500">Project:</span>{' '}
                <span className="font-semibold">{license.project}</span>
              </div>
              <div>
                <span className="text-gray-500">Issued to:</span>{' '}
                <span className="font-semibold">{license.subject}</span>
              </div>
              <div>
                <span className="text-gray-500">Features:</span>{' '}
                <span className="font-semibold">{license.features.join(', ') || '—'}</span>
              </div>
              {license.expires_at && (
                <div>
                  <span className="text-gray-500">Expires:</span>{' '}
                  <span className="font-semibold">
                    {new Date(license.expires_at).toLocaleString()}
                  </span>
                </div>
              )}
            </>
          )}
        </div>
      )}

      <div className="mt-4">
        <label htmlFor="license-input" className="block text-sm font-medium text-gray-700">
          {valid ? 'Replace license' : 'Upload license JWT'}
        </label>
        <textarea
          id="license-input"
          value={input}
          onChange={(e) => setInput(e.target.value)}
          placeholder="eyJhbGciOiJSUzI1NiIsInR5cCI6..."
          className="mt-2 h-24 w-full resize-y rounded-md border border-gray-300 bg-white px-3 py-2 font-mono text-xs focus:border-purple-400 focus:outline-none focus:ring-1 focus:ring-purple-400"
          disabled={saving}
        />
        <div className="mt-3 flex items-center gap-3">
          <button
            onClick={() => void putLicense(input)}
            disabled={saving || !input.trim()}
            className="rounded-md bg-purple-600 px-4 py-2 font-semibold text-white transition hover:bg-purple-700 disabled:cursor-not-allowed disabled:bg-gray-300"
          >
            {saving ? 'Verifying...' : 'Upload'}
          </button>
          {valid && (
            <button
              onClick={() => void putLicense('')}
              disabled={saving}
              className="text-sm text-gray-500 underline-offset-2 hover:underline disabled:opacity-50"
            >
              Clear
            </button>
          )}
        </div>
        {message && (
          <p
            className={`mt-3 text-sm ${
              message.kind === 'ok' ? 'text-green-700' : 'text-red-700'
            }`}
          >
            {message.text}
          </p>
        )}
      </div>
    </section>
  );
}

function TierBadge({ valid, tier }: { valid: boolean; tier: string }) {
  const styles = valid
    ? 'bg-green-100 text-green-800'
    : 'bg-gray-100 text-gray-600';
  const label = valid ? tier.charAt(0).toUpperCase() + tier.slice(1) : 'Community';
  return (
    <span className={`inline-flex items-center gap-1 rounded-md px-2 py-0.5 text-xs font-semibold ${styles}`}>
      {label}
    </span>
  );
}


function EncryptionSection({ status }: { status: SettingsStatus | null }) {
  const configured = status?.encryption_key_configured ?? false;
  const [rotating, setRotating] = useState(false);
  const [result, setResult] = useState<
    { kind: 'ok'; rotated: number } | { kind: 'err'; text: string } | null
  >(null);

  async function rotate() {
    if (
      !confirm(
        'Rotate the encryption key? The API will re-encrypt every stored secret ' +
          '(DB connection strings, Anthropic key, license, OIDC client secret) ' +
          'with the current primary key. Make sure the new key is already in ' +
          'DEPART_ENCRYPTION_KEYS and the server has been restarted.',
      )
    ) {
      return;
    }
    setRotating(true);
    setResult(null);
    try {
      const r = await rotateEncryptionKey();
      setResult({ kind: 'ok', rotated: r.rotated });
    } catch (e: any) {
      setResult({
        kind: 'err',
        text: e?.response?.data?.detail || e?.message || 'Rotation failed.',
      });
    } finally {
      setRotating(false);
    }
  }

  return (
    <section className="mt-8 rounded-xl border border-gray-200 bg-white p-8 shadow-sm">
      <div className="flex items-baseline justify-between">
        <h2 className="text-xl font-bold text-gray-900">Encryption at rest</h2>
        {configured ? (
          <span className="inline-flex items-center gap-1 rounded-md bg-green-100 px-2 py-0.5 text-xs font-semibold text-green-800">
            ✓ Enabled
          </span>
        ) : (
          <span className="inline-flex items-center gap-1 rounded-md bg-amber-100 px-2 py-0.5 text-xs font-semibold text-amber-900">
            ⚠ Not configured
          </span>
        )}
      </div>
      <p className="mt-3 text-sm text-gray-600">
        Sensitive columns — DB connection strings, Anthropic key, license JWT,
        OIDC client secret — are encrypted with Fernet when a key is
        configured via <code className="rounded bg-gray-100 px-1">DEPART_ENCRYPTION_KEY</code>.
      </p>

      {!configured && (
        <div className="mt-4 rounded-md border border-amber-200 bg-amber-50 p-3 text-sm text-amber-900">
          <p className="font-semibold">Configure encryption:</p>
          <ol className="ml-4 mt-1 list-decimal space-y-0.5 text-xs">
            <li>
              Generate a key:{' '}
              <code className="rounded bg-white px-1">
                python -c &quot;from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())&quot;
              </code>
            </li>
            <li>
              Set <code className="rounded bg-white px-1">DEPART_ENCRYPTION_KEY=&lt;that-key&gt;</code>{' '}
              in the API container&apos;s env.
            </li>
            <li>Restart the API. New writes will be encrypted automatically.</li>
            <li>
              Come back here and click <strong>Rotate now</strong> to encrypt
              any pre-existing plaintext rows.
            </li>
          </ol>
        </div>
      )}

      {configured && (
        <div className="mt-4">
          <button
            onClick={rotate}
            disabled={rotating}
            className="rounded-md border border-gray-300 bg-white px-4 py-2 text-sm font-semibold text-gray-700 shadow-sm hover:bg-gray-50 disabled:opacity-50"
            title="Re-encrypt every sensitive column with the current primary key."
          >
            {rotating ? 'Rotating…' : 'Rotate encryption key'}
          </button>
          <p className="mt-2 text-xs text-gray-500">
            Use after you&apos;ve prepended a fresh key to{' '}
            <code className="rounded bg-gray-100 px-1">DEPART_ENCRYPTION_KEYS</code>{' '}
            and restarted the API.
          </p>
        </div>
      )}

      {result && (
        <p
          className={`mt-3 text-sm ${
            result.kind === 'ok' ? 'text-green-700' : 'text-red-700'
          }`}
        >
          {result.kind === 'ok'
            ? `✓ Re-encrypted ${result.rotated} row${result.rotated === 1 ? '' : 's'} with the current primary key.`
            : result.text}
        </p>
      )}
    </section>
  );
}

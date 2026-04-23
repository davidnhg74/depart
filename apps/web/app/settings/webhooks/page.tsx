'use client';

/**
 * /settings/webhooks — admin-only webhook subscriptions.
 *
 * Lists every endpoint configured on this install, lets an admin
 * create / edit / delete / test-fire each one. URL and secret are
 * write-only (mirrors /settings/sso); the listing shows the host and
 * `*_set` booleans so admins can see which endpoint is which without
 * the backend re-emitting the secret.
 *
 * License gate: `webhooks` feature. The 402 from the API is caught
 * and rendered as a call-to-action pointing at /settings/instance.
 */

import { useEffect, useState } from 'react';

import SelfHostedGuard from '@/app/components/SelfHostedGuard';
import {
  createWebhook,
  deleteWebhook,
  listWebhooks,
  testWebhook,
  updateWebhook,
  Webhook,
  WEBHOOK_EVENTS,
} from '@/app/lib/api';
import { useAuthStore } from '@/app/store/authStore';


export default function WebhooksSettingsPage() {
  return (
    <SelfHostedGuard>
      <AdminOnly>
        <WebhooksContent />
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
              Webhook endpoints receive migration events and carry
              signing secrets. Admin role required.
            </p>
          </div>
        </div>
      </main>
    );
  }
  return <>{children}</>;
}


type LoadState =
  | { kind: 'loading' }
  | { kind: 'ok'; endpoints: Webhook[] }
  | { kind: 'unlicensed' }
  | { kind: 'error'; message: string };


function WebhooksContent() {
  const [state, setState] = useState<LoadState>({ kind: 'loading' });
  const [editing, setEditing] = useState<Webhook | null>(null);
  const [creating, setCreating] = useState(false);

  async function refresh() {
    setState({ kind: 'loading' });
    try {
      const endpoints = await listWebhooks();
      setState({ kind: 'ok', endpoints });
    } catch (e: any) {
      if (e?.response?.status === 402) {
        setState({ kind: 'unlicensed' });
        return;
      }
      setState({
        kind: 'error',
        message: e?.response?.data?.detail || e?.message || 'Failed to load.',
      });
    }
  }

  useEffect(() => {
    void refresh();
  }, []);

  return (
    <main className="min-h-screen bg-gray-50">
      <div className="container mx-auto max-w-4xl px-4 py-12">
        <div className="flex items-center justify-between">
          <div>
            <h1 className="text-3xl font-bold text-gray-900">Webhooks</h1>
            <p className="mt-2 text-gray-600">
              Get notified when migrations finish or fail. Depart signs
              each request with HMAC-SHA256 using the secret you
              configure — subscribers validate via the{' '}
              <code className="rounded bg-gray-100 px-1">
                X-Depart-Signature
              </code>{' '}
              header.
            </p>
          </div>
          {state.kind === 'ok' && (
            <button
              onClick={() => {
                setEditing(null);
                setCreating(true);
              }}
              className="rounded-lg bg-purple-600 px-4 py-2 text-sm font-medium text-white hover:bg-purple-700"
            >
              + Add endpoint
            </button>
          )}
        </div>

        {state.kind === 'loading' && (
          <div className="mt-8 text-gray-600">Loading…</div>
        )}

        {state.kind === 'unlicensed' && <UnlicensedNotice />}

        {state.kind === 'error' && (
          <div className="mt-8 rounded-lg border border-red-200 bg-red-50 p-4 text-sm text-red-700">
            {state.message}
          </div>
        )}

        {state.kind === 'ok' && (
          <>
            {state.endpoints.length === 0 && !creating && (
              <div className="mt-8 rounded-xl border border-dashed border-gray-300 bg-white p-10 text-center text-gray-600">
                No webhooks configured yet. Add one to receive
                migration events on Slack, Microsoft Teams, or any HTTP
                endpoint of your choice.
              </div>
            )}
            {state.endpoints.length > 0 && (
              <ul className="mt-8 space-y-3">
                {state.endpoints.map((ep) => (
                  <WebhookRow
                    key={ep.id}
                    endpoint={ep}
                    onEdit={() => {
                      setCreating(false);
                      setEditing(ep);
                    }}
                    onChanged={refresh}
                  />
                ))}
              </ul>
            )}
            {(creating || editing) && (
              <EditorPanel
                existing={editing}
                onClose={() => {
                  setCreating(false);
                  setEditing(null);
                }}
                onSaved={() => {
                  setCreating(false);
                  setEditing(null);
                  void refresh();
                }}
              />
            )}
          </>
        )}
      </div>
    </main>
  );
}


function UnlicensedNotice() {
  return (
    <div className="mt-8 rounded-xl border border-amber-200 bg-amber-50 p-8">
      <h2 className="text-xl font-semibold text-amber-900">
        Webhooks require a Pro license
      </h2>
      <p className="mt-2 text-amber-800">
        Upload a Depart license that includes the{' '}
        <code className="rounded bg-amber-100 px-1">webhooks</code>{' '}
        feature to enable this page.
      </p>
      <a
        href="/settings/instance"
        className="mt-4 inline-block rounded-lg bg-amber-600 px-4 py-2 text-sm font-medium text-white hover:bg-amber-700"
      >
        Go to license settings
      </a>
    </div>
  );
}


function WebhookRow({
  endpoint,
  onEdit,
  onChanged,
}: {
  endpoint: Webhook;
  onEdit: () => void;
  onChanged: () => void;
}) {
  const [busy, setBusy] = useState<'test' | 'delete' | null>(null);
  const [flash, setFlash] = useState<string | null>(null);

  const statusPill = endpoint.last_status
    ? endpoint.last_status >= 200 && endpoint.last_status < 300
      ? 'bg-green-100 text-green-800'
      : 'bg-red-100 text-red-800'
    : 'bg-gray-100 text-gray-600';

  async function doTest() {
    setBusy('test');
    setFlash(null);
    try {
      const r = await testWebhook(endpoint.id);
      setFlash(
        r.last_status && r.last_status < 300
          ? `Sent — receiver returned ${r.last_status}.`
          : `Delivery recorded: ${r.last_error || `HTTP ${r.last_status}`}`,
      );
      onChanged();
    } catch (e: any) {
      setFlash(e?.response?.data?.detail || e?.message || 'Test failed.');
    } finally {
      setBusy(null);
    }
  }

  async function doDelete() {
    if (!window.confirm(`Delete webhook "${endpoint.name}"?`)) return;
    setBusy('delete');
    try {
      await deleteWebhook(endpoint.id);
      onChanged();
    } catch (e: any) {
      setFlash(e?.response?.data?.detail || e?.message || 'Delete failed.');
      setBusy(null);
    }
  }

  return (
    <li className="rounded-xl border border-gray-200 bg-white p-5 shadow-sm">
      <div className="flex items-start justify-between gap-4">
        <div className="min-w-0">
          <div className="flex items-center gap-2">
            <h3 className="truncate text-lg font-semibold text-gray-900">
              {endpoint.name}
            </h3>
            {!endpoint.enabled && (
              <span className="rounded-full bg-gray-200 px-2 py-0.5 text-xs font-medium text-gray-700">
                disabled
              </span>
            )}
          </div>
          <p className="mt-1 text-sm text-gray-600">
            <span className="font-mono">{endpoint.url_host || '—'}</span>
            {endpoint.secret_set && (
              <span className="ml-3 text-xs text-gray-500">· secret set</span>
            )}
          </p>
          <div className="mt-2 flex flex-wrap gap-1">
            {endpoint.events.map((ev) => (
              <span
                key={ev}
                className="rounded bg-purple-50 px-2 py-0.5 text-xs text-purple-800"
              >
                {ev}
              </span>
            ))}
          </div>
          {endpoint.last_triggered_at && (
            <p className="mt-3 text-xs text-gray-500">
              Last delivery{' '}
              <span
                className={`rounded-full px-2 py-0.5 font-medium ${statusPill}`}
              >
                {endpoint.last_status ?? 'no status'}
              </span>{' '}
              at {new Date(endpoint.last_triggered_at).toLocaleString()}
              {endpoint.last_error && (
                <span className="ml-2 text-red-600">· {endpoint.last_error}</span>
              )}
            </p>
          )}
          {flash && (
            <p className="mt-2 text-sm text-gray-700">{flash}</p>
          )}
        </div>
        <div className="flex shrink-0 flex-col gap-2">
          <button
            onClick={doTest}
            disabled={busy !== null}
            className="rounded border border-gray-300 bg-white px-3 py-1 text-xs font-medium text-gray-700 hover:bg-gray-50 disabled:opacity-50"
          >
            {busy === 'test' ? 'Sending…' : 'Send test'}
          </button>
          <button
            onClick={onEdit}
            disabled={busy !== null}
            className="rounded border border-gray-300 bg-white px-3 py-1 text-xs font-medium text-gray-700 hover:bg-gray-50 disabled:opacity-50"
          >
            Edit
          </button>
          <button
            onClick={doDelete}
            disabled={busy !== null}
            className="rounded border border-red-300 bg-white px-3 py-1 text-xs font-medium text-red-700 hover:bg-red-50 disabled:opacity-50"
          >
            {busy === 'delete' ? '…' : 'Delete'}
          </button>
        </div>
      </div>
    </li>
  );
}


function EditorPanel({
  existing,
  onClose,
  onSaved,
}: {
  existing: Webhook | null;
  onClose: () => void;
  onSaved: () => void;
}) {
  const [name, setName] = useState(existing?.name || '');
  const [url, setUrl] = useState('');
  const [secret, setSecret] = useState('');
  const [events, setEvents] = useState<string[]>(
    existing?.events || [...WEBHOOK_EVENTS],
  );
  const [enabled, setEnabled] = useState(existing?.enabled ?? true);
  const [saving, setSaving] = useState(false);
  const [err, setErr] = useState<string | null>(null);

  function toggleEvent(ev: string) {
    setEvents((prev) =>
      prev.includes(ev) ? prev.filter((e) => e !== ev) : [...prev, ev],
    );
  }

  async function save() {
    if (!name.trim()) {
      setErr('Give the endpoint a name.');
      return;
    }
    if (!existing && !url.trim()) {
      setErr('Endpoint URL is required.');
      return;
    }
    if (events.length === 0) {
      setErr('Pick at least one event.');
      return;
    }
    setSaving(true);
    setErr(null);
    try {
      if (existing) {
        const patch: Parameters<typeof updateWebhook>[1] = {
          name,
          events,
          enabled,
        };
        if (url.trim()) patch.url = url.trim();
        // Secret: empty string clears on PATCH, so only send it if
        // the operator actually typed something. undefined = untouched.
        if (secret !== '') patch.secret = secret;
        await updateWebhook(existing.id, patch);
      } else {
        await createWebhook({
          name,
          url: url.trim(),
          secret: secret || undefined,
          events,
          enabled,
        });
      }
      onSaved();
    } catch (e: any) {
      const detail = e?.response?.data?.detail;
      setErr(
        typeof detail === 'string'
          ? detail
          : detail?.error
            ? `${detail.error}${detail.unknown ? `: ${detail.unknown.join(', ')}` : ''}`
            : e?.message || 'Save failed.',
      );
    } finally {
      setSaving(false);
    }
  }

  return (
    <div className="mt-6 rounded-xl border border-purple-200 bg-white p-6 shadow-sm">
      <div className="flex items-center justify-between">
        <h2 className="text-lg font-semibold text-gray-900">
          {existing ? `Edit "${existing.name}"` : 'Add webhook'}
        </h2>
        <button
          onClick={onClose}
          className="text-sm text-gray-500 hover:text-gray-700"
        >
          Cancel
        </button>
      </div>

      <div className="mt-4 space-y-4">
        <label className="block">
          <span className="text-sm font-medium text-gray-700">Name</span>
          <input
            value={name}
            onChange={(e) => setName(e.target.value)}
            placeholder="e.g. Ops Slack"
            className="mt-1 block w-full rounded-lg border border-gray-300 px-3 py-2 text-sm"
          />
        </label>

        <label className="block">
          <span className="text-sm font-medium text-gray-700">
            Endpoint URL
          </span>
          <input
            value={url}
            onChange={(e) => setUrl(e.target.value)}
            placeholder={
              existing
                ? `Currently: ${existing.url_host} (leave blank to keep)`
                : 'https://hooks.slack.com/services/T000/B000/xxxx'
            }
            className="mt-1 block w-full rounded-lg border border-gray-300 px-3 py-2 font-mono text-sm"
          />
        </label>

        <label className="block">
          <span className="text-sm font-medium text-gray-700">
            Signing secret (HMAC-SHA256)
          </span>
          <input
            value={secret}
            onChange={(e) => setSecret(e.target.value)}
            placeholder={
              existing?.secret_set
                ? 'Stored. Leave blank to keep, or type to replace.'
                : 'Optional — subscribers validate X-Depart-Signature with this key.'
            }
            className="mt-1 block w-full rounded-lg border border-gray-300 px-3 py-2 font-mono text-sm"
          />
        </label>

        <div>
          <span className="text-sm font-medium text-gray-700">Events</span>
          <div className="mt-2 flex flex-wrap gap-3">
            {WEBHOOK_EVENTS.map((ev) => (
              <label
                key={ev}
                className="flex items-center gap-2 rounded border border-gray-300 bg-white px-3 py-1.5 text-sm"
              >
                <input
                  type="checkbox"
                  checked={events.includes(ev)}
                  onChange={() => toggleEvent(ev)}
                />
                <span className="font-mono">{ev}</span>
              </label>
            ))}
          </div>
        </div>

        <label className="flex items-center gap-2 text-sm text-gray-700">
          <input
            type="checkbox"
            checked={enabled}
            onChange={(e) => setEnabled(e.target.checked)}
          />
          Enabled (fires events when checked)
        </label>

        {err && (
          <div className="rounded-lg border border-red-200 bg-red-50 p-3 text-sm text-red-700">
            {err}
          </div>
        )}

        <div className="flex gap-3 pt-2">
          <button
            onClick={save}
            disabled={saving}
            className="rounded-lg bg-purple-600 px-4 py-2 text-sm font-medium text-white hover:bg-purple-700 disabled:opacity-50"
          >
            {saving ? 'Saving…' : existing ? 'Save changes' : 'Add endpoint'}
          </button>
          <button
            onClick={onClose}
            disabled={saving}
            className="rounded-lg border border-gray-300 bg-white px-4 py-2 text-sm font-medium text-gray-700 hover:bg-gray-50"
          >
            Cancel
          </button>
        </div>
      </div>
    </div>
  );
}

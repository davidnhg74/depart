'use client';

/**
 * /settings/audit — admin / viewer-visible audit log.
 *
 * Reads /api/v1/audit with a simple action filter + load-more button.
 * Role gating is enforced by the backend (admin or viewer); this page
 * shows a friendly forbidden notice to operators who try to reach it
 * directly.
 */

import { useEffect, useState } from 'react';

import SelfHostedGuard from '@/app/components/SelfHostedGuard';
import {
  AuditEvent,
  AuditPage,
  AuditVerifyResult,
  listAuditEvents,
  verifyAuditChain,
} from '@/app/lib/api';
import { useAuthStore } from '@/app/store/authStore';


const COMMON_ACTIONS = [
  { value: '', label: 'All actions' },
  { value: 'user.login', label: 'Logins' },
  { value: 'user.login_failed', label: 'Failed logins' },
  { value: 'user.created', label: 'User created' },
  { value: 'user.updated', label: 'User updated' },
  { value: 'user.deleted', label: 'User deleted' },
  { value: 'migration.created', label: 'Migration created' },
  { value: 'migration.run', label: 'Migration run' },
  { value: 'settings.anthropic_key_updated', label: 'Anthropic key changed' },
  { value: 'license.uploaded', label: 'License uploaded' },
  { value: 'license.cleared', label: 'License cleared' },
  { value: 'convert.live', label: 'AI conversion' },
  { value: 'runbook.generated', label: 'Runbook generated' },
  { value: 'install.bootstrapped', label: 'Install bootstrapped' },
];


const PAGE_SIZE = 50;


export default function AuditPage_() {
  return (
    <SelfHostedGuard>
      <AdminOrViewer>
        <AuditContent />
      </AdminOrViewer>
    </SelfHostedGuard>
  );
}


function AdminOrViewer({ children }: { children: React.ReactNode }) {
  const { user } = useAuthStore();
  if (!user) return null;
  if (user.role !== 'admin' && user.role !== 'viewer') {
    return (
      <main className="min-h-screen bg-gray-50">
        <div className="container mx-auto max-w-2xl px-4 py-20">
          <div className="rounded-xl border border-amber-200 bg-amber-50 p-8">
            <h1 className="text-2xl font-bold text-amber-900">Not your view</h1>
            <p className="mt-3 text-amber-800">
              The audit log is visible to <strong>admin</strong> and{' '}
              <strong>viewer</strong> roles. Operators can see their own
              actions on each feature page; compliance oversight lives here.
            </p>
          </div>
        </div>
      </main>
    );
  }
  return <>{children}</>;
}


function AuditContent() {
  const [page, setPage] = useState<AuditPage | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');
  const [action, setAction] = useState('');
  const [days, setDays] = useState(30);
  const [offset, setOffset] = useState(0);
  const [accumulated, setAccumulated] = useState<AuditEvent[]>([]);
  const [verifying, setVerifying] = useState(false);
  const [verifyResult, setVerifyResult] = useState<AuditVerifyResult | null>(null);

  async function runVerify() {
    setVerifying(true);
    setVerifyResult(null);
    try {
      setVerifyResult(await verifyAuditChain());
    } catch (e: any) {
      setError(e?.response?.data?.detail || e?.message || 'Verify failed.');
    } finally {
      setVerifying(false);
    }
  }

  async function load(reset: boolean) {
    setLoading(true);
    setError('');
    try {
      const next = await listAuditEvents({
        action: action || undefined,
        days,
        limit: PAGE_SIZE,
        offset: reset ? 0 : offset,
      });
      setPage(next);
      if (reset) {
        setAccumulated(next.items);
        setOffset(next.items.length);
      } else {
        setAccumulated((prev) => [...prev, ...next.items]);
        setOffset((prev) => prev + next.items.length);
      }
    } catch (e: any) {
      setError(e?.response?.data?.detail || e?.message || 'Failed to load.');
    } finally {
      setLoading(false);
    }
  }

  // Reload whenever filter changes.
  useEffect(() => {
    void load(true);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [action, days]);

  const canLoadMore = page && offset < page.total;

  return (
    <main className="min-h-screen bg-gray-50">
      <div className="container mx-auto max-w-6xl px-4 py-12">
        <div className="mb-8 flex items-start justify-between gap-4">
          <div>
            <h1 className="text-3xl font-bold text-gray-900">Audit log</h1>
            <p className="mt-2 text-gray-600">
              Every mutating action on this install: logins, user changes,
              migrations, AI calls, license uploads. Events with identifying
              metadata survive user deletion for compliance.
            </p>
          </div>
          <button
            onClick={runVerify}
            disabled={verifying}
            className="whitespace-nowrap rounded-md border border-gray-300 bg-white px-4 py-2 text-sm font-semibold text-gray-700 shadow-sm hover:bg-gray-50 disabled:opacity-50"
            title="Walk the SHA-256 hash chain and report any row that doesn't match its stored hash."
          >
            {verifying ? 'Verifying…' : 'Verify integrity'}
          </button>
        </div>

        {verifyResult && (
          <div
            className={`mb-6 rounded-lg border p-4 text-sm ${
              verifyResult.ok
                ? 'border-green-200 bg-green-50 text-green-900'
                : 'border-red-200 bg-red-50 text-red-800'
            }`}
          >
            {verifyResult.ok ? (
              <>
                ✓ Chain intact. Verified {verifyResult.checked.toLocaleString()}{' '}
                event{verifyResult.checked === 1 ? '' : 's'}.
              </>
            ) : (
              <>
                <div className="font-semibold">✗ Tampering detected</div>
                <div className="mt-1 font-mono text-xs">
                  First break: row{' '}
                  <span className="font-bold">
                    {verifyResult.first_break?.id.slice(0, 8)}…
                  </span>{' '}
                  ({verifyResult.first_break?.action}) at{' '}
                  {verifyResult.first_break
                    ? new Date(
                        verifyResult.first_break.created_at,
                      ).toLocaleString()
                    : '?'}
                </div>
                <div className="mt-1 text-xs">
                  Either this row was modified or an earlier row was deleted.
                  Compare the DB snapshot to your most recent backup.
                </div>
              </>
            )}
          </div>
        )}

        <div className="mb-6 flex flex-wrap gap-3 rounded-lg border border-gray-200 bg-white p-4 shadow-sm">
          <label className="flex items-center gap-2 text-sm">
            <span className="text-gray-600">Action:</span>
            <select
              value={action}
              onChange={(e) => setAction(e.target.value)}
              className="rounded-md border border-gray-300 px-2 py-1 text-sm"
            >
              {COMMON_ACTIONS.map((o) => (
                <option key={o.value} value={o.value}>
                  {o.label}
                </option>
              ))}
            </select>
          </label>
          <label className="flex items-center gap-2 text-sm">
            <span className="text-gray-600">Window:</span>
            <select
              value={days}
              onChange={(e) => setDays(Number(e.target.value))}
              className="rounded-md border border-gray-300 px-2 py-1 text-sm"
            >
              <option value={1}>last 24 hours</option>
              <option value={7}>last 7 days</option>
              <option value={30}>last 30 days</option>
              <option value={90}>last 90 days</option>
              <option value={365}>last year</option>
            </select>
          </label>
          {page && (
            <span className="ml-auto self-center text-xs text-gray-500">
              Showing {accumulated.length} of {page.total}
            </span>
          )}
        </div>

        {error && (
          <div className="mb-6 rounded-lg border border-red-200 bg-red-50 p-4 text-sm text-red-700">
            {error}
          </div>
        )}

        <EventsTable events={accumulated} />

        <div className="mt-6 flex justify-center">
          {canLoadMore && (
            <button
              onClick={() => load(false)}
              disabled={loading}
              className="rounded-md border border-gray-300 bg-white px-5 py-2 text-sm font-semibold text-gray-700 shadow-sm hover:bg-gray-50 disabled:opacity-50"
            >
              {loading ? 'Loading…' : 'Load more'}
            </button>
          )}
          {!canLoadMore && !loading && page && accumulated.length > 0 && (
            <span className="text-xs text-gray-400">End of log.</span>
          )}
        </div>
      </div>
    </main>
  );
}


function EventsTable({ events }: { events: AuditEvent[] }) {
  if (events.length === 0) {
    return (
      <div className="rounded-lg border border-gray-200 bg-white p-8 text-center text-sm text-gray-500 shadow-sm">
        No events match this filter.
      </div>
    );
  }
  return (
    <div className="overflow-hidden rounded-xl border border-gray-200 bg-white shadow-sm">
      <table className="w-full text-sm">
        <thead className="border-b border-gray-200 bg-gray-50 text-xs uppercase tracking-wide text-gray-500">
          <tr>
            <th className="px-4 py-3 text-left font-semibold">When</th>
            <th className="px-4 py-3 text-left font-semibold">Who</th>
            <th className="px-4 py-3 text-left font-semibold">Action</th>
            <th className="px-4 py-3 text-left font-semibold">Resource</th>
            <th className="px-4 py-3 text-left font-semibold">Details</th>
            <th className="px-4 py-3 text-left font-semibold">IP</th>
          </tr>
        </thead>
        <tbody className="divide-y divide-gray-100">
          {events.map((e) => (
            <tr key={e.id} className="align-top">
              <td className="px-4 py-3 font-mono text-xs text-gray-600 whitespace-nowrap">
                {new Date(e.created_at).toLocaleString()}
              </td>
              <td className="px-4 py-3 text-xs text-gray-700">
                {e.user_email || <span className="italic text-gray-400">system</span>}
              </td>
              <td className="px-4 py-3">
                <span className="rounded-md bg-purple-50 px-2 py-0.5 font-mono text-xs text-purple-900">
                  {e.action}
                </span>
              </td>
              <td className="px-4 py-3 font-mono text-xs text-gray-600">
                {e.resource_type && (
                  <>
                    {e.resource_type}
                    {e.resource_id && `: ${e.resource_id.slice(0, 8)}…`}
                  </>
                )}
              </td>
              <td className="px-4 py-3 font-mono text-xs text-gray-500">
                {e.details ? (
                  <code className="break-all">{JSON.stringify(e.details)}</code>
                ) : (
                  ''
                )}
              </td>
              <td className="px-4 py-3 font-mono text-xs text-gray-500">{e.ip || ''}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

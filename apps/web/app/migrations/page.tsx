'use client';

/**
 * /migrations — list of every migration created on this install.
 *
 * Newest first. Clicking a row goes to /migrations/[id] for config +
 * progress. The "New migration" button is always visible but only
 * actually useful for admin/operator roles — the backend rejects
 * viewer creates with 403. We don't hide the button from viewers
 * because showing a grayed-out CTA is less confusing than a feature
 * that silently disappears.
 */

import { useEffect, useState } from 'react';
import Link from 'next/link';

import SelfHostedGuard from '@/app/components/SelfHostedGuard';
import { listMigrations, MigrationSummary } from '@/app/lib/api';

import { StatusBadge } from './StatusBadge';


export default function MigrationsPage() {
  return (
    <SelfHostedGuard>
      <MigrationsContent />
    </SelfHostedGuard>
  );
}


function MigrationsContent() {
  const [rows, setRows] = useState<MigrationSummary[]>([]);
  const [error, setError] = useState('');
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    (async () => {
      try {
        setRows(await listMigrations());
      } catch (e: any) {
        setError(
          e?.response?.data?.detail || e?.message || 'Failed to load migrations.',
        );
      } finally {
        setLoading(false);
      }
    })();
  }, []);

  return (
    <main className="min-h-screen bg-gray-50">
      <div className="container mx-auto max-w-6xl px-4 py-12">
        <div className="mb-8 flex items-start justify-between">
          <div>
            <h1 className="text-3xl font-bold text-gray-900">Migrations</h1>
            <p className="mt-2 text-gray-600">
              Every migration this install has run. Click a row for per-table
              progress, runbook details, and verification status.
            </p>
          </div>
          <Link
            href="/migrations/new"
            className="rounded-md bg-purple-600 px-5 py-2.5 font-semibold text-white shadow-sm transition hover:bg-purple-700"
          >
            + New migration
          </Link>
        </div>

        {error && (
          <div className="mb-6 rounded-lg border border-red-200 bg-red-50 p-4 text-sm text-red-700">
            {error}
          </div>
        )}

        {loading ? (
          <p className="py-6 text-center text-sm text-gray-500">Loading…</p>
        ) : rows.length === 0 ? (
          <EmptyState />
        ) : (
          <MigrationsTable rows={rows} />
        )}
      </div>
    </main>
  );
}


function EmptyState() {
  return (
    <div className="rounded-xl border border-gray-200 bg-white p-10 text-center shadow-sm">
      <p className="text-lg font-semibold text-gray-900">No migrations yet</p>
      <p className="mt-2 text-sm text-gray-600">
        Create one to introspect an Oracle (or Postgres) schema and stream it
        into a Postgres target.
      </p>
      <Link
        href="/migrations/new"
        className="mt-6 inline-block rounded-md bg-purple-600 px-5 py-2.5 font-semibold text-white shadow-sm transition hover:bg-purple-700"
      >
        Create your first migration →
      </Link>
    </div>
  );
}


function MigrationsTable({ rows }: { rows: MigrationSummary[] }) {
  return (
    <div className="overflow-hidden rounded-xl border border-gray-200 bg-white shadow-sm">
      <table className="w-full text-sm">
        <thead className="border-b border-gray-200 bg-gray-50 text-xs uppercase tracking-wide text-gray-500">
          <tr>
            <th className="px-4 py-3 text-left font-semibold">Name</th>
            <th className="px-4 py-3 text-left font-semibold">Source → target</th>
            <th className="px-4 py-3 text-left font-semibold">Status</th>
            <th className="px-4 py-3 text-right font-semibold">Rows</th>
            <th className="px-4 py-3 text-left font-semibold">Created</th>
          </tr>
        </thead>
        <tbody className="divide-y divide-gray-100">
          {rows.map((r) => (
            <tr key={r.id} className="hover:bg-purple-50/30">
              <td className="px-4 py-3">
                <Link
                  href={`/migrations/${r.id}`}
                  className="font-semibold text-purple-700 hover:underline"
                >
                  {r.name || '(unnamed)'}
                </Link>
              </td>
              <td className="px-4 py-3 font-mono text-xs text-gray-700">
                {r.source_schema ?? '—'}{' '}
                <span className="text-gray-400">→</span>{' '}
                {r.target_schema ?? '—'}
              </td>
              <td className="px-4 py-3">
                <StatusBadge status={r.status} />
              </td>
              <td className="px-4 py-3 text-right font-mono text-xs">
                {r.rows_transferred.toLocaleString()}
              </td>
              <td className="px-4 py-3 text-xs text-gray-600">
                {formatDate(r.created_at)}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}


function formatDate(iso: string): string {
  try {
    return new Date(iso).toLocaleString();
  } catch {
    return iso;
  }
}

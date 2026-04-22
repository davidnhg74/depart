'use client';

/**
 * /migrations/new — create-migration form.
 *
 * Fields match the MigrationCreate Pydantic schema on the backend.
 * A few UX niceties:
 *   * `tables` is a free-text input; empty = migrate every table in
 *     the source schema. Comma-separated when you want a subset.
 *   * Connection strings are stored verbatim in the metadata DB for
 *     v1 — a small privacy note next to the fields sets expectations.
 *     Encryption-at-rest is tracked as follow-up work.
 *   * On success we redirect to /migrations/[id] where the operator
 *     clicks "Run" to actually kick off the data movement.
 */

import { useState } from 'react';
import { useRouter } from 'next/navigation';
import Link from 'next/link';

import SelfHostedGuard from '@/app/components/SelfHostedGuard';
import {
  ConnectionTestResult,
  createMigration,
  testConnection,
} from '@/app/lib/api';


export default function NewMigrationPage() {
  return (
    <SelfHostedGuard>
      <NewMigrationContent />
    </SelfHostedGuard>
  );
}


function NewMigrationContent() {
  const router = useRouter();
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState('');

  const [name, setName] = useState('');
  const [sourceUrl, setSourceUrl] = useState(
    'oracle+oracledb://user:pw@host:1521/?service_name=ORCL',
  );
  const [targetUrl, setTargetUrl] = useState(
    'postgresql+psycopg://user:pw@host:5432/dbname',
  );
  const [sourceSchema, setSourceSchema] = useState('HR');
  const [targetSchema, setTargetSchema] = useState('public');
  const [tables, setTables] = useState('');
  const [batchSize, setBatchSize] = useState(5000);
  const [createTables, setCreateTables] = useState(false);

  async function submit(e: React.FormEvent) {
    e.preventDefault();
    setError('');
    setSaving(true);
    const parsedTables = tables
      .split(',')
      .map((t) => t.trim())
      .filter(Boolean);
    try {
      const created = await createMigration({
        name,
        source_url: sourceUrl,
        target_url: targetUrl,
        source_schema: sourceSchema,
        target_schema: targetSchema,
        tables: parsedTables.length > 0 ? parsedTables : null,
        batch_size: batchSize,
        create_tables: createTables,
      });
      router.push(`/migrations/${created.id}`);
    } catch (e: any) {
      setError(e?.response?.data?.detail || e?.message || 'Create failed.');
    } finally {
      setSaving(false);
    }
  }

  return (
    <main className="min-h-screen bg-gray-50">
      <div className="container mx-auto max-w-3xl px-4 py-12">
        <div className="mb-6 text-sm">
          <Link href="/migrations" className="text-purple-700 hover:underline">
            ← Back to migrations
          </Link>
        </div>

        <h1 className="text-3xl font-bold text-gray-900">New migration</h1>
        <p className="mt-2 text-gray-600">
          depart introspects the source, plans load order, and streams rows
          into the target with Merkle verification. You can edit everything
          below until you click <strong>Run</strong> on the next screen.
        </p>

        {error && (
          <div className="mt-6 rounded-lg border border-red-200 bg-red-50 p-4 text-sm text-red-700">
            {error}
          </div>
        )}

        <form
          onSubmit={submit}
          className="mt-8 space-y-6 rounded-xl border border-gray-200 bg-white p-8 shadow-sm"
        >
          <Field label="Migration name" hint="A friendly label for your records.">
            <input
              type="text"
              value={name}
              onChange={(e) => setName(e.target.value)}
              required
              placeholder="acme-q2-2026"
              className="w-full rounded-md border border-gray-300 px-3 py-2 text-sm"
            />
          </Field>

          <div className="grid grid-cols-1 gap-6 md:grid-cols-2">
            <Field
              label="Source URL"
              hint="SQLAlchemy DSN — oracle+oracledb://… or postgresql+psycopg://…"
            >
              <ConnectableInput
                value={sourceUrl}
                onChange={setSourceUrl}
                schema={sourceSchema}
                required
              />
            </Field>
            <Field label="Target URL" hint="Must be a Postgres target.">
              <ConnectableInput
                value={targetUrl}
                onChange={setTargetUrl}
                schema={targetSchema}
                required
              />
            </Field>
          </div>

          <div className="grid grid-cols-1 gap-6 md:grid-cols-2">
            <Field label="Source schema">
              <input
                type="text"
                value={sourceSchema}
                onChange={(e) => setSourceSchema(e.target.value)}
                required
                placeholder="HR"
                className="w-full rounded-md border border-gray-300 px-3 py-2 text-sm"
              />
            </Field>
            <Field label="Target schema">
              <input
                type="text"
                value={targetSchema}
                onChange={(e) => setTargetSchema(e.target.value)}
                required
                placeholder="public"
                className="w-full rounded-md border border-gray-300 px-3 py-2 text-sm"
              />
            </Field>
          </div>

          <Field
            label="Tables (optional)"
            hint="Comma-separated subset. Leave empty to migrate every table in the source schema that has a primary key."
          >
            <input
              type="text"
              value={tables}
              onChange={(e) => setTables(e.target.value)}
              placeholder="EMPLOYEES, DEPARTMENTS, JOBS"
              className="w-full rounded-md border border-gray-300 px-3 py-2 text-sm"
            />
          </Field>

          <div className="grid grid-cols-1 gap-6 md:grid-cols-2">
            <Field label="Batch size">
              <input
                type="number"
                min={1}
                max={500_000}
                value={batchSize}
                onChange={(e) => setBatchSize(Number(e.target.value))}
                className="w-full rounded-md border border-gray-300 px-3 py-2 text-sm"
              />
            </Field>
            <Field label="Create target tables">
              <label className="flex items-center gap-2 text-sm text-gray-700">
                <input
                  type="checkbox"
                  checked={createTables}
                  onChange={(e) => setCreateTables(e.target.checked)}
                  className="h-4 w-4 rounded border-gray-300 text-purple-600 focus:ring-purple-400"
                />
                Emit CREATE TABLE IF NOT EXISTS from introspected schema
              </label>
            </Field>
          </div>

          <p className="rounded-md border border-amber-200 bg-amber-50 p-3 text-xs text-amber-900">
            ⚠ Connection strings, including passwords, are stored verbatim in
            the local Postgres metadata DB so the runner can re-connect on
            resume. Keep this install on a trusted network until we ship
            per-field encryption (tracked as follow-up).
          </p>

          <div className="flex items-center justify-end gap-3">
            <Link
              href="/migrations"
              className="rounded-md px-4 py-2 text-sm text-gray-600 hover:bg-gray-100"
            >
              Cancel
            </Link>
            <button
              type="submit"
              disabled={saving}
              className="rounded-md bg-purple-600 px-6 py-2 font-semibold text-white shadow-sm transition hover:bg-purple-700 disabled:bg-gray-300"
            >
              {saving ? 'Creating…' : 'Create migration'}
            </button>
          </div>
        </form>
      </div>
    </main>
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


/**
 * Text input + inline "Test" button. The operator is going to paste a
 * DSN that might or might not work; giving them a one-click probe
 * before they submit the whole form is worth the 30 lines of UI.
 */
function ConnectableInput({
  value,
  onChange,
  schema,
  required,
}: {
  value: string;
  onChange: (v: string) => void;
  schema: string;
  required?: boolean;
}) {
  const [testing, setTesting] = useState(false);
  const [result, setResult] = useState<ConnectionTestResult | null>(null);

  async function run() {
    if (!value.trim()) return;
    setTesting(true);
    setResult(null);
    try {
      setResult(await testConnection(value, schema));
    } catch (e: any) {
      setResult({
        ok: false,
        dialect: null,
        message: e?.response?.data?.detail || e?.message || 'Test failed',
        schema: schema || null,
        tables_found: null,
      });
    } finally {
      setTesting(false);
    }
  }

  return (
    <div>
      <div className="flex gap-2">
        <input
          type="text"
          value={value}
          onChange={(e) => onChange(e.target.value)}
          required={required}
          className="w-full rounded-md border border-gray-300 px-3 py-2 font-mono text-xs"
        />
        <button
          type="button"
          onClick={run}
          disabled={testing || !value.trim()}
          className="whitespace-nowrap rounded-md border border-gray-300 bg-white px-3 py-2 text-xs font-semibold text-gray-700 hover:bg-gray-50 disabled:opacity-50"
        >
          {testing ? 'Testing…' : 'Test'}
        </button>
      </div>
      {result && (
        <p
          className={`mt-1.5 text-xs ${
            result.ok ? 'text-green-700' : 'text-red-700'
          }`}
        >
          {result.ok ? '✓ ' : '✗ '}
          {result.message}
          {result.ok && result.tables_found != null && (
            <> · {result.tables_found} tables in <code>{result.schema}</code></>
          )}
        </p>
      )}
    </div>
  );
}

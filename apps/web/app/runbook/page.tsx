'use client';

/**
 * /runbook — generate a migration runbook PDF or JSON.
 *
 * Driven by the existing multipart endpoint at
 * /api/v3/projects/runbook. PDF output is gated server-side behind a
 * Pro license (`runbook_pdf` feature flag); JSON is available on
 * Community tier as a preview.
 *
 * Two optional ZIP inputs:
 *   * schema_zip — Oracle DDL (required)
 *   * source_zip — application source (Java / Python / etc). When
 *     present, the app-impact analyzer walks it for cross-language
 *     references to migrated objects.
 *
 * Success path:
 *   * PDF → immediate browser download
 *   * JSON → rendered on the page as a structured summary
 */

import { useState } from 'react';

import SelfHostedGuard from '@/app/components/SelfHostedGuard';
import { apiBaseUrl } from '@/app/lib/api';
import { useAuthStore } from '@/app/store/authStore';


export default function RunbookPage() {
  return (
    <SelfHostedGuard>
      <AdminOrOperator>
        <RunbookContent />
      </AdminOrOperator>
    </SelfHostedGuard>
  );
}


function AdminOrOperator({ children }: { children: React.ReactNode }) {
  const { user } = useAuthStore();
  if (!user) return null;
  if (user.role !== 'admin' && user.role !== 'operator') {
    return (
      <main className="min-h-screen bg-gray-50">
        <div className="container mx-auto max-w-2xl px-4 py-20">
          <div className="rounded-xl border border-amber-200 bg-amber-50 p-8">
            <h1 className="text-2xl font-bold text-amber-900">
              Operators and admins only
            </h1>
            <p className="mt-3 text-amber-800">
              Runbook generation is a mutating action — it writes to the audit
              log, and the PDF variant consumes license tokens. Viewers can
              see past runbooks on individual migration detail pages.
            </p>
          </div>
        </div>
      </main>
    );
  }
  return <>{children}</>;
}


function RunbookContent() {
  const [projectName, setProjectName] = useState('');
  const [customer, setCustomer] = useState('');
  const [sourceVersion, setSourceVersion] = useState('Oracle 19c');
  const [targetVersion, setTargetVersion] = useState('PostgreSQL 16');
  const [cutoverWindow, setCutoverWindow] = useState('TBD');
  const [ratePerDay, setRatePerDay] = useState(1500);
  const [explain, setExplain] = useState(false);
  const [format, setFormat] = useState<'pdf' | 'json'>('pdf');
  const [schemaZip, setSchemaZip] = useState<File | null>(null);
  const [sourceZip, setSourceZip] = useState<File | null>(null);

  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState('');
  const [jsonResult, setJsonResult] = useState<unknown | null>(null);

  async function submit(e: React.FormEvent) {
    e.preventDefault();
    setError('');
    setJsonResult(null);
    if (!schemaZip) {
      setError('Schema zip is required.');
      return;
    }
    setSubmitting(true);
    try {
      const form = new FormData();
      form.append('project_name', projectName);
      form.append('customer', customer);
      form.append('source_version', sourceVersion);
      form.append('target_version', targetVersion);
      form.append('cutover_window', cutoverWindow);
      form.append('rate_per_day', String(ratePerDay));
      form.append('explain', String(explain));
      form.append('format', format);
      form.append('schema_zip', schemaZip);
      if (sourceZip) form.append('source_zip', sourceZip);

      const token = getToken();
      const resp = await fetch(`${apiBaseUrl()}/api/v3/projects/runbook`, {
        method: 'POST',
        headers: token ? { Authorization: `Bearer ${token}` } : {},
        body: form,
      });

      if (!resp.ok) {
        const text = await resp.text();
        // 402 license error has a structured body; surface its 'reason'
        try {
          const parsed = JSON.parse(text);
          if (parsed?.detail?.reason) {
            throw new Error(parsed.detail.reason);
          }
        } catch {
          /* fall through */
        }
        throw new Error(`HTTP ${resp.status}: ${text.slice(0, 300)}`);
      }

      if (format === 'pdf') {
        const blob = await resp.blob();
        const url = URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;
        a.download = `runbook-${customer.replace(/\s+/g, '_') || 'depart'}.pdf`;
        document.body.appendChild(a);
        a.click();
        a.remove();
        URL.revokeObjectURL(url);
      } else {
        setJsonResult(await resp.json());
      }
    } catch (e: any) {
      setError(e?.message || 'Generation failed.');
    } finally {
      setSubmitting(false);
    }
  }

  return (
    <main className="min-h-screen bg-gray-50">
      <div className="container mx-auto max-w-3xl px-4 py-12">
        <h1 className="text-3xl font-bold text-gray-900">Generate runbook</h1>
        <p className="mt-2 text-gray-600">
          Produces a phased migration plan — load order, risk flags, sign-off
          checklist — from your introspected schema. PDF output requires a Pro
          license; JSON is available on the free tier so you can preview
          without paying.
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
          <div className="grid grid-cols-1 gap-6 md:grid-cols-2">
            <Field label="Project name">
              <input
                type="text"
                value={projectName}
                onChange={(e) => setProjectName(e.target.value)}
                required
                placeholder="acme-oracle-exit"
                className="input"
              />
            </Field>
            <Field label="Customer">
              <input
                type="text"
                value={customer}
                onChange={(e) => setCustomer(e.target.value)}
                required
                placeholder="Acme Corp"
                className="input"
              />
            </Field>
            <Field label="Source version">
              <input
                type="text"
                value={sourceVersion}
                onChange={(e) => setSourceVersion(e.target.value)}
                className="input"
              />
            </Field>
            <Field label="Target version">
              <input
                type="text"
                value={targetVersion}
                onChange={(e) => setTargetVersion(e.target.value)}
                className="input"
              />
            </Field>
            <Field label="Cutover window">
              <input
                type="text"
                value={cutoverWindow}
                onChange={(e) => setCutoverWindow(e.target.value)}
                placeholder="Sat 2026-06-01 02:00-06:00 UTC"
                className="input"
              />
            </Field>
            <Field label="Rate ($/day)" hint="Used to estimate total cost.">
              <input
                type="number"
                min={100}
                max={10000}
                value={ratePerDay}
                onChange={(e) => setRatePerDay(Number(e.target.value))}
                className="input"
              />
            </Field>
          </div>

          <Field label="Schema zip (required)" hint="A zip of .sql / .pls / .plsql files.">
            <input
              type="file"
              accept=".zip"
              onChange={(e) => setSchemaZip(e.target.files?.[0] ?? null)}
              required
              className="text-sm"
            />
          </Field>

          <Field
            label="Source code zip (optional)"
            hint="Application source. Zip of Java / Python / etc. Runs the app-impact analyzer for cross-language references."
          >
            <input
              type="file"
              accept=".zip"
              onChange={(e) => setSourceZip(e.target.files?.[0] ?? null)}
              className="text-sm"
            />
          </Field>

          <div className="grid grid-cols-1 gap-6 md:grid-cols-2">
            <Field label="AI narrative">
              <label className="flex items-center gap-2 text-sm text-gray-700">
                <input
                  type="checkbox"
                  checked={explain}
                  onChange={(e) => setExplain(e.target.checked)}
                  className="h-4 w-4 rounded border-gray-300 text-purple-600"
                />
                Add executive summary + risk narrative (uses your Anthropic key)
              </label>
            </Field>
            <Field label="Output format">
              <div className="flex gap-4 text-sm text-gray-700">
                <label className="flex items-center gap-2">
                  <input
                    type="radio"
                    name="format"
                    value="pdf"
                    checked={format === 'pdf'}
                    onChange={() => setFormat('pdf')}
                  />
                  PDF (Pro)
                </label>
                <label className="flex items-center gap-2">
                  <input
                    type="radio"
                    name="format"
                    value="json"
                    checked={format === 'json'}
                    onChange={() => setFormat('json')}
                  />
                  JSON (free)
                </label>
              </div>
            </Field>
          </div>

          <div className="flex justify-end">
            <button
              type="submit"
              disabled={submitting}
              className="rounded-md bg-purple-600 px-6 py-2 font-semibold text-white shadow-sm transition hover:bg-purple-700 disabled:bg-gray-300"
            >
              {submitting ? 'Generating…' : 'Generate runbook'}
            </button>
          </div>
        </form>

        {jsonResult !== null && (
          <section className="mt-8 rounded-xl border border-gray-200 bg-white p-6 shadow-sm">
            <h2 className="mb-3 text-lg font-semibold text-gray-900">Result</h2>
            <pre className="max-h-[60vh] overflow-auto rounded-md bg-gray-50 p-4 text-xs text-gray-800">
{JSON.stringify(jsonResult, null, 2)}
            </pre>
          </section>
        )}
      </div>

      <style jsx>{`
        :global(.input) {
          width: 100%;
          border-radius: 0.375rem;
          border: 1px solid #d1d5db;
          padding: 0.5rem 0.75rem;
          font-size: 0.875rem;
        }
      `}</style>
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


function getToken(): string | null {
  if (typeof document === 'undefined') return null;
  const match = document.cookie
    .split(';')
    .map((s) => s.trim())
    .find((s) => s.startsWith('access_token='));
  return match ? decodeURIComponent(match.split('=')[1]) : null;
}

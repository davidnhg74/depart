'use client';

/**
 * Masking panel on the migration detail page.
 *
 * Lists current per-column rules as a flat table and provides an
 * editor for adding/removing rules. "Preview" button hits the
 * preview endpoint and shows masked sample rows below. Only masked
 * values are shown — we never round-trip original PII through the
 * product for a preview.
 */

import { useEffect, useState } from 'react';
import Link from 'next/link';

import {
  MaskingPreview,
  MaskingRule,
  MaskingRules,
  MaskingStrategy,
  deleteMasking,
  getMasking,
  previewMasking,
  putMasking,
} from '@/app/lib/api';
import { useAuthStore } from '@/app/store/authStore';


const STRATEGIES: { value: MaskingStrategy; label: string; hint: string }[] = [
  { value: 'null', label: 'null', hint: 'replace with NULL' },
  { value: 'fixed', label: 'fixed', hint: 'replace with a constant string' },
  {
    value: 'hash',
    label: 'hash',
    hint: 'HMAC-SHA256 (deterministic, preserves FK joins)',
  },
  { value: 'partial', label: 'partial', hint: 'keep first N + last M chars' },
  { value: 'regex', label: 'regex', hint: 'pattern + replacement' },
];


type LoadState =
  | { kind: 'loading' }
  | { kind: 'ok'; rules: MaskingRules }
  | { kind: 'unlicensed' }
  | { kind: 'error'; message: string };


export default function MaskingPanel({ migrationId }: { migrationId: string }) {
  const { user } = useAuthStore();
  const isAdmin = user?.role === 'admin';
  const [state, setState] = useState<LoadState>({ kind: 'loading' });
  const [editing, setEditing] = useState(false);
  const [preview, setPreview] = useState<MaskingPreview | null>(null);
  const [previewing, setPreviewing] = useState(false);
  const [previewErr, setPreviewErr] = useState<string | null>(null);

  async function refresh() {
    setState({ kind: 'loading' });
    try {
      const rules = await getMasking(migrationId);
      setState({ kind: 'ok', rules });
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
  }, [migrationId]);

  async function doPreview() {
    setPreviewing(true);
    setPreviewErr(null);
    try {
      const result = await previewMasking(migrationId, 5);
      setPreview(result);
    } catch (e: any) {
      setPreviewErr(
        e?.response?.data?.detail || e?.message || 'Preview failed.',
      );
    } finally {
      setPreviewing(false);
    }
  }

  async function clearRules() {
    if (!window.confirm('Remove all masking rules from this migration?')) return;
    try {
      await deleteMasking(migrationId);
      setPreview(null);
      await refresh();
    } catch (e: any) {
      setPreviewErr(e?.response?.data?.detail || e?.message || 'Delete failed.');
    }
  }

  const ruleCount =
    state.kind === 'ok'
      ? Object.values(state.rules).reduce((n, cols) => n + Object.keys(cols).length, 0)
      : 0;

  return (
    <section className="mt-6 rounded-xl border border-gray-200 bg-white p-5">
      <div className="flex items-center justify-between">
        <div>
          <h2 className="text-lg font-semibold text-gray-900">Data masking</h2>
          <p className="mt-1 text-sm text-gray-600">
            Redact PII per-column during the migration. The same value
            always masks to the same output with <code>hash</code> so
            foreign keys still join after masking.
          </p>
        </div>
        {state.kind === 'ok' && isAdmin && !editing && (
          <div className="flex gap-2">
            {ruleCount > 0 && (
              <button
                onClick={doPreview}
                disabled={previewing}
                className="rounded border border-gray-300 bg-white px-3 py-1 text-xs font-medium text-gray-700 hover:bg-gray-50 disabled:opacity-50"
              >
                {previewing ? 'Previewing…' : 'Preview'}
              </button>
            )}
            <button
              onClick={() => setEditing(true)}
              className={
                ruleCount === 0
                  ? 'rounded-lg bg-purple-600 px-3 py-1.5 text-xs font-medium text-white hover:bg-purple-700'
                  : 'rounded border border-gray-300 bg-white px-3 py-1 text-xs font-medium text-gray-700 hover:bg-gray-50'
              }
            >
              {ruleCount === 0 ? 'Configure masking' : 'Edit rules'}
            </button>
            {ruleCount > 0 && (
              <button
                onClick={clearRules}
                className="rounded border border-red-300 bg-white px-3 py-1 text-xs font-medium text-red-700 hover:bg-red-50"
              >
                Clear
              </button>
            )}
          </div>
        )}
      </div>

      {state.kind === 'loading' && (
        <p className="mt-3 text-sm text-gray-500">Loading…</p>
      )}

      {state.kind === 'unlicensed' && (
        <div className="mt-3 rounded-lg border border-amber-200 bg-amber-50 p-3 text-sm text-amber-800">
          Data masking requires a Pro license with the{' '}
          <code className="rounded bg-amber-100 px-1">data_masking</code>{' '}
          feature.{' '}
          <Link href="/settings/instance" className="underline">
            Manage license →
          </Link>
        </div>
      )}

      {state.kind === 'error' && (
        <div className="mt-3 rounded-lg border border-red-200 bg-red-50 p-3 text-sm text-red-700">
          {state.message}
        </div>
      )}

      {state.kind === 'ok' && !editing && ruleCount === 0 && (
        <p className="mt-3 text-sm text-gray-600">
          No rules configured — all columns migrate unchanged.
        </p>
      )}

      {state.kind === 'ok' && !editing && ruleCount > 0 && (
        <RulesTable rules={state.rules} />
      )}

      {editing && (
        <RulesEditor
          migrationId={migrationId}
          initial={state.kind === 'ok' ? state.rules : {}}
          onCancel={() => setEditing(false)}
          onSaved={() => {
            setEditing(false);
            setPreview(null);
            void refresh();
          }}
        />
      )}

      {previewErr && (
        <div className="mt-3 rounded-lg border border-red-200 bg-red-50 p-3 text-sm text-red-700">
          {previewErr}
        </div>
      )}

      {preview && !editing && <PreviewPane preview={preview} />}
    </section>
  );
}


function RulesTable({ rules }: { rules: MaskingRules }) {
  const flat: Array<{ table: string; column: string; rule: MaskingRule }> = [];
  for (const [table, cols] of Object.entries(rules)) {
    for (const [column, rule] of Object.entries(cols)) {
      flat.push({ table, column, rule });
    }
  }
  return (
    <div className="mt-3 overflow-x-auto">
      <table className="w-full text-sm">
        <thead>
          <tr className="border-b border-gray-200 text-left text-xs uppercase tracking-wide text-gray-500">
            <th className="py-2 pr-4">Table</th>
            <th className="py-2 pr-4">Column</th>
            <th className="py-2 pr-4">Strategy</th>
            <th className="py-2">Options</th>
          </tr>
        </thead>
        <tbody>
          {flat.map((r, i) => (
            <tr key={i} className="border-b border-gray-100">
              <td className="py-2 pr-4 font-mono">{r.table}</td>
              <td className="py-2 pr-4 font-mono">{r.column}</td>
              <td className="py-2 pr-4">
                <span className="rounded bg-purple-50 px-2 py-0.5 text-xs text-purple-800">
                  {r.rule.strategy}
                </span>
              </td>
              <td className="py-2 text-xs text-gray-600">{summarizeOpts(r.rule)}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}


function summarizeOpts(rule: MaskingRule): string {
  switch (rule.strategy) {
    case 'fixed':
      return `value: ${rule.value ?? '[REDACTED]'}`;
    case 'hash':
      return `length: ${rule.length ?? 32}`;
    case 'partial':
      return `keep_first=${rule.keep_first ?? 0}, keep_last=${rule.keep_last ?? 0}`;
    case 'regex':
      return `${rule.pattern} → ${rule.replacement ?? ''}`;
    default:
      return '—';
  }
}


function RulesEditor({
  migrationId,
  initial,
  onCancel,
  onSaved,
}: {
  migrationId: string;
  initial: MaskingRules;
  onCancel: () => void;
  onSaved: () => void;
}) {
  type Row = { table: string; column: string; rule: MaskingRule };
  const [rows, setRows] = useState<Row[]>(() => {
    const out: Row[] = [];
    for (const [table, cols] of Object.entries(initial)) {
      for (const [column, rule] of Object.entries(cols)) {
        out.push({ table, column, rule: { ...rule } });
      }
    }
    return out;
  });
  const [saving, setSaving] = useState(false);
  const [err, setErr] = useState<string | null>(null);

  function updateRow(i: number, patch: Partial<Row>) {
    setRows((prev) => prev.map((r, idx) => (idx === i ? { ...r, ...patch } : r)));
  }
  function updateRule(i: number, patch: Partial<MaskingRule>) {
    setRows((prev) =>
      prev.map((r, idx) =>
        idx === i ? { ...r, rule: { ...r.rule, ...patch } } : r,
      ),
    );
  }
  function addRow() {
    setRows((prev) => [
      ...prev,
      { table: '', column: '', rule: { strategy: 'null' } },
    ]);
  }
  function removeRow(i: number) {
    setRows((prev) => prev.filter((_, idx) => idx !== i));
  }

  async function save() {
    setSaving(true);
    setErr(null);
    try {
      const nested: MaskingRules = {};
      for (const { table, column, rule } of rows) {
        if (!table.trim() || !column.trim()) {
          throw new Error('Every row needs a table and column.');
        }
        if (!nested[table]) nested[table] = {};
        nested[table][column] = rule;
      }
      await putMasking(migrationId, nested);
      onSaved();
    } catch (e: any) {
      setErr(
        e?.response?.data?.detail ||
          (typeof e?.message === 'string' ? e.message : 'Save failed.'),
      );
    } finally {
      setSaving(false);
    }
  }

  return (
    <div className="mt-4 space-y-3">
      <div className="overflow-x-auto">
        <table className="w-full text-sm">
          <thead>
            <tr className="border-b border-gray-200 text-left text-xs uppercase tracking-wide text-gray-500">
              <th className="py-2 pr-2">Table</th>
              <th className="py-2 pr-2">Column</th>
              <th className="py-2 pr-2">Strategy</th>
              <th className="py-2 pr-2">Options</th>
              <th className="py-2"></th>
            </tr>
          </thead>
          <tbody>
            {rows.map((r, i) => (
              <tr key={i} className="border-b border-gray-100 align-top">
                <td className="py-2 pr-2">
                  <input
                    value={r.table}
                    onChange={(e) => updateRow(i, { table: e.target.value })}
                    placeholder="HR.EMPLOYEES"
                    className="w-full rounded border border-gray-300 px-2 py-1 font-mono text-xs"
                  />
                </td>
                <td className="py-2 pr-2">
                  <input
                    value={r.column}
                    onChange={(e) => updateRow(i, { column: e.target.value })}
                    placeholder="EMAIL"
                    className="w-full rounded border border-gray-300 px-2 py-1 font-mono text-xs"
                  />
                </td>
                <td className="py-2 pr-2">
                  <select
                    value={r.rule.strategy}
                    onChange={(e) =>
                      updateRule(i, {
                        strategy: e.target.value as MaskingStrategy,
                      })
                    }
                    className="rounded border border-gray-300 px-2 py-1 text-xs"
                  >
                    {STRATEGIES.map((s) => (
                      <option key={s.value} value={s.value}>
                        {s.label}
                      </option>
                    ))}
                  </select>
                </td>
                <td className="py-2 pr-2 text-xs">
                  <StrategyOpts rule={r.rule} onChange={(p) => updateRule(i, p)} />
                </td>
                <td className="py-2">
                  <button
                    onClick={() => removeRow(i)}
                    className="text-xs text-red-600 hover:underline"
                  >
                    remove
                  </button>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      <button
        onClick={addRow}
        className="rounded border border-dashed border-gray-400 bg-white px-3 py-1.5 text-xs font-medium text-gray-700 hover:bg-gray-50"
      >
        + Add rule
      </button>

      {err && (
        <div className="rounded-lg border border-red-200 bg-red-50 p-3 text-sm text-red-700">
          {err}
        </div>
      )}

      <div className="flex gap-2 pt-1">
        <button
          onClick={save}
          disabled={saving}
          className="rounded-lg bg-purple-600 px-4 py-2 text-sm font-medium text-white hover:bg-purple-700 disabled:opacity-50"
        >
          {saving ? 'Saving…' : 'Save rules'}
        </button>
        <button
          onClick={onCancel}
          disabled={saving}
          className="rounded-lg border border-gray-300 bg-white px-4 py-2 text-sm font-medium text-gray-700 hover:bg-gray-50"
        >
          Cancel
        </button>
      </div>
    </div>
  );
}


function StrategyOpts({
  rule,
  onChange,
}: {
  rule: MaskingRule;
  onChange: (patch: Partial<MaskingRule>) => void;
}) {
  if (rule.strategy === 'fixed') {
    return (
      <input
        value={rule.value ?? ''}
        onChange={(e) => onChange({ value: e.target.value })}
        placeholder="[REDACTED]"
        className="w-40 rounded border border-gray-300 px-2 py-1 font-mono text-xs"
      />
    );
  }
  if (rule.strategy === 'hash') {
    return (
      <label className="inline-flex items-center gap-1 text-xs text-gray-700">
        length
        <input
          type="number"
          min={1}
          max={64}
          value={rule.length ?? 32}
          onChange={(e) => onChange({ length: Number(e.target.value) })}
          className="w-16 rounded border border-gray-300 px-2 py-1 text-xs"
        />
      </label>
    );
  }
  if (rule.strategy === 'partial') {
    return (
      <div className="flex gap-2 text-xs text-gray-700">
        <label className="inline-flex items-center gap-1">
          first
          <input
            type="number"
            min={0}
            value={rule.keep_first ?? 0}
            onChange={(e) =>
              onChange({ keep_first: Number(e.target.value) })
            }
            className="w-14 rounded border border-gray-300 px-2 py-1"
          />
        </label>
        <label className="inline-flex items-center gap-1">
          last
          <input
            type="number"
            min={0}
            value={rule.keep_last ?? 0}
            onChange={(e) =>
              onChange({ keep_last: Number(e.target.value) })
            }
            className="w-14 rounded border border-gray-300 px-2 py-1"
          />
        </label>
      </div>
    );
  }
  if (rule.strategy === 'regex') {
    return (
      <div className="flex flex-col gap-1 text-xs">
        <input
          value={rule.pattern ?? ''}
          onChange={(e) => onChange({ pattern: e.target.value })}
          placeholder="\\d{3}-\\d{2}-\\d{4}"
          className="w-48 rounded border border-gray-300 px-2 py-1 font-mono"
        />
        <input
          value={rule.replacement ?? ''}
          onChange={(e) => onChange({ replacement: e.target.value })}
          placeholder="XXX-XX-XXXX"
          className="w-48 rounded border border-gray-300 px-2 py-1 font-mono"
        />
      </div>
    );
  }
  return <span className="text-gray-400">—</span>;
}


function PreviewPane({ preview }: { preview: MaskingPreview }) {
  return (
    <div className="mt-5 border-t border-gray-200 pt-4">
      <h3 className="text-sm font-semibold text-gray-900">
        Preview (masked rows only)
      </h3>
      <p className="mt-1 text-xs text-gray-500">
        Sampled from the source; original PII never leaves your database.
      </p>
      {Object.keys(preview.errors).length > 0 && (
        <div className="mt-2 rounded-lg border border-red-200 bg-red-50 p-3 text-xs text-red-700">
          {Object.entries(preview.errors).map(([table, msg]) => (
            <div key={table}>
              <span className="font-mono">{table}</span>: {msg}
            </div>
          ))}
        </div>
      )}
      <div className="mt-3 space-y-4">
        {Object.entries(preview.samples).map(([table, rows]) => (
          <div key={table}>
            <div className="text-xs font-semibold text-gray-700">
              <code className="rounded bg-gray-100 px-1">{table}</code>
            </div>
            {rows.length === 0 ? (
              <p className="mt-1 text-xs text-gray-500">(no rows)</p>
            ) : (
              <div className="mt-1 overflow-x-auto">
                <table className="min-w-full text-xs">
                  <thead>
                    <tr className="border-b border-gray-200 text-left text-gray-500">
                      {Object.keys(rows[0]).map((c) => (
                        <th key={c} className="py-1 pr-4 font-mono">
                          {c}
                        </th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {rows.map((row, i) => (
                      <tr key={i} className="border-b border-gray-100">
                        {Object.keys(rows[0]).map((c) => (
                          <td
                            key={c}
                            className="py-1 pr-4 font-mono text-gray-700"
                          >
                            {row[c] === null ? (
                              <span className="text-gray-400">NULL</span>
                            ) : (
                              String(row[c])
                            )}
                          </td>
                        ))}
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
          </div>
        ))}
      </div>
    </div>
  );
}

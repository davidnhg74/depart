/**
 * Render the per-file findings of an AppImpactResponse, grouped by file
 * and sorted within each file by risk descending. Optional risk filter
 * lets the customer slice "show me the blockers" quickly.
 */
'use client';

import React, { useMemo, useState } from 'react';

import RiskBadge, { Risk } from './RiskBadge';

export interface Finding {
  code: string;
  risk: Risk;
  message: string;
  suggestion: string;
  file: string;
  line: number;
  snippet: string;
  schema_objects?: string[];
  construct_tags?: string[];
  explanation?: string | null;
  before?: string | null;
  after?: string | null;
  caveats?: string[];
}

export interface FileImpact {
  file: string;
  language: string;
  fragments_scanned: number;
  findings: Finding[];
  max_risk: Risk;
}

export interface AppImpactResponse {
  files: FileImpact[];
  total_files_scanned: number;
  total_fragments: number;
  total_findings: number;
  findings_by_risk: Partial<Record<Risk, number>>;
  schema_objects_scanned: number;
  explained: boolean;
  explanations_generated?: number;
  explanations_failed?: number;
}

const RISK_RANK: Record<Risk, number> = {
  critical: 3, high: 2, medium: 1, low: 0,
};

const RISKS: Risk[] = ['critical', 'high', 'medium', 'low'];

interface Props {
  report: AppImpactResponse;
}

export default function FindingsList({ report }: Props) {
  const [filter, setFilter] = useState<Risk | 'all'>('all');

  const files = useMemo(() => {
    if (filter === 'all') return report.files;
    return report.files
      .map((fi) => ({ ...fi, findings: fi.findings.filter((f) => f.risk === filter) }))
      .filter((fi) => fi.findings.length > 0);
  }, [report.files, filter]);

  return (
    <div data-testid="findings-list" className="space-y-6">
      <header className="flex flex-wrap items-center justify-between gap-4 bg-white border border-gray-200 rounded-lg p-4">
        <div className="text-sm text-gray-700">
          <span className="font-semibold">{report.total_findings}</span> findings across{' '}
          <span className="font-semibold">{report.total_files_scanned}</span> files
          ({report.total_fragments} SQL fragments scanned, {report.schema_objects_scanned} schema objects)
          {report.explained && (
            <span className="ml-2 inline-flex items-center px-2 py-0.5 rounded-md text-xs font-medium bg-purple-100 text-purple-800">
              AI explanations: {report.explanations_generated}/{(report.explanations_generated || 0) + (report.explanations_failed || 0)}
            </span>
          )}
        </div>

        <div className="flex items-center gap-2 text-sm">
          <button
            onClick={() => setFilter('all')}
            className={`px-2 py-1 rounded ${filter === 'all' ? 'bg-gray-900 text-white' : 'bg-gray-100 text-gray-700'}`}
          >
            All
          </button>
          {RISKS.map((r) => (
            <button
              key={r}
              onClick={() => setFilter(r)}
              data-testid={`filter-${r}`}
              className={`px-2 py-1 rounded ${filter === r ? 'bg-gray-900 text-white' : 'bg-gray-100 text-gray-700'}`}
            >
              <RiskBadge risk={r} className="mr-1" />
              {report.findings_by_risk[r] ?? 0}
            </button>
          ))}
        </div>
      </header>

      {files.length === 0 && (
        <div className="bg-white border border-gray-200 rounded-lg p-6 text-center text-gray-500">
          No findings at this filter.
        </div>
      )}

      {files.map((fi) => (
        <FileCard key={fi.file} fi={fi} />
      ))}
    </div>
  );
}

function FileCard({ fi }: { fi: FileImpact }) {
  const sorted = useMemo(
    () => [...fi.findings].sort((a, b) => RISK_RANK[b.risk] - RISK_RANK[a.risk] || a.line - b.line),
    [fi.findings]
  );
  return (
    <section
      data-testid="file-card"
      className="bg-white border border-gray-200 rounded-lg overflow-hidden"
    >
      <header className="bg-gray-50 px-4 py-3 border-b border-gray-200 flex items-center justify-between">
        <div className="font-mono text-sm text-gray-900 truncate">{fi.file}</div>
        <div className="flex items-center gap-2 text-xs text-gray-600">
          <span>{fi.language}</span>
          <RiskBadge risk={fi.max_risk} />
        </div>
      </header>
      <ul className="divide-y divide-gray-100">
        {sorted.map((f, i) => (
          <li key={`${f.code}-${f.line}-${i}`} className="px-4 py-3" data-testid="finding-row">
            <div className="flex items-baseline gap-3 flex-wrap">
              <RiskBadge risk={f.risk} />
              <code className="text-xs text-gray-500">{f.code}</code>
              <span className="text-xs text-gray-400">line {f.line}</span>
            </div>
            <div className="text-sm text-gray-900 mt-1">{f.message}</div>
            {f.snippet && (
              <pre className="mt-2 p-2 bg-gray-50 rounded text-xs text-gray-800 overflow-x-auto">
                {f.snippet}
              </pre>
            )}
            <div className="mt-2 text-sm text-gray-700">
              <span className="font-semibold">Suggested:</span> {f.suggestion}
            </div>
            {f.explanation && (
              <div className="mt-2 text-sm text-purple-900 bg-purple-50 border border-purple-100 rounded p-2">
                {f.explanation}
              </div>
            )}
            {(f.before || f.after) && (
              <div className="mt-2 grid md:grid-cols-2 gap-2">
                {f.before && (
                  <div>
                    <div className="text-xs font-semibold text-gray-500 mb-1">Before (Oracle)</div>
                    <pre className="p-2 bg-amber-50 rounded text-xs text-gray-800 overflow-x-auto">
                      {f.before}
                    </pre>
                  </div>
                )}
                {f.after && (
                  <div>
                    <div className="text-xs font-semibold text-gray-500 mb-1">After (PostgreSQL)</div>
                    <pre className="p-2 bg-green-50 rounded text-xs text-gray-800 overflow-x-auto">
                      {f.after}
                    </pre>
                  </div>
                )}
              </div>
            )}
            {f.caveats && f.caveats.length > 0 && (
              <ul className="mt-2 text-xs text-gray-600 list-disc list-inside">
                {f.caveats.map((c, j) => (
                  <li key={j}>{c}</li>
                ))}
              </ul>
            )}
          </li>
        ))}
      </ul>
    </section>
  );
}

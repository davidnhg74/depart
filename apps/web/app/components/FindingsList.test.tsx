import { render, screen, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { describe, it, expect } from 'vitest';

import FindingsList, { AppImpactResponse } from './FindingsList';

function fakeReport(overrides: Partial<AppImpactResponse> = {}): AppImpactResponse {
  return {
    files: [
      {
        file: 'src/Repo.java',
        language: 'java',
        fragments_scanned: 4,
        max_risk: 'critical',
        findings: [
          {
            code: 'APP.SQL.DBLINK', risk: 'critical', message: 'dblink used',
            suggestion: 'use postgres_fdw', file: 'src/Repo.java', line: 42,
            snippet: 'SELECT * FROM t@prod', schema_objects: [],
          },
          {
            code: 'APP.SQL.FN.NVL', risk: 'medium', message: 'NVL used',
            suggestion: 'replace with COALESCE', file: 'src/Repo.java', line: 17,
            snippet: 'NVL(x, y)', schema_objects: [],
          },
        ],
      },
      {
        file: 'src/orders.py',
        language: 'python',
        fragments_scanned: 2,
        max_risk: 'high',
        findings: [
          {
            code: 'APP.SQL.MERGE', risk: 'high', message: 'MERGE used',
            suggestion: 'rewrite as INSERT ON CONFLICT', file: 'src/orders.py',
            line: 8, snippet: 'MERGE INTO t', schema_objects: [],
          },
        ],
      },
    ],
    total_files_scanned: 2,
    total_fragments: 6,
    total_findings: 3,
    findings_by_risk: { critical: 1, high: 1, medium: 1 },
    schema_objects_scanned: 5,
    explained: false,
    ...overrides,
  };
}

describe('FindingsList', () => {
  it('renders header counts', () => {
    render(<FindingsList report={fakeReport()} />);
    // Header reads: "3 findings across 2 files (6 SQL fragments scanned, 5 schema objects)"
    expect(screen.getByText(/findings across/)).toHaveTextContent(
      /3.+findings across.+2.+files.+6 SQL fragments scanned, 5 schema objects/
    );
  });

  it('renders one card per file', () => {
    render(<FindingsList report={fakeReport()} />);
    expect(screen.getAllByTestId('file-card')).toHaveLength(2);
  });

  it('orders findings within a file by risk descending', () => {
    render(<FindingsList report={fakeReport()} />);
    const java = screen.getAllByTestId('file-card')[0];
    const rows = within(java).getAllByTestId('finding-row');
    // CRITICAL (DBLINK) must come before MEDIUM (NVL).
    expect(rows[0]).toHaveTextContent('APP.SQL.DBLINK');
    expect(rows[1]).toHaveTextContent('APP.SQL.FN.NVL');
  });

  it('filters by risk', async () => {
    const user = userEvent.setup();
    render(<FindingsList report={fakeReport()} />);
    await user.click(screen.getByTestId('filter-critical'));
    // Only the Java file (which has the CRITICAL finding) survives.
    const cards = screen.getAllByTestId('file-card');
    expect(cards).toHaveLength(1);
    expect(within(cards[0]).getByText('src/Repo.java')).toBeInTheDocument();
    expect(within(cards[0]).getAllByTestId('finding-row')).toHaveLength(1);
  });

  it('shows AI fields when present', () => {
    const r = fakeReport({
      explained: true,
      explanations_generated: 3,
      explanations_failed: 0,
      files: [{
        file: 'x.java', language: 'java', fragments_scanned: 1, max_risk: 'medium',
        findings: [{
          code: 'APP.SQL.FN.NVL', risk: 'medium', message: 'NVL', suggestion: 'COALESCE',
          file: 'x.java', line: 1, snippet: 'NVL(...)',
          explanation: 'AI says swap NVL for COALESCE.',
          before: 'NVL(x, y)', after: 'COALESCE(x, y)',
          caveats: ['watch nulls', 'and quoted args'],
        }],
      }],
    });
    render(<FindingsList report={r} />);
    expect(screen.getByText(/AI says swap NVL/)).toBeInTheDocument();
    expect(screen.getByText('NVL(x, y)')).toBeInTheDocument();
    expect(screen.getByText('COALESCE(x, y)')).toBeInTheDocument();
    expect(screen.getByText('watch nulls')).toBeInTheDocument();
    expect(screen.getByText(/AI explanations: 3\/3/)).toBeInTheDocument();
  });

  it('renders empty state when filter has no matches', async () => {
    const user = userEvent.setup();
    render(<FindingsList report={fakeReport({ files: [], total_files_scanned: 0,
      total_findings: 0, total_fragments: 0, findings_by_risk: {} })} />);
    expect(screen.queryAllByTestId('file-card')).toHaveLength(0);
    await user.click(screen.getByTestId('filter-critical'));
    expect(screen.getByText(/No findings at this filter/)).toBeInTheDocument();
  });
});

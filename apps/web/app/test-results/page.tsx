'use client';

import { useEffect, useState } from 'react';
import { useSearchParams } from 'next/navigation';
import axios from 'axios';
import Link from 'next/link';
import RiskHeatmap from '../components/RiskHeatmap';

interface MigrationReport {
  migration_id: string;
  total_objects: number;
  converted_count: number;
  tests_generated: number;
  conversion_percentage: number;
  risk_breakdown: Record<string, number>;
  blockers: Array<{ name: string; reason: string }>;
  generated_at: string;
}

export default function TestResultsPage() {
  const searchParams = useSearchParams();
  const migrationId = searchParams.get('migration_id');

  const [report, setReport] = useState<MigrationReport | null>(null);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState('');

  useEffect(() => {
    if (!migrationId) {
      setError('Missing migration_id parameter');
      setIsLoading(false);
      return;
    }

    const fetchReport = async () => {
      try {
        setIsLoading(true);
        const response = await axios.get(
          `${process.env.NEXT_PUBLIC_API_URL}/api/v3/migration/${migrationId}/report`
        );
        setReport(response.data);
        setError('');
      } catch (err) {
        const errorMsg = axios.isAxiosError(err)
          ? err.response?.data?.detail || 'Failed to load report'
          : 'An error occurred';
        setError(errorMsg);
      } finally {
        setIsLoading(false);
      }
    };

    fetchReport();
  }, [migrationId]);

  const handleDownloadPgTAP = () => {
    if (!migrationId) return;

    const pgTAPTemplate = `BEGIN;

-- pgTAP Test Suite for Migration ${migrationId}
-- Generated: ${new Date().toISOString()}

SELECT plan(${report?.tests_generated || 0});

-- Tests would be populated from individual procedure/function conversions

SELECT * FROM finish();
ROLLBACK;
`;

    const element = document.createElement('a');
    element.setAttribute(
      'href',
      'data:text/plain;charset=utf-8,' + encodeURIComponent(pgTAPTemplate)
    );
    element.setAttribute('download', `migration_${migrationId}_tests.sql`);
    element.style.display = 'none';
    document.body.appendChild(element);
    element.click();
    document.body.removeChild(element);
  };

  if (isLoading) {
    return (
      <div className="min-h-screen bg-gradient-to-br from-purple-50 to-blue-50 flex items-center justify-center">
        <div className="text-center">
          <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-purple-600 mx-auto mb-4"></div>
          <p className="text-gray-600">Loading migration report...</p>
        </div>
      </div>
    );
  }

  if (error || !report) {
    return (
      <div className="min-h-screen bg-gradient-to-br from-purple-50 to-blue-50">
        <header className="bg-gradient-to-r from-purple-600 to-blue-600 text-white py-8">
          <div className="container mx-auto px-4">
            <h1 className="text-4xl font-bold mb-2">Migration Test Results</h1>
          </div>
        </header>
        <main className="container mx-auto px-4 py-12">
          <div className="bg-red-50 border border-red-200 rounded-lg p-6 text-center">
            <p className="text-red-800 font-medium">{error || 'Migration report not found'}</p>
            <Link href="/convert" className="text-purple-600 hover:text-purple-700 mt-4 inline-block">
              ← Back to Converter
            </Link>
          </div>
        </main>
      </div>
    );
  }

  const failingTests = report.blockers.length;
  const passingTests = report.tests_generated - failingTests;

  return (
    <div className="min-h-screen bg-gradient-to-br from-purple-50 to-blue-50">
      {/* Header */}
      <header className="bg-gradient-to-r from-purple-600 to-blue-600 text-white py-8">
        <div className="container mx-auto px-4">
          <div className="flex justify-between items-center">
            <div>
              <h1 className="text-4xl font-bold mb-2">Migration Test Results</h1>
              <p className="text-purple-100">
                Conversion Progress: {report.conversion_percentage.toFixed(1)}%
              </p>
            </div>
            <Link
              href="/convert"
              className="px-4 py-2 bg-white text-purple-600 rounded-lg hover:bg-purple-50 font-medium transition"
            >
              ← Back to Converter
            </Link>
          </div>
        </div>
      </header>

      {/* Main Content */}
      <main className="container mx-auto px-4 py-12 space-y-8">
        {/* Summary Cards */}
        <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
          <div className="bg-white rounded-lg shadow p-6">
            <p className="text-gray-600 text-sm font-medium">Total Objects</p>
            <p className="text-4xl font-bold text-gray-900 mt-2">{report.total_objects}</p>
          </div>
          <div className="bg-white rounded-lg shadow p-6">
            <p className="text-gray-600 text-sm font-medium">Converted</p>
            <p className="text-4xl font-bold text-green-600 mt-2">{report.converted_count}</p>
          </div>
          <div className="bg-white rounded-lg shadow p-6">
            <p className="text-gray-600 text-sm font-medium">Tests Generated</p>
            <p className="text-4xl font-bold text-blue-600 mt-2">{report.tests_generated}</p>
          </div>
          <div className="bg-white rounded-lg shadow p-6">
            <p className="text-gray-600 text-sm font-medium">Conversion %</p>
            <p className="text-4xl font-bold text-purple-600 mt-2">
              {report.conversion_percentage.toFixed(0)}%
            </p>
          </div>
        </div>

        {/* Progress Bar */}
        <div className="bg-white rounded-lg shadow p-6">
          <h2 className="text-lg font-bold text-gray-900 mb-4">Progress Overview</h2>
          <div className="space-y-4">
            <div>
              <div className="flex justify-between items-center mb-2">
                <span className="text-sm font-medium text-gray-700">Conversion Progress</span>
                <span className="text-sm font-medium text-gray-900">
                  {report.converted_count} / {report.total_objects}
                </span>
              </div>
              <div className="w-full bg-gray-200 rounded-full h-2">
                <div
                  className="bg-gradient-to-r from-green-500 to-blue-500 h-2 rounded-full transition-all duration-500"
                  style={{ width: `${report.conversion_percentage}%` }}
                ></div>
              </div>
            </div>

            <div>
              <div className="flex justify-between items-center mb-2">
                <span className="text-sm font-medium text-gray-700">Test Results</span>
                <span className="text-sm font-medium text-gray-900">
                  {passingTests} / {report.tests_generated}
                </span>
              </div>
              <div className="w-full bg-gray-200 rounded-full h-2">
                <div
                  className="bg-gradient-to-r from-green-500 to-emerald-500 h-2 rounded-full transition-all duration-500"
                  style={{ width: `${report.tests_generated > 0 ? (passingTests / report.tests_generated) * 100 : 0}%` }}
                ></div>
              </div>
            </div>
          </div>
        </div>

        {/* Risk Heatmap */}
        {(report.risk_breakdown.high > 0 || report.risk_breakdown.medium > 0 || report.risk_breakdown.low > 0) && (
          <div className="bg-white rounded-lg shadow p-6">
            <h2 className="text-lg font-bold text-gray-900 mb-4">Risk Assessment</h2>
            <RiskHeatmap
              items={[
                ...Array(report.risk_breakdown.high).fill({ risk: 'high' as const, name: 'High Risk', construct_type: 'Complex' }),
                ...Array(report.risk_breakdown.medium).fill({ risk: 'medium' as const, name: 'Medium Risk', construct_type: 'Moderate' }),
                ...Array(report.risk_breakdown.low).fill({ risk: 'low' as const, name: 'Low Risk', construct_type: 'Simple' }),
              ]}
            />
          </div>
        )}

        {/* Blockers */}
        {report.blockers.length > 0 && (
          <div className="bg-white rounded-lg shadow p-6">
            <h2 className="text-lg font-bold text-gray-900 mb-4">Issues & Blockers ({report.blockers.length})</h2>
            <div className="space-y-3">
              {report.blockers.map((blocker, idx) => (
                <div key={idx} className="border border-red-200 rounded-lg p-4 bg-red-50">
                  <p className="font-medium text-red-900">{blocker.name}</p>
                  <p className="text-sm text-red-700 mt-1">{blocker.reason}</p>
                </div>
              ))}
            </div>
          </div>
        )}

        {/* Test Summary Table */}
        <div className="bg-white rounded-lg shadow p-6">
          <div className="flex justify-between items-center mb-4">
            <h2 className="text-lg font-bold text-gray-900">Test Summary</h2>
            <button
              onClick={handleDownloadPgTAP}
              className="px-4 py-2 bg-green-600 text-white rounded-lg hover:bg-green-700 transition font-medium"
            >
              Download pgTAP SQL
            </button>
          </div>

          <div className="space-y-2 text-sm text-gray-700">
            <p>
              <span className="font-medium">Passing Tests:</span>{' '}
              <span className="text-green-600 font-bold">{passingTests}</span>
            </p>
            <p>
              <span className="font-medium">Failing Tests:</span>{' '}
              <span className="text-red-600 font-bold">{failingTests}</span>
            </p>
            <p>
              <span className="font-medium">Total Tests:</span>{' '}
              <span className="text-blue-600 font-bold">{report.tests_generated}</span>
            </p>
            <p>
              <span className="font-medium">Generated:</span> {new Date(report.generated_at).toLocaleString()}
            </p>
          </div>
        </div>
      </main>
    </div>
  );
}

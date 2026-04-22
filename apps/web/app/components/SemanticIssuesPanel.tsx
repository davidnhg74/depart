'use client';

import { useState, useEffect } from 'react';

interface SemanticIssue {
  severity: 'CRITICAL' | 'ERROR' | 'WARNING' | 'INFO';
  issue_type: string;
  affected_object: string;
  oracle_type: string;
  pg_type: string;
  description: string;
  recommendation: string;
}

interface SemanticAnalysisResult {
  mode: string;
  analyzed_objects: number;
  issues: SemanticIssue[];
  summary: {
    critical: number;
    error: number;
    warning: number;
    info: number;
    total: number;
  };
}

interface SemanticIssuesPanelProps {
  oracleDdl: string;
  pgDdl: string;
  autoAnalyze?: boolean;
}

const severityColors = {
  CRITICAL: {
    container: 'bg-red-50 border-red-200',
    badge: 'bg-red-100 text-red-800',
    border: 'border-l-4 border-l-red-500',
  },
  ERROR: {
    container: 'bg-orange-50 border-orange-200',
    badge: 'bg-orange-100 text-orange-800',
    border: 'border-l-4 border-l-orange-500',
  },
  WARNING: {
    container: 'bg-yellow-50 border-yellow-200',
    badge: 'bg-yellow-100 text-yellow-800',
    border: 'border-l-4 border-l-yellow-500',
  },
  INFO: {
    container: 'bg-blue-50 border-blue-200',
    badge: 'bg-blue-100 text-blue-800',
    border: 'border-l-4 border-l-blue-500',
  },
};

export default function SemanticIssuesPanel({
  oracleDdl,
  pgDdl,
  autoAnalyze = true,
}: SemanticIssuesPanelProps) {
  const [loading, setLoading] = useState(autoAnalyze);
  const [result, setResult] = useState<SemanticAnalysisResult | null>(null);
  const [error, setError] = useState('');

  useEffect(() => {
    if (autoAnalyze) {
      analyzeIssues();
    }
  }, [autoAnalyze, oracleDdl, pgDdl]);

  const analyzeIssues = async () => {
    if (!oracleDdl.trim() || !pgDdl.trim()) {
      setError('Both Oracle and PostgreSQL DDL are required');
      return;
    }

    setLoading(true);
    setError('');
    setResult(null);

    try {
      const apiUrl = process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8000';
      const response = await fetch(`${apiUrl}/api/v3/analyze/semantic`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          oracle_ddl: oracleDdl,
          pg_ddl: pgDdl,
        }),
      });

      if (!response.ok) {
        throw new Error('Failed to analyze semantic issues');
      }

      const data = await response.json();
      setResult(data);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'An error occurred');
    } finally {
      setLoading(false);
    }
  };

  if (!result && !loading && !error) {
    return null;
  }

  return (
    <div className="mt-8 space-y-4">
      {/* Header */}
      <div>
        <h2 className="text-2xl font-bold text-gray-900 mb-2">Semantic Analysis</h2>
        <p className="text-gray-600">
          AI-powered detection of logical errors: precision loss, date behavior, NULL semantics,
          type coercion, and encoding mismatches.
        </p>
      </div>

      {/* Loading */}
      {loading && (
        <div className="flex items-center justify-center p-8 bg-blue-50 border border-blue-200 rounded-lg">
          <div className="animate-spin h-5 w-5 text-blue-600 mr-3"></div>
          <p className="text-blue-700">Analyzing for semantic issues...</p>
        </div>
      )}

      {/* Error */}
      {error && (
        <div className="p-4 bg-red-50 border border-red-200 rounded-lg text-red-700">
          {error}
        </div>
      )}

      {/* Results */}
      {result && !loading && (
        <div className="space-y-4">
          {/* Summary Cards */}
          <div className="grid grid-cols-2 md:grid-cols-5 gap-3">
            <div className="bg-white border border-gray-200 rounded-lg p-3">
              <p className="text-xs text-gray-600 font-medium">Analyzed Objects</p>
              <p className="text-2xl font-bold text-gray-900">{result.analyzed_objects}</p>
            </div>

            <div className="bg-red-50 border border-red-200 rounded-lg p-3">
              <p className="text-xs text-red-600 font-medium">Critical</p>
              <p className="text-2xl font-bold text-red-700">{result.summary.critical}</p>
            </div>

            <div className="bg-orange-50 border border-orange-200 rounded-lg p-3">
              <p className="text-xs text-orange-600 font-medium">Errors</p>
              <p className="text-2xl font-bold text-orange-700">{result.summary.error}</p>
            </div>

            <div className="bg-yellow-50 border border-yellow-200 rounded-lg p-3">
              <p className="text-xs text-yellow-600 font-medium">Warnings</p>
              <p className="text-2xl font-bold text-yellow-700">{result.summary.warning}</p>
            </div>

            <div className="bg-blue-50 border border-blue-200 rounded-lg p-3">
              <p className="text-xs text-blue-600 font-medium">Info</p>
              <p className="text-2xl font-bold text-blue-700">{result.summary.info}</p>
            </div>
          </div>

          {/* Issues List */}
          {result.issues.length > 0 ? (
            <div className="space-y-3">
              <p className="text-sm font-medium text-gray-700">
                {result.issues.length} issue{result.issues.length !== 1 ? 's' : ''} found
              </p>
              {result.issues.map((issue, idx) => {
                const colors = severityColors[issue.severity];
                return (
                  <div
                    key={idx}
                    className={`${colors.border} border rounded-lg p-4 ${colors.container}`}
                  >
                    <div className="flex items-start justify-between mb-2">
                      <div className="flex gap-2 items-start flex-1">
                        <span className={`${colors.badge} px-2 py-1 rounded text-xs font-semibold whitespace-nowrap`}>
                          {issue.severity}
                        </span>
                        <span className="text-xs font-medium text-gray-700 bg-white px-2 py-1 rounded whitespace-nowrap">
                          {issue.issue_type}
                        </span>
                      </div>
                    </div>

                    <p className="font-mono text-sm font-bold text-gray-900 mb-3">
                      {issue.affected_object}
                    </p>

                    <div className="grid grid-cols-2 gap-3 mb-3">
                      <div>
                        <p className="text-xs text-gray-600 font-medium mb-1">Oracle Type</p>
                        <p className="font-mono text-sm bg-white px-2 py-1 rounded border border-gray-300">
                          {issue.oracle_type}
                        </p>
                      </div>
                      <div>
                        <p className="text-xs text-gray-600 font-medium mb-1">PostgreSQL Type</p>
                        <p className="font-mono text-sm bg-white px-2 py-1 rounded border border-gray-300">
                          {issue.pg_type}
                        </p>
                      </div>
                    </div>

                    <div className="mb-3">
                      <p className="text-xs text-gray-600 font-medium mb-1">Issue Description</p>
                      <p className="text-sm text-gray-800">{issue.description}</p>
                    </div>

                    <div>
                      <p className="text-xs text-gray-600 font-medium mb-1">Recommendation</p>
                      <p className="text-sm text-gray-800 bg-white bg-opacity-50 px-2 py-1 rounded">
                        {issue.recommendation}
                      </p>
                    </div>
                  </div>
                );
              })}
            </div>
          ) : (
            <div className="p-6 bg-green-50 border border-green-200 rounded-lg text-center">
              <p className="text-green-700 font-medium">✓ No semantic issues detected</p>
              <p className="text-green-600 text-sm mt-1">
                Your type mappings look safe for migration.
              </p>
            </div>
          )}
        </div>
      )}

      {/* Manual Analyze Button */}
      {!autoAnalyze && (
        <button
          onClick={analyzeIssues}
          disabled={loading}
          className="w-full bg-blue-600 hover:bg-blue-700 disabled:opacity-50 text-white py-2 px-4 rounded-lg font-medium transition"
        >
          {loading ? 'Analyzing...' : 'Analyze for Semantic Issues'}
        </button>
      )}
    </div>
  );
}

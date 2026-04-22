/**
 * Process-lifetime AI token spend, broken down by feature and model.
 *
 * Polls /api/v3/usage/summary on mount + every 30s. Numbers reset when
 * the API process restarts — that's a known limitation pending the
 * per-project persistence pass.
 */
'use client';

import React, { useCallback, useEffect, useState } from 'react';
import axios from 'axios';

import { apiBaseUrl } from '@/app/lib/api';

interface FeatureUsage {
  feature: string;
  calls: number;
  input_tokens: number;
  output_tokens: number;
  cache_read_input_tokens: number;
  cache_creation_input_tokens: number;
  avg_latency_ms: number;
  estimated_cost_usd: number;
}

interface ModelUsage {
  model: string;
  calls: number;
  input_tokens: number;
  output_tokens: number;
  estimated_cost_usd: number;
}

export interface UsageSummary {
  total_calls: number;
  total_input_tokens: number;
  total_output_tokens: number;
  total_cache_read_tokens: number;
  total_cache_creation_tokens: number;
  total_estimated_cost_usd: number;
  by_feature: FeatureUsage[];
  by_model: ModelUsage[];
}

const POLL_MS = 30_000;

export default function TokenUsageCard() {
  const [summary, setSummary] = useState<UsageSummary | null>(null);
  const [error, setError] = useState<string>('');

  const refresh = useCallback(async () => {
    try {
      const resp = await axios.get<UsageSummary>(`${apiBaseUrl()}/api/v3/usage/summary`);
      setSummary(resp.data);
      setError('');
    } catch (e) {
      setError('Could not load token usage.');
    }
  }, []);

  useEffect(() => {
    refresh();
    const id = window.setInterval(refresh, POLL_MS);
    return () => window.clearInterval(id);
  }, [refresh]);

  return (
    <div data-testid="token-usage-card" className="bg-white rounded-lg shadow p-6">
      <div className="flex items-center justify-between mb-4">
        <h2 className="text-lg font-semibold text-gray-900">AI Token Spend</h2>
        <button
          onClick={refresh}
          className="text-xs text-purple-600 hover:text-purple-800"
          data-testid="usage-refresh"
        >
          Refresh
        </button>
      </div>

      {error && (
        <p data-testid="usage-error" className="text-sm text-red-700">{error}</p>
      )}

      {!summary ? (
        <p className="text-sm text-gray-500">Loading…</p>
      ) : (
        <>
          <div className="mb-6">
            <div className="text-3xl font-bold text-purple-700" data-testid="total-cost">
              ${summary.total_estimated_cost_usd.toFixed(4)}
            </div>
            <div className="text-sm text-gray-600 mt-1">
              {summary.total_calls.toLocaleString()} calls,{' '}
              {(summary.total_input_tokens + summary.total_output_tokens).toLocaleString()} tokens
            </div>
            <div className="text-xs text-gray-500 mt-1">
              Process-lifetime totals; resets on API restart.
            </div>
          </div>

          {summary.by_feature.length > 0 && (
            <section className="mb-4">
              <h3 className="text-xs font-semibold text-gray-500 uppercase tracking-wide mb-2">
                By feature
              </h3>
              <table className="w-full text-sm" data-testid="by-feature-table">
                <thead className="text-gray-500 text-left">
                  <tr>
                    <th className="font-medium pb-1">Feature</th>
                    <th className="font-medium pb-1 text-right">Calls</th>
                    <th className="font-medium pb-1 text-right">Tokens</th>
                    <th className="font-medium pb-1 text-right">Cost</th>
                  </tr>
                </thead>
                <tbody className="text-gray-800">
                  {summary.by_feature.map((f) => (
                    <tr key={f.feature} className="border-t border-gray-100">
                      <td className="py-1 font-mono text-xs">{f.feature}</td>
                      <td className="py-1 text-right">{f.calls.toLocaleString()}</td>
                      <td className="py-1 text-right">
                        {(f.input_tokens + f.output_tokens).toLocaleString()}
                      </td>
                      <td className="py-1 text-right">${f.estimated_cost_usd.toFixed(4)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </section>
          )}

          {summary.by_model.length > 0 && (
            <section>
              <h3 className="text-xs font-semibold text-gray-500 uppercase tracking-wide mb-2">
                By model
              </h3>
              <ul className="text-sm text-gray-700 space-y-1" data-testid="by-model-list">
                {summary.by_model.map((m) => (
                  <li key={m.model} className="flex justify-between">
                    <span className="font-mono text-xs">{m.model}</span>
                    <span>${m.estimated_cost_usd.toFixed(4)}</span>
                  </li>
                ))}
              </ul>
            </section>
          )}
        </>
      )}
    </div>
  );
}

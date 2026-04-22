import { render, screen, waitFor, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import axios from 'axios';
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';

import TokenUsageCard, { UsageSummary } from './TokenUsageCard';

vi.mock('axios');
const mockedAxios = vi.mocked(axios, true);


function fakeSummary(overrides: Partial<UsageSummary> = {}): UsageSummary {
  return {
    total_calls: 12,
    total_input_tokens: 50000,
    total_output_tokens: 8000,
    total_cache_read_tokens: 0,
    total_cache_creation_tokens: 0,
    total_estimated_cost_usd: 0.4321,
    by_feature: [
      { feature: 'app_impact', calls: 8, input_tokens: 30000, output_tokens: 5000,
        cache_read_input_tokens: 0, cache_creation_input_tokens: 0,
        avg_latency_ms: 320, estimated_cost_usd: 0.2810 },
      { feature: 'runbook', calls: 4, input_tokens: 20000, output_tokens: 3000,
        cache_read_input_tokens: 0, cache_creation_input_tokens: 0,
        avg_latency_ms: 1500, estimated_cost_usd: 0.1511 },
    ],
    by_model: [
      { model: 'claude-haiku-4-5', calls: 8, input_tokens: 30000, output_tokens: 5000,
        estimated_cost_usd: 0.0700 },
      { model: 'claude-opus-4-7', calls: 4, input_tokens: 20000, output_tokens: 3000,
        estimated_cost_usd: 0.3621 },
    ],
    ...overrides,
  };
}


describe('TokenUsageCard', () => {
  beforeEach(() => {
    vi.useFakeTimers({ shouldAdvanceTime: true });
  });
  afterEach(() => {
    vi.useRealTimers();
    vi.clearAllMocks();
  });

  it('shows loading then renders the summary on mount', async () => {
    mockedAxios.get.mockResolvedValueOnce({ data: fakeSummary() });
    render(<TokenUsageCard />);

    expect(screen.getByText(/Loading/)).toBeInTheDocument();

    await waitFor(() => expect(screen.getByTestId('total-cost')).toBeInTheDocument());
    expect(screen.getByTestId('total-cost')).toHaveTextContent('$0.4321');
    expect(screen.getByText(/12 calls/)).toBeInTheDocument();
  });

  it('renders the by-feature table', async () => {
    mockedAxios.get.mockResolvedValueOnce({ data: fakeSummary() });
    render(<TokenUsageCard />);
    await waitFor(() => screen.getByTestId('by-feature-table'));

    const table = screen.getByTestId('by-feature-table');
    expect(within(table).getByText('app_impact')).toBeInTheDocument();
    expect(within(table).getByText('runbook')).toBeInTheDocument();
    expect(within(table).getByText('$0.2810')).toBeInTheDocument();
    expect(within(table).getByText('$0.1511')).toBeInTheDocument();
  });

  it('renders the by-model list', async () => {
    mockedAxios.get.mockResolvedValueOnce({ data: fakeSummary() });
    render(<TokenUsageCard />);
    await waitFor(() => screen.getByTestId('by-model-list'));

    const list = screen.getByTestId('by-model-list');
    expect(within(list).getByText('claude-haiku-4-5')).toBeInTheDocument();
    expect(within(list).getByText('claude-opus-4-7')).toBeInTheDocument();
  });

  it('refreshes on button click', async () => {
    mockedAxios.get
      .mockResolvedValueOnce({ data: fakeSummary({ total_estimated_cost_usd: 0.10 }) })
      .mockResolvedValueOnce({ data: fakeSummary({ total_estimated_cost_usd: 0.50 }) });
    render(<TokenUsageCard />);
    await waitFor(() => expect(screen.getByTestId('total-cost')).toHaveTextContent('$0.1000'));

    await userEvent.click(screen.getByTestId('usage-refresh'));
    await waitFor(() => expect(screen.getByTestId('total-cost')).toHaveTextContent('$0.5000'));
    expect(mockedAxios.get).toHaveBeenCalledTimes(2);
  });

  it('shows an error when the request fails', async () => {
    mockedAxios.get.mockRejectedValueOnce(new Error('boom'));
    render(<TokenUsageCard />);
    await waitFor(() => expect(screen.getByTestId('usage-error')).toBeInTheDocument());
    expect(screen.getByTestId('usage-error')).toHaveTextContent(/Could not load/);
  });

  it('clears error after a successful refresh', async () => {
    mockedAxios.get
      .mockRejectedValueOnce(new Error('boom'))
      .mockResolvedValueOnce({ data: fakeSummary() });
    render(<TokenUsageCard />);
    await waitFor(() => expect(screen.getByTestId('usage-error')).toBeInTheDocument());

    await userEvent.click(screen.getByTestId('usage-refresh'));
    await waitFor(() => expect(screen.queryByTestId('usage-error')).not.toBeInTheDocument());
  });

  it('hides feature/model sections when empty', async () => {
    const empty = fakeSummary({
      total_calls: 0, total_input_tokens: 0, total_output_tokens: 0,
      total_estimated_cost_usd: 0, by_feature: [], by_model: [],
    });
    mockedAxios.get.mockResolvedValueOnce({ data: empty });
    render(<TokenUsageCard />);
    await waitFor(() => expect(screen.getByTestId('total-cost')).toBeInTheDocument());

    expect(screen.queryByTestId('by-feature-table')).not.toBeInTheDocument();
    expect(screen.queryByTestId('by-model-list')).not.toBeInTheDocument();
  });
});

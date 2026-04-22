import { render, screen } from '@testing-library/react';
import { describe, it, expect } from 'vitest';

import RiskBadge, { Risk } from './RiskBadge';

describe('RiskBadge', () => {
  const cases: Array<[Risk, string]> = [
    ['low', 'Low'], ['medium', 'Medium'], ['high', 'High'], ['critical', 'Critical'],
  ];

  it.each(cases)('renders %s as "%s"', (risk, label) => {
    render(<RiskBadge risk={risk} />);
    const badge = screen.getByTestId(`risk-${risk}`);
    expect(badge).toHaveTextContent(label);
  });

  it('applies caller className alongside the palette', () => {
    render(<RiskBadge risk="critical" className="extra-class" />);
    const badge = screen.getByTestId('risk-critical');
    expect(badge.className).toContain('extra-class');
    expect(badge.className).toContain('bg-red-100');
  });

  it('falls back gracefully on unknown risk', () => {
    // @ts-expect-error -- forcing the runtime path even though TS would catch it.
    render(<RiskBadge risk="other" />);
    expect(screen.getByText('other')).toBeInTheDocument();
  });
});

/**
 * Color-coded badge for a finding's risk level.
 * Critical=red, High=amber, Medium=yellow-green, Low=green. Same palette
 * as the runbook PDF so customers see the same colors in both deliverables.
 */
import React from 'react';

export type Risk = 'low' | 'medium' | 'high' | 'critical';

const STYLES: Record<Risk, string> = {
  low:      'bg-green-100 text-green-800 border-green-200',
  medium:   'bg-yellow-100 text-yellow-800 border-yellow-300',
  high:     'bg-orange-100 text-orange-800 border-orange-300',
  critical: 'bg-red-100 text-red-800 border-red-300',
};

const LABEL: Record<Risk, string> = {
  low: 'Low', medium: 'Medium', high: 'High', critical: 'Critical',
};

interface Props {
  risk: Risk;
  className?: string;
}

export default function RiskBadge({ risk, className = '' }: Props) {
  const style = STYLES[risk] || STYLES.medium;
  const label = LABEL[risk] || risk;
  return (
    <span
      data-testid={`risk-${risk}`}
      className={`inline-flex items-center px-2 py-0.5 rounded-md text-xs font-semibold border ${style} ${className}`}
    >
      {label}
    </span>
  );
}

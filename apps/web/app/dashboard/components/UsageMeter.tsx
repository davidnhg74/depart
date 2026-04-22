'use client';

import { User } from '@/app/store/authStore';
import { PLAN_LIMITS } from '@/app/lib/planLimits';

interface UsageMeterProps {
  user: User;
}

export default function UsageMeter({ user }: UsageMeterProps) {
  const limits = PLAN_LIMITS[user.plan ?? 'trial'];
  const databasesPercent = limits.databases ? ((user.databases_used ?? 0) / limits.databases) * 100 : 0;
  const migrationsPercent = limits.migrations_per_month ? ((user.migrations_used_this_month ?? 0) / limits.migrations_per_month) * 100 : 0;
  const llmPercent = limits.llm_per_month ? ((user.llm_conversions_this_month ?? 0) / limits.llm_per_month) * 100 : 0;

  const getColor = (percent: number) => {
    if (percent > 90) return 'bg-red-500';
    if (percent > 75) return 'bg-yellow-500';
    return 'bg-green-500';
  };

  return (
    <div className="space-y-6">
      {/* Databases */}
      <div>
        <div className="flex justify-between mb-2">
          <span className="text-sm font-medium text-gray-700">Databases</span>
          <span className="text-sm text-gray-600">
            {user.databases_used} / {limits.databases || '∞'}
          </span>
        </div>
        <div className="w-full bg-gray-200 rounded-full h-2">
          <div
            className={`h-2 rounded-full transition-all ${getColor(databasesPercent)}`}
            style={{ width: `${Math.min(databasesPercent, 100)}%` }}
          />
        </div>
      </div>

      {/* Migrations/Month */}
      <div>
        <div className="flex justify-between mb-2">
          <span className="text-sm font-medium text-gray-700">Migrations (this month)</span>
          <span className="text-sm text-gray-600">
            {user.migrations_used_this_month} / {limits.migrations_per_month || '∞'}
          </span>
        </div>
        <div className="w-full bg-gray-200 rounded-full h-2">
          <div
            className={`h-2 rounded-full transition-all ${getColor(migrationsPercent)}`}
            style={{ width: `${Math.min(migrationsPercent, 100)}%` }}
          />
        </div>
      </div>

      {/* LLM Conversions/Month */}
      <div>
        <div className="flex justify-between mb-2">
          <span className="text-sm font-medium text-gray-700">AI Conversions (this month)</span>
          <span className="text-sm text-gray-600">
            {user.llm_conversions_this_month} / {limits.llm_per_month || '∞'}
          </span>
        </div>
        <div className="w-full bg-gray-200 rounded-full h-2">
          <div
            className={`h-2 rounded-full transition-all ${getColor(llmPercent)}`}
            style={{ width: `${Math.min(llmPercent, 100)}%` }}
          />
        </div>
      </div>
    </div>
  );
}

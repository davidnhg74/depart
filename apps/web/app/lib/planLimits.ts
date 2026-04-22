/**
 * Plan-tier usage limits, mirrored from the API's billing service so the
 * UI can render usage meters without an extra round trip. Keep in sync
 * with src/services/billing.py (PLAN_LIMITS).
 *
 * `null` (or absent) on a limit means "unlimited" for that tier.
 */
import type { User } from '@/app/store/authStore';

export interface Limits {
  databases: number | null;
  migrations_per_month: number | null;
  llm_per_month: number | null;
}

export const PLAN_LIMITS: Record<NonNullable<User['plan']>, Limits> = {
  trial: {
    databases: 1,
    migrations_per_month: 3,
    llm_per_month: 50,
  },
  starter: {
    databases: 3,
    migrations_per_month: 20,
    llm_per_month: 500,
  },
  professional: {
    databases: 10,
    migrations_per_month: 100,
    llm_per_month: 5000,
  },
  enterprise: {
    databases: null,
    migrations_per_month: null,
    llm_per_month: null,
  },
};

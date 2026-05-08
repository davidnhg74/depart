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
    llm_per_month: 10,
  },
  starter: {
    databases: 5,
    migrations_per_month: 25,
    llm_per_month: 100,
  },
  professional: {
    databases: 20,
    migrations_per_month: 100,
    llm_per_month: 500,
  },
  enterprise: {
    databases: null,
    migrations_per_month: null,
    llm_per_month: null,
  },
};

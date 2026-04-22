/**
 * Single source of truth for whether this web build is the cloud variant
 * (signup/login/billing/support enabled) or the self-hosted product build.
 *
 * Mirrors the backend `settings.enable_cloud_routes` flag. Set via the
 * NEXT_PUBLIC_ENABLE_CLOUD_ROUTES env var at build-time OR runtime —
 * Next inlines NEXT_PUBLIC_ vars at build time for client components
 * but still reads them at request time for server ones.
 *
 * Default is **false** (self-hosted): the same default as the backend.
 * depart.cloud sets NEXT_PUBLIC_ENABLE_CLOUD_ROUTES=true at deploy time.
 */

export function cloudRoutesEnabled(): boolean {
  const raw = process.env.NEXT_PUBLIC_ENABLE_CLOUD_ROUTES;
  if (!raw) return false;
  return ['1', 'true', 'yes', 'on'].includes(raw.toLowerCase());
}

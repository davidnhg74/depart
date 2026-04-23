'use client';

import { useEffect } from 'react';

import { fetchBranding } from '@/app/lib/api';
import { useBrandingStore } from '@/app/store/brandingStore';


/**
 * Bootstraps the branding store on layout mount.
 *
 * The store seeds itself with the Hafen defaults so the first paint
 * is correct for the common case. If the operator's install has a
 * white-label config, the second paint swaps in their values; the
 * fetch is unauthenticated, so this works on the sign-in page too.
 *
 * Failures are intentionally silent — if the API is unreachable, the
 * UI keeps the defaults rather than blocking on a network error or
 * surfacing a toast. The branding endpoint is non-essential.
 */
export default function BrandingInitializer() {
  const setBranding = useBrandingStore((s) => s.setBranding);

  useEffect(() => {
    fetchBranding()
      .then(setBranding)
      .catch(() => {
        // Keep defaults — no need to clobber the UI for a branding
        // fetch failure.
      });
  }, [setBranding]);

  return null;
}

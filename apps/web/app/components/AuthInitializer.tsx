'use client';

import { useEffect } from 'react';
import Cookies from 'js-cookie';

import { fetchCurrentUser, fetchCurrentUserLocal } from '@/app/lib/api';
import { cloudRoutesEnabled } from '@/app/lib/cloudRoutes';
import { useAuthStore } from '@/app/store/authStore';


/**
 * Runs once on layout mount. If we have a stored access token in the
 * cookie jar, ask the backend who it belongs to and hydrate the auth
 * store. Silently drops the token on 401 / unknown user.
 *
 * Uses the cloud or self-hosted /me endpoint based on the build flag —
 * the two responses share a common subset that the auth store cares
 * about (id, email, full_name, role).
 */
export default function AuthInitializer() {
  useEffect(() => {
    // Tokens are stored as cookies by `persistTokens` in lib/api.ts,
    // not localStorage. The previous implementation looked at the
    // wrong place and quietly did nothing.
    const token = Cookies.get('access_token');
    if (!token) return;

    const fetcher = cloudRoutesEnabled() ? fetchCurrentUser : fetchCurrentUserLocal;
    fetcher().catch(() => {
      // 401 / unknown — clear the stale token so /login's redirect
      // dance doesn't loop on the next page load.
      Cookies.remove('access_token');
      Cookies.remove('refresh_token');
      useAuthStore.getState().logout();
    });
  }, []);

  return null;
}

'use client';

/**
 * Wraps app pages (assess, settings/instance, migrations, …) so that
 * in a self-hosted build with auth enforced, unauthenticated users
 * get bounced to /login, and fresh installs (no admin yet) get
 * bounced to /setup.
 *
 * This is intentionally distinct from `AuthGuard` — AuthGuard was
 * written for the cloud flow where every page requires a signed-in
 * user regardless of build. SelfHostedGuard is cheap (just delegates
 * to AuthGuard in cloud mode) but adds the /setup bootstrap bounce
 * that self-hosted needs and cloud doesn't.
 *
 * When cloud routes are on, this is a no-op (render children); the
 * cloud build handles auth via its own AuthGuard chain on each page.
 */

import { useEffect, useState } from 'react';
import { useRouter, usePathname } from 'next/navigation';
import Cookies from 'js-cookie';

import { cloudRoutesEnabled } from '@/app/lib/cloudRoutes';
import { getSetupStatus } from '@/app/lib/api';
import { useAuthStore } from '@/app/store/authStore';


export default function SelfHostedGuard({
  children,
}: {
  children: React.ReactNode;
}) {
  const router = useRouter();
  const pathname = usePathname();
  const { user, isAuthenticated } = useAuthStore();
  const [phase, setPhase] = useState<'checking' | 'ok' | 'blocked'>('checking');

  useEffect(() => {
    // In cloud mode this guard is a pass-through — cloud pages still
    // rely on AuthGuard as before.
    if (cloudRoutesEnabled()) {
      setPhase('ok');
      return;
    }

    (async () => {
      // Is this a fresh install? Bounce to /setup if so.
      try {
        const s = await getSetupStatus();
        if (s.needs_bootstrap) {
          router.replace('/setup');
          setPhase('blocked');
          return;
        }
      } catch {
        // If setup status is unreachable, fall through — the user
        // will see a helpful error on the page itself.
      }

      // Admin exists but we're not signed in → /login with a `next`
      // query so login bounces back here.
      const token = Cookies.get('access_token');
      if (!token || !isAuthenticated) {
        const next = encodeURIComponent(pathname || '/assess');
        router.replace(`/login?next=${next}`);
        setPhase('blocked');
        return;
      }

      setPhase('ok');
    })();
  }, [router, pathname, isAuthenticated]);

  if (phase === 'checking') {
    return (
      <div className="flex min-h-screen items-center justify-center bg-gray-50">
        <p className="text-sm text-gray-500">Checking access…</p>
      </div>
    );
  }
  if (phase === 'blocked') return null;

  return <>{children}</>;
}

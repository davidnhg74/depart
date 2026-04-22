'use client';

import { useEffect, ReactNode } from 'react';
import { useRouter, usePathname } from 'next/navigation';
import { useAuthStore } from '@/app/store/authStore';

interface AuthGuardProps {
  children: ReactNode;
}

export default function AuthGuard({ children }: AuthGuardProps) {
  const router = useRouter();
  const pathname = usePathname();
  const { user, isAuthenticated } = useAuthStore();

  useEffect(() => {
    // Wait for store to hydrate from localStorage
    const unsubscribe = useAuthStore.subscribe(
      (state) => state.user,
      () => {
        // Check auth after state is available
        if (!isAuthenticated) {
          router.push(`/login?next=${encodeURIComponent(pathname)}`);
        }
      }
    );

    // Initial check
    if (!isAuthenticated) {
      router.push(`/login?next=${encodeURIComponent(pathname)}`);
    }

    return () => unsubscribe();
  }, [isAuthenticated, pathname, router]);

  if (!isAuthenticated) {
    return (
      <div className="min-h-screen flex items-center justify-center">
        <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-purple-600"></div>
      </div>
    );
  }

  return <>{children}</>;
}

'use client';

import { useEffect, useState } from 'react';
import { useRouter, useSearchParams } from 'next/navigation';
import Link from 'next/link';
import { apiBaseUrl, getSsoPublicStatus, login, loginLocal } from '@/app/lib/api';
import { cloudRoutesEnabled } from '@/app/lib/cloudRoutes';

export default function LoginPage() {
  // Login works in both builds:
  //   * Cloud → hits /api/v4/auth/login (has user-in-response)
  //   * Self-hosted → hits /api/v1/auth/login (token-only; /me populates store)
  return <LoginContent />;
}

function LoginContent() {
  const isCloud = cloudRoutesEnabled();
  const [email, setEmail] = useState('');
  const [password, setPassword] = useState('');
  const [error, setError] = useState('');
  const [loading, setLoading] = useState(false);
  const [ssoEnabled, setSsoEnabled] = useState(false);
  const [ssoProtocol, setSsoProtocol] = useState<'oidc' | 'saml' | null>(null);
  const router = useRouter();
  const searchParams = useSearchParams();

  // SSO availability + any ?error= payload the /callback bounces back.
  useEffect(() => {
    if (isCloud) return;
    const ssoError = searchParams.get('error');
    if (ssoError) {
      setError(`SSO: ${ssoError.replace(/^sso_/, '').replace(/_/g, ' ')}`);
    }
    (async () => {
      try {
        const s = await getSsoPublicStatus();
        setSsoEnabled(s.enabled);
        setSsoProtocol(s.protocol ?? null);
      } catch {
        setSsoEnabled(false);
      }
    })();
  }, [isCloud, searchParams]);

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    setError('');
    setLoading(true);

    try {
      if (isCloud) {
        await login(email, password);
      } else {
        await loginLocal(email, password);
      }
      // Cloud default lands in /dashboard; self-hosted default lands in
      // /assess because there's no user-facing dashboard concept.
      const fallback = isCloud ? '/dashboard' : '/assess';
      router.push(searchParams.get('next') || fallback);
    } catch (err: any) {
      setError(err.response?.data?.detail || err.response?.data?.error || 'Login failed');
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="min-h-screen flex items-center justify-center bg-gray-50 py-12 px-4">
      <div className="max-w-md w-full bg-white rounded-lg shadow p-8">
        <h1 className="text-2xl font-bold text-gray-900 mb-6">Log in</h1>

        {error && (
          <div className="mb-4 p-3 bg-red-50 border border-red-200 rounded text-red-700 text-sm">
            {error}
          </div>
        )}

        <form onSubmit={handleSubmit} className="space-y-4">
          <div>
            <label htmlFor="email" className="block text-sm font-medium text-gray-700">
              Email
            </label>
            <input
              id="email"
              type="email"
              required
              value={email}
              onChange={(e) => setEmail(e.target.value)}
              className="mt-1 block w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm focus:outline-none focus:ring-purple-500 focus:border-purple-500"
              placeholder="you@example.com"
            />
          </div>

          <div>
            <label htmlFor="password" className="block text-sm font-medium text-gray-700">
              Password
            </label>
            <input
              id="password"
              type="password"
              required
              value={password}
              onChange={(e) => setPassword(e.target.value)}
              className="mt-1 block w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm focus:outline-none focus:ring-purple-500 focus:border-purple-500"
              placeholder="••••••••"
            />
          </div>

          <button
            type="submit"
            disabled={loading}
            className="w-full py-2 px-4 bg-purple-600 text-white font-medium rounded-md hover:bg-purple-700 disabled:bg-gray-400"
          >
            {loading ? 'Logging in...' : 'Log in'}
          </button>
        </form>

        {!isCloud && ssoEnabled && (
          <>
            <div className="my-6 flex items-center gap-3 text-xs uppercase tracking-wide text-gray-400">
              <span className="h-px flex-1 bg-gray-200" />
              or
              <span className="h-px flex-1 bg-gray-200" />
            </div>
            <a
              href={
                ssoProtocol === 'saml'
                  ? `${apiBaseUrl()}/api/v1/auth/sso/saml/login`
                  : `${apiBaseUrl()}/api/v1/auth/sso/start?next=${encodeURIComponent(
                      searchParams.get('next') || '/assess',
                    )}`
              }
              className="flex w-full items-center justify-center rounded-md border border-gray-300 bg-white px-4 py-2 font-medium text-gray-700 hover:bg-gray-50"
            >
              Log in with {ssoProtocol === 'saml' ? 'SAML SSO' : 'SSO'}
            </a>
          </>
        )}

        {/* Only show forgot-password + signup links in the cloud build.
            Self-hosted admins manage users directly from /settings/users. */}
        {isCloud && (
          <div className="mt-6 space-y-3 text-sm text-center">
            <Link href="/forgot-password" className="text-purple-600 hover:text-purple-700">
              Forgot password?
            </Link>
            <p>
              Don&apos;t have an account?{' '}
              <Link href="/signup" className="text-purple-600 hover:text-purple-700 font-medium">
                Sign up
              </Link>
            </p>
          </div>
        )}
        {!isCloud && (
          <p className="mt-6 text-center text-xs text-gray-500">
            This is a self-hosted install — contact your depart admin for an account.
          </p>
        )}
      </div>
    </div>
  );
}

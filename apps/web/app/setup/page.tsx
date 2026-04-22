'use client';

/**
 * /setup — first-run admin bootstrap for self-hosted installs.
 *
 * Flow:
 *   1. On mount, call /api/v1/setup/status.
 *   2. If an admin already exists → redirect to /login.
 *      (Setup-already-done is the normal case after the first use.)
 *   3. Otherwise render the form. On submit, POST bootstrap → redirect
 *      to /login so the freshly-minted admin can sign in.
 *
 * Operators who prefer a headless install can skip this page entirely
 * by setting DEPART_ADMIN_EMAIL + DEPART_ADMIN_PASSWORD in the API
 * environment — the backend creates the admin at startup and this
 * page's status check then bounces them straight to /login.
 */

import { useEffect, useState } from 'react';
import { useRouter } from 'next/navigation';

import { bootstrapAdmin, getSetupStatus } from '@/app/lib/api';


export default function SetupPage() {
  const router = useRouter();
  const [phase, setPhase] = useState<'checking' | 'form' | 'submitting'>('checking');
  const [email, setEmail] = useState('');
  const [password, setPassword] = useState('');
  const [fullName, setFullName] = useState('');
  const [error, setError] = useState('');

  useEffect(() => {
    (async () => {
      try {
        const s = await getSetupStatus();
        if (!s.needs_bootstrap) {
          router.replace('/login');
          return;
        }
        setPhase('form');
      } catch (e: any) {
        setError(e?.message || 'Failed to check setup status.');
        setPhase('form');
      }
    })();
  }, [router]);

  async function handleSubmit(e: React.FormEvent) {
    e.preventDefault();
    setError('');
    setPhase('submitting');
    try {
      await bootstrapAdmin(email, password, fullName || undefined);
      router.push('/login');
    } catch (err: any) {
      setError(err?.response?.data?.detail || err?.message || 'Bootstrap failed.');
      setPhase('form');
    }
  }

  if (phase === 'checking') {
    return (
      <main className="min-h-screen bg-gray-50 flex items-center justify-center px-4">
        <p className="text-gray-500 text-sm">Checking install status…</p>
      </main>
    );
  }

  return (
    <main className="min-h-screen bg-gray-50 flex items-center justify-center px-4 py-16">
      <div className="w-full max-w-md rounded-xl border border-gray-200 bg-white p-8 shadow-sm">
        <div className="mb-6">
          <div className="text-sm font-semibold uppercase tracking-wide text-purple-600">
            First-run setup
          </div>
          <h1 className="mt-2 text-2xl font-bold text-gray-900">
            Create the initial admin
          </h1>
          <p className="mt-2 text-sm text-gray-600">
            This runs once per install. The admin can then invite additional
            operator and viewer users from{' '}
            <code className="rounded bg-gray-100 px-1">/settings/users</code>.
          </p>
        </div>

        {error && (
          <div className="mb-4 rounded-md border border-red-200 bg-red-50 p-3 text-sm text-red-700">
            {error}
          </div>
        )}

        <form onSubmit={handleSubmit} className="space-y-4">
          <Field
            id="email"
            label="Email"
            type="email"
            value={email}
            onChange={setEmail}
            placeholder="admin@acme.com"
            required
          />
          <Field
            id="fullName"
            label="Full name (optional)"
            type="text"
            value={fullName}
            onChange={setFullName}
            placeholder="Ada Admin"
          />
          <Field
            id="password"
            label="Password"
            type="password"
            value={password}
            onChange={setPassword}
            placeholder="At least 8 characters"
            required
            minLength={8}
          />

          <button
            type="submit"
            disabled={phase === 'submitting'}
            className="w-full rounded-md bg-purple-600 px-4 py-2 font-semibold text-white transition hover:bg-purple-700 disabled:cursor-not-allowed disabled:bg-gray-300"
          >
            {phase === 'submitting' ? 'Creating admin…' : 'Create admin →'}
          </button>
        </form>
      </div>
    </main>
  );
}


function Field({
  id,
  label,
  type,
  value,
  onChange,
  placeholder,
  required,
  minLength,
}: {
  id: string;
  label: string;
  type: string;
  value: string;
  onChange: (v: string) => void;
  placeholder?: string;
  required?: boolean;
  minLength?: number;
}) {
  return (
    <div>
      <label htmlFor={id} className="block text-sm font-medium text-gray-700">
        {label}
      </label>
      <input
        id={id}
        type={type}
        value={value}
        onChange={(e) => onChange(e.target.value)}
        placeholder={placeholder}
        required={required}
        minLength={minLength}
        className="mt-1 block w-full rounded-md border border-gray-300 px-3 py-2 text-sm shadow-sm focus:border-purple-400 focus:outline-none focus:ring-1 focus:ring-purple-400"
      />
    </div>
  );
}

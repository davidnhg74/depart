'use client';

/**
 * /settings/users — admin-only user management for self-hosted installs.
 *
 * Lists all users with their role + active status. Admins can:
 *   * create a new user (email, password, role, full name)
 *   * change a user's role (admin / operator / viewer)
 *   * deactivate / reactivate a user
 *   * delete a user
 *
 * Backend-enforced last-admin guardrails: the server refuses to demote,
 * deactivate, or delete the last remaining admin. The UI mirrors that
 * with cheap client-side checks so common mistakes don't even make a
 * round-trip.
 *
 * Non-admin users hit this page and get the self-hosted "forbidden"
 * surface — identical in spirit to the CloudOnlyNotice but for role
 * mismatch, and with a link back to /assess.
 */

import { useEffect, useState } from 'react';

import SelfHostedGuard from '@/app/components/SelfHostedGuard';
import {
  createUser,
  deleteUser,
  listUsers,
  ManagedUser,
  updateUser,
} from '@/app/lib/api';
import { useAuthStore } from '@/app/store/authStore';


export default function UsersPage() {
  return (
    <SelfHostedGuard>
      <AdminOnly>
        <UsersContent />
      </AdminOnly>
    </SelfHostedGuard>
  );
}


/* ─── Admin-only wrapper ───────────────────────────────────────────────── */

function AdminOnly({ children }: { children: React.ReactNode }) {
  const { user } = useAuthStore();
  if (!user) return null;
  if (user.role !== 'admin') {
    return (
      <main className="min-h-screen bg-gray-50">
        <div className="container mx-auto max-w-2xl px-4 py-20">
          <div className="rounded-xl border border-amber-200 bg-amber-50 p-8">
            <h1 className="text-2xl font-bold text-amber-900">Admins only</h1>
            <p className="mt-3 text-amber-800">
              This page manages users for your depart install. Only accounts
              with the <code className="rounded bg-white px-1">admin</code>{' '}
              role can see it. Ask an admin on your team to add you, or to
              change your role.
            </p>
          </div>
        </div>
      </main>
    );
  }
  return <>{children}</>;
}


/* ─── Main content ─────────────────────────────────────────────────────── */

function UsersContent() {
  const [users, setUsers] = useState<ManagedUser[]>([]);
  const [error, setError] = useState('');
  const [loading, setLoading] = useState(true);
  const { user: currentUser } = useAuthStore();

  async function refresh() {
    setError('');
    try {
      setUsers(await listUsers());
    } catch (e: any) {
      setError(e?.response?.data?.detail || e?.message || 'Failed to load users.');
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    void refresh();
  }, []);

  const adminCount = users.filter((u) => u.role === 'admin').length;

  return (
    <main className="min-h-screen bg-gray-50">
      <div className="container mx-auto max-w-5xl px-4 py-12">
        <div className="mb-8">
          <h1 className="text-3xl font-bold text-gray-900">Users</h1>
          <p className="mt-2 text-gray-600">
            Manage who can reach this depart instance.{' '}
            <strong>Admin</strong> can do everything.{' '}
            <strong>Operator</strong> can run migrations and use AI conversion.{' '}
            <strong>Viewer</strong> is read-only.
          </p>
        </div>

        {error && (
          <div className="mb-6 rounded-lg border border-red-200 bg-red-50 p-4 text-sm text-red-700">
            {error}
          </div>
        )}

        <NewUserForm onCreated={refresh} />

        {loading ? (
          <p className="py-6 text-center text-sm text-gray-500">Loading users…</p>
        ) : (
          <UserTable
            users={users}
            currentUserId={currentUser?.id}
            adminCount={adminCount}
            onChange={refresh}
            onError={setError}
          />
        )}
      </div>
    </main>
  );
}


/* ─── Add user form ────────────────────────────────────────────────────── */

function NewUserForm({ onCreated }: { onCreated: () => void }) {
  const [email, setEmail] = useState('');
  const [password, setPassword] = useState('');
  const [fullName, setFullName] = useState('');
  const [role, setRole] = useState<'admin' | 'operator' | 'viewer'>('operator');
  const [saving, setSaving] = useState(false);
  const [message, setMessage] = useState<{ kind: 'ok' | 'err'; text: string } | null>(
    null,
  );

  async function submit(e: React.FormEvent) {
    e.preventDefault();
    setSaving(true);
    setMessage(null);
    try {
      await createUser({
        email,
        password,
        full_name: fullName || undefined,
        role,
      });
      setEmail('');
      setPassword('');
      setFullName('');
      setRole('operator');
      setMessage({ kind: 'ok', text: `Created ${email}.` });
      onCreated();
    } catch (e: any) {
      setMessage({
        kind: 'err',
        text: e?.response?.data?.detail || e?.message || 'Create failed.',
      });
    } finally {
      setSaving(false);
    }
  }

  return (
    <section className="mb-8 rounded-xl border border-gray-200 bg-white p-6 shadow-sm">
      <h2 className="mb-4 text-lg font-semibold text-gray-900">Add a user</h2>
      <form onSubmit={submit} className="grid grid-cols-1 gap-3 md:grid-cols-5">
        <input
          type="email"
          value={email}
          onChange={(e) => setEmail(e.target.value)}
          placeholder="email@company.com"
          required
          className="rounded-md border border-gray-300 px-3 py-2 text-sm md:col-span-2"
        />
        <input
          type="text"
          value={fullName}
          onChange={(e) => setFullName(e.target.value)}
          placeholder="Full name (optional)"
          className="rounded-md border border-gray-300 px-3 py-2 text-sm"
        />
        <input
          type="password"
          value={password}
          onChange={(e) => setPassword(e.target.value)}
          placeholder="Password (8+ chars)"
          required
          minLength={8}
          className="rounded-md border border-gray-300 px-3 py-2 text-sm"
        />
        <select
          value={role}
          onChange={(e) => setRole(e.target.value as 'admin' | 'operator' | 'viewer')}
          className="rounded-md border border-gray-300 px-3 py-2 text-sm"
        >
          <option value="admin">admin</option>
          <option value="operator">operator</option>
          <option value="viewer">viewer</option>
        </select>
        <button
          type="submit"
          disabled={saving}
          className="rounded-md bg-purple-600 px-4 py-2 text-sm font-semibold text-white transition hover:bg-purple-700 disabled:bg-gray-300 md:col-span-1"
        >
          {saving ? 'Adding…' : 'Add user'}
        </button>
      </form>

      {message && (
        <p
          className={`mt-3 text-sm ${
            message.kind === 'ok' ? 'text-green-700' : 'text-red-700'
          }`}
        >
          {message.text}
        </p>
      )}
    </section>
  );
}


/* ─── User table ───────────────────────────────────────────────────────── */

function UserTable({
  users,
  currentUserId,
  adminCount,
  onChange,
  onError,
}: {
  users: ManagedUser[];
  currentUserId: string | undefined;
  adminCount: number;
  onChange: () => void;
  onError: (msg: string) => void;
}) {
  return (
    <section className="rounded-xl border border-gray-200 bg-white shadow-sm">
      <table className="w-full text-sm">
        <thead className="border-b border-gray-200 bg-gray-50 text-xs uppercase tracking-wide text-gray-500">
          <tr>
            <th className="px-4 py-3 text-left font-semibold">User</th>
            <th className="px-4 py-3 text-left font-semibold">Role</th>
            <th className="px-4 py-3 text-left font-semibold">Status</th>
            <th className="px-4 py-3 text-right font-semibold">Actions</th>
          </tr>
        </thead>
        <tbody className="divide-y divide-gray-100">
          {users.map((u) => (
            <UserRow
              key={u.id}
              user={u}
              isSelf={u.id === currentUserId}
              adminCount={adminCount}
              onChange={onChange}
              onError={onError}
            />
          ))}
          {users.length === 0 && (
            <tr>
              <td colSpan={4} className="px-4 py-6 text-center text-gray-500">
                No users yet.
              </td>
            </tr>
          )}
        </tbody>
      </table>
    </section>
  );
}


function UserRow({
  user,
  isSelf,
  adminCount,
  onChange,
  onError,
}: {
  user: ManagedUser;
  isSelf: boolean;
  adminCount: number;
  onChange: () => void;
  onError: (msg: string) => void;
}) {
  const [working, setWorking] = useState(false);
  const isLastAdmin = user.role === 'admin' && adminCount <= 1;

  async function wrap<T>(fn: () => Promise<T>) {
    setWorking(true);
    onError('');
    try {
      await fn();
      onChange();
    } catch (e: any) {
      onError(e?.response?.data?.detail || e?.message || 'Request failed.');
    } finally {
      setWorking(false);
    }
  }

  async function changeRole(next: 'admin' | 'operator' | 'viewer') {
    if (next === user.role) return;
    await wrap(() => updateUser(user.id, { role: next }));
  }

  async function toggleActive() {
    await wrap(() => updateUser(user.id, { is_active: !user.is_active }));
  }

  async function remove() {
    if (!confirm(`Delete user ${user.email}? This cannot be undone.`)) return;
    await wrap(() => deleteUser(user.id));
  }

  return (
    <tr className="text-sm">
      <td className="px-4 py-3">
        <div className="font-medium text-gray-900">{user.email}</div>
        {user.full_name && (
          <div className="text-xs text-gray-500">{user.full_name}</div>
        )}
        {isSelf && <div className="mt-0.5 text-xs italic text-gray-400">you</div>}
      </td>
      <td className="px-4 py-3">
        <select
          value={user.role}
          onChange={(e) =>
            changeRole(e.target.value as 'admin' | 'operator' | 'viewer')
          }
          disabled={working || (isLastAdmin && user.role === 'admin')}
          title={
            isLastAdmin
              ? 'Cannot demote the last admin. Promote someone else first.'
              : ''
          }
          className="rounded-md border border-gray-300 px-2 py-1 text-xs disabled:cursor-not-allowed disabled:bg-gray-50"
        >
          <option value="admin">admin</option>
          <option value="operator">operator</option>
          <option value="viewer">viewer</option>
        </select>
      </td>
      <td className="px-4 py-3">
        <button
          onClick={toggleActive}
          disabled={working || (isLastAdmin && user.is_active)}
          className={`rounded-md px-2 py-0.5 text-xs font-semibold ${
            user.is_active
              ? 'bg-green-100 text-green-800 hover:bg-green-200'
              : 'bg-gray-200 text-gray-600 hover:bg-gray-300'
          } disabled:cursor-not-allowed disabled:opacity-50`}
          title={
            isLastAdmin && user.is_active
              ? 'Cannot deactivate the last active admin.'
              : ''
          }
        >
          {user.is_active ? 'active' : 'deactivated'}
        </button>
      </td>
      <td className="px-4 py-3 text-right">
        <button
          onClick={remove}
          disabled={working || isSelf || isLastAdmin}
          className="rounded-md px-2 py-1 text-xs font-semibold text-red-600 hover:bg-red-50 disabled:cursor-not-allowed disabled:opacity-40"
          title={
            isSelf
              ? "Can't delete yourself."
              : isLastAdmin
              ? "Can't delete the last admin."
              : ''
          }
        >
          delete
        </button>
      </td>
    </tr>
  );
}

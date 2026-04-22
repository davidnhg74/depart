'use client';

import { useEffect, useState } from 'react';
import { useRouter } from 'next/navigation';
import AuthGuard from '@/app/components/AuthGuard';
import { useAuthStore } from '@/app/store/authStore';
import { api } from '@/app/lib/api';
import { cloudRoutesEnabled } from '@/app/lib/cloudRoutes';

function SettingsContent() {
  const { user, setUser } = useAuthStore();
  const [activeTab, setActiveTab] = useState<'profile' | 'security' | 'api-keys'>('profile');
  const [loading, setLoading] = useState(false);
  const [message, setMessage] = useState('');

  // Profile state
  const [fullName, setFullName] = useState(user?.full_name || '');
  const [email, setEmail] = useState(user?.email || '');

  // Security state
  const [currentPassword, setCurrentPassword] = useState('');
  const [newPassword, setNewPassword] = useState('');
  const [confirmPassword, setConfirmPassword] = useState('');

  // API Keys state
  const [apiKeys, setApiKeys] = useState<any[]>([]);
  const [keyName, setKeyName] = useState('');
  const [showNewKey, setShowNewKey] = useState(false);
  const [newKey, setNewKey] = useState('');

  const handleSaveProfile = async () => {
    setLoading(true);
    setMessage('');

    try {
      const response = await api.put('/api/v4/account/profile', {
        full_name: fullName,
        email,
      });
      setUser(response.data);
      setMessage('Profile updated successfully');
    } catch (err: any) {
      setMessage(err.response?.data?.error || 'Failed to update profile');
    } finally {
      setLoading(false);
    }
  };

  const handleChangePassword = async () => {
    if (newPassword !== confirmPassword) {
      setMessage('Passwords do not match');
      return;
    }

    setLoading(true);
    setMessage('');

    try {
      await api.put('/api/v4/account/password', {
        current_password: currentPassword,
        new_password: newPassword,
      });
      setCurrentPassword('');
      setNewPassword('');
      setConfirmPassword('');
      setMessage('Password changed successfully');
    } catch (err: any) {
      setMessage(err.response?.data?.error || 'Failed to change password');
    } finally {
      setLoading(false);
    }
  };

  const handleGenerateKey = async () => {
    if (!keyName.trim()) {
      setMessage('Please enter a name for the API key');
      return;
    }

    setLoading(true);
    setMessage('');

    try {
      const response = await api.post('/api/v4/account/api-keys', {
        name: keyName,
      });
      setNewKey(response.data.key);
      setShowNewKey(true);
      setKeyName('');
      // Reload keys
      const keysRes = await api.get('/api/v4/account/api-keys');
      setApiKeys(keysRes.data.keys);
    } catch (err: any) {
      setMessage(err.response?.data?.error || 'Failed to generate API key');
    } finally {
      setLoading(false);
    }
  };

  const handleRevokeKey = async (keyId: string) => {
    if (!confirm('Are you sure you want to revoke this API key?')) return;

    try {
      await api.delete(`/api/v4/account/api-keys/${keyId}`);
      const keysRes = await api.get('/api/v4/account/api-keys');
      setApiKeys(keysRes.data.keys);
      setMessage('API key revoked');
    } catch (err: any) {
      setMessage(err.response?.data?.error || 'Failed to revoke API key');
    }
  };

  return (
    <div className="max-w-2xl mx-auto py-8">
      <h1 className="text-3xl font-bold text-gray-900 mb-8">Settings</h1>

      {message && (
        <div className={`mb-6 p-4 rounded ${message.includes('success') ? 'bg-green-50 text-green-700' : 'bg-red-50 text-red-700'}`}>
          {message}
        </div>
      )}

      {/* Tabs */}
      <div className="flex gap-0 mb-6 border-b border-gray-200">
        {(['profile', 'security', 'api-keys'] as const).map((tab) => (
          <button
            key={tab}
            onClick={() => setActiveTab(tab)}
            className={`px-4 py-3 font-medium border-b-2 capitalize transition ${
              activeTab === tab
                ? 'border-purple-600 text-purple-600'
                : 'border-transparent text-gray-600 hover:text-gray-900'
            }`}
          >
            {tab === 'api-keys' ? 'API Keys' : tab}
          </button>
        ))}
      </div>

      {/* Profile Tab */}
      {activeTab === 'profile' && (
        <div className="bg-white rounded-lg shadow p-6 space-y-6">
          <div>
            <label className="block text-sm font-medium text-gray-700">Full Name</label>
            <input
              type="text"
              value={fullName}
              onChange={(e) => setFullName(e.target.value)}
              className="mt-1 block w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm focus:outline-none focus:ring-purple-500 focus:border-purple-500"
            />
          </div>

          <div>
            <label className="block text-sm font-medium text-gray-700">Email</label>
            <input
              type="email"
              value={email}
              onChange={(e) => setEmail(e.target.value)}
              className="mt-1 block w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm focus:outline-none focus:ring-purple-500 focus:border-purple-500"
            />
          </div>

          <button
            onClick={handleSaveProfile}
            disabled={loading}
            className="w-full px-4 py-2 bg-purple-600 text-white font-medium rounded-md hover:bg-purple-700 disabled:bg-gray-400"
          >
            {loading ? 'Saving...' : 'Save Changes'}
          </button>
        </div>
      )}

      {/* Security Tab */}
      {activeTab === 'security' && (
        <div className="bg-white rounded-lg shadow p-6 space-y-6">
          <div>
            <label className="block text-sm font-medium text-gray-700">Current Password</label>
            <input
              type="password"
              value={currentPassword}
              onChange={(e) => setCurrentPassword(e.target.value)}
              className="mt-1 block w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm focus:outline-none focus:ring-purple-500 focus:border-purple-500"
            />
          </div>

          <div>
            <label className="block text-sm font-medium text-gray-700">New Password</label>
            <input
              type="password"
              value={newPassword}
              onChange={(e) => setNewPassword(e.target.value)}
              className="mt-1 block w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm focus:outline-none focus:ring-purple-500 focus:border-purple-500"
            />
          </div>

          <div>
            <label className="block text-sm font-medium text-gray-700">Confirm Password</label>
            <input
              type="password"
              value={confirmPassword}
              onChange={(e) => setConfirmPassword(e.target.value)}
              className="mt-1 block w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm focus:outline-none focus:ring-purple-500 focus:border-purple-500"
            />
          </div>

          <button
            onClick={handleChangePassword}
            disabled={loading}
            className="w-full px-4 py-2 bg-purple-600 text-white font-medium rounded-md hover:bg-purple-700 disabled:bg-gray-400"
          >
            {loading ? 'Changing...' : 'Change Password'}
          </button>
        </div>
      )}

      {/* API Keys Tab */}
      {activeTab === 'api-keys' && (
        <div className="bg-white rounded-lg shadow p-6 space-y-6">
          {showNewKey && (
            <div className="p-4 bg-green-50 border border-green-200 rounded">
              <p className="text-sm text-gray-600 mb-2">Save this key in a safe place. You won't be able to see it again:</p>
              <code className="block bg-gray-900 text-green-400 p-3 rounded font-mono text-sm break-all mb-2">{newKey}</code>
              <button
                onClick={() => {
                  navigator.clipboard.writeText(newKey);
                  alert('Copied to clipboard');
                }}
                className="text-sm text-purple-600 hover:text-purple-700 font-medium"
              >
                Copy to clipboard
              </button>
            </div>
          )}

          <div className="space-y-3">
            <label className="block text-sm font-medium text-gray-700">Create New API Key</label>
            <div className="flex gap-2">
              <input
                type="text"
                value={keyName}
                onChange={(e) => setKeyName(e.target.value)}
                placeholder="Key name (e.g., CI/CD, Production)"
                className="flex-1 px-3 py-2 border border-gray-300 rounded-md shadow-sm focus:outline-none focus:ring-purple-500 focus:border-purple-500"
              />
              <button
                onClick={handleGenerateKey}
                disabled={loading}
                className="px-4 py-2 bg-purple-600 text-white font-medium rounded-md hover:bg-purple-700 disabled:bg-gray-400"
              >
                Generate
              </button>
            </div>
          </div>

          {apiKeys.length > 0 && (
            <div>
              <h3 className="font-medium text-gray-900 mb-3">Active Keys</h3>
              <div className="space-y-2">
                {apiKeys.map((key) => (
                  <div key={key.id} className="flex items-center justify-between p-3 bg-gray-50 rounded">
                    <div>
                      <p className="font-medium text-gray-900">{key.name}</p>
                      <p className="text-sm text-gray-600">{key.key_prefix}****</p>
                    </div>
                    <button
                      onClick={() => handleRevokeKey(key.id)}
                      className="text-red-600 hover:text-red-700 font-medium text-sm"
                    >
                      Revoke
                    </button>
                  </div>
                ))}
              </div>
            </div>
          )}
        </div>
      )}
    </div>
  );
}

export default function SettingsPage() {
  // In self-hosted mode the SaaS user-settings page (profile, security,
  // per-user API keys) doesn't apply — there's no user account. Redirect
  // to the instance-settings page which holds the BYOK Anthropic key +
  // license JWT uploader.
  const router = useRouter();
  useEffect(() => {
    if (!cloudRoutesEnabled()) router.replace('/settings/instance');
  }, [router]);

  if (!cloudRoutesEnabled()) return null;

  return (
    <AuthGuard>
      <SettingsContent />
    </AuthGuard>
  );
}

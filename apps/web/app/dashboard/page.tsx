'use client';

import { useEffect } from 'react';
import { useRouter } from 'next/navigation';
import AuthGuard from '@/app/components/AuthGuard';
import { useAuthStore } from '@/app/store/authStore';
import { fetchCurrentUser } from '@/app/lib/api';
import TokenUsageCard from '@/app/dashboard/components/TokenUsageCard';

function DashboardContent() {
  const { user, setUser, isLoading, setLoading } = useAuthStore();
  const router = useRouter();

  useEffect(() => {
    const loadUser = async () => {
      if (!user) {
        setLoading(true);
        try {
          const userData = await fetchCurrentUser();
          if (userData) {
            setUser(userData);
          } else {
            router.push('/login');
          }
        } catch (error) {
          router.push('/login');
        } finally {
          setLoading(false);
        }
      }
    };

    loadUser();
  }, [user, setUser, setLoading, router]);

  if (isLoading || !user) {
    return (
      <div className="min-h-screen flex items-center justify-center">
        <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-purple-600"></div>
      </div>
    );
  }

  return (
    <div className="container mx-auto px-4 py-8">
      <div className="max-w-4xl mx-auto">
        <h1 className="text-3xl font-bold text-gray-900 mb-2">Welcome, {user.full_name || user.email}</h1>
        <p className="text-gray-600 mb-8">Here's your dashboard overview</p>

        <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
          {/* Plan Card */}
          <div className="bg-white rounded-lg shadow p-6">
            <h2 className="text-lg font-semibold text-gray-900 mb-2">Current Plan</h2>
            <p className="text-3xl font-bold text-purple-600 capitalize">{user.plan}</p>
            {user.plan === 'trial' && (
              <p className="text-sm text-gray-600 mt-2">14-day free trial</p>
            )}
            <button
              onClick={() => window.location.href = '/billing'}
              className="mt-4 px-4 py-2 bg-purple-600 text-white font-medium rounded-md hover:bg-purple-700"
            >
              Manage Plan
            </button>
          </div>

          {/* Account Card */}
          <div className="bg-white rounded-lg shadow p-6">
            <h2 className="text-lg font-semibold text-gray-900 mb-2">Account</h2>
            <p className="text-gray-600">{user.email}</p>
            <p className="text-sm text-gray-500 mt-2">
              {user.email_verified ? '✓ Email verified' : 'Email not verified'}
            </p>
            <button
              onClick={() => window.location.href = '/settings'}
              className="mt-4 px-4 py-2 bg-gray-200 text-gray-900 font-medium rounded-md hover:bg-gray-300"
            >
              Edit Profile
            </button>
          </div>

          {/* Quick Actions Card */}
          <div className="bg-white rounded-lg shadow p-6">
            <h2 className="text-lg font-semibold text-gray-900 mb-4">Quick Actions</h2>
            <div className="space-y-2">
              <button
                onClick={() => window.location.href = '/app-impact'}
                className="w-full px-4 py-2 bg-purple-50 text-purple-600 font-medium rounded-md hover:bg-purple-100 text-left"
              >
                Run App-Impact Analysis
              </button>
              <button
                onClick={() => window.location.href = '/support'}
                className="w-full px-4 py-2 bg-gray-50 text-gray-900 font-medium rounded-md hover:bg-gray-100 text-left"
              >
                Support
              </button>
            </div>
          </div>
        </div>

        <div className="mt-6 grid grid-cols-1 lg:grid-cols-2 gap-6">
          <TokenUsageCard />
        </div>
      </div>
    </div>
  );
}

export default function DashboardPage() {
  return (
    <AuthGuard>
      <DashboardContent />
    </AuthGuard>
  );
}

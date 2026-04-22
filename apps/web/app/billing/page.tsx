'use client';

import { useState, useEffect } from 'react';
import AuthGuard from '@/app/components/AuthGuard';
import CloudOnlyNotice from '@/app/components/CloudOnlyNotice';
import { useAuthStore } from '@/app/store/authStore';
import { api } from '@/app/lib/api';
import { cloudRoutesEnabled } from '@/app/lib/cloudRoutes';
import { PLAN_LIMITS } from '@/app/lib/planLimits';

function BillingContent() {
  const { user } = useAuthStore();
  const [invoices, setInvoices] = useState<any[]>([]);
  const [loading, setLoading] = useState(false);

  useEffect(() => {
    const loadData = async () => {
      try {
        const invoicesRes = await api.get('/api/v4/billing/invoices');
        setInvoices(invoicesRes.data.invoices || []);
      } catch (err) {
        console.error('Failed to load billing data');
      }
    };
    loadData();
  }, []);

  const handleCheckout = async (planId: string) => {
    setLoading(true);
    try {
      const response = await api.post('/api/v4/billing/checkout', { plan: planId });
      window.location.href = response.data.checkout_url;
    } catch (err) {
      alert('Failed to start checkout');
      setLoading(false);
    }
  };

  const handleManageBilling = async () => {
    try {
      const response = await api.get('/api/v4/billing/portal');
      window.location.href = response.data.portal_url;
    } catch (err) {
      alert('Failed to open billing portal');
    }
  };

  const currentPlan = (user?.plan ?? 'trial') as keyof typeof PLAN_LIMITS;
  const limits = PLAN_LIMITS[currentPlan];

  return (
    <div className="max-w-4xl mx-auto py-8">
      <h1 className="text-3xl font-bold text-gray-900 mb-2">Billing</h1>
      <p className="text-gray-600 mb-8">Manage your subscription and view your invoices</p>

      {/* Current Plan */}
      <div className="bg-white rounded-lg shadow p-6 mb-8">
        <div className="flex items-center justify-between">
          <div>
            <h2 className="text-2xl font-bold text-gray-900 capitalize">{currentPlan} Plan</h2>
            <p className="text-gray-600 mt-1">
              {currentPlan === 'trial'
                ? 'Your 14-day free trial'
                : `$${
                    currentPlan === 'starter'
                      ? 249
                      : currentPlan === 'professional'
                        ? 599
                        : 'Custom'
                  }/month`}
            </p>
          </div>
          {currentPlan !== 'trial' && (
            <button
              onClick={handleManageBilling}
              className="px-6 py-2 bg-purple-600 text-white font-medium rounded-md hover:bg-purple-700"
            >
              Manage Subscription
            </button>
          )}
        </div>

        {/* Limits */}
        <div className="mt-6 pt-6 border-t border-gray-200">
          <h3 className="font-semibold text-gray-900 mb-3">Included Features</h3>
          <ul className="space-y-2 text-gray-700">
            <li className="flex items-center gap-2">
              <span className="text-purple-600">✓</span>
              {limits.databases ? `${limits.databases} databases` : 'Unlimited databases'}
            </li>
            <li className="flex items-center gap-2">
              <span className="text-purple-600">✓</span>
              {limits.migrations_per_month
                ? `${limits.migrations_per_month} migrations/month`
                : 'Unlimited migrations'}
            </li>
            <li className="flex items-center gap-2">
              <span className="text-purple-600">✓</span>
              {limits.llm_per_month ? `${limits.llm_per_month} AI conversions/month` : 'Unlimited AI conversions'}
            </li>
          </ul>
        </div>
      </div>

      {/* Upgrade CTA */}
      {currentPlan === 'trial' && (
        <div className="bg-gradient-to-r from-purple-600 to-blue-600 rounded-lg shadow p-8 mb-8 text-white">
          <h2 className="text-2xl font-bold mb-2">Ready to upgrade?</h2>
          <p className="mb-6">Choose a plan that fits your needs</p>
          <div className="grid grid-cols-2 gap-4">
            <button
              onClick={() => handleCheckout('starter')}
              disabled={loading}
              className="px-6 py-2 bg-white text-purple-600 font-semibold rounded-md hover:bg-gray-100 disabled:bg-gray-300"
            >
              Starter - $249/mo
            </button>
            <button
              onClick={() => handleCheckout('professional')}
              disabled={loading}
              className="px-6 py-2 bg-white text-purple-600 font-semibold rounded-md hover:bg-gray-100 disabled:bg-gray-300"
            >
              Professional - $599/mo
            </button>
          </div>
        </div>
      )}

      {/* Invoices */}
      <div className="bg-white rounded-lg shadow p-6">
        <h2 className="text-xl font-bold text-gray-900 mb-4">Invoices</h2>
        {invoices.length === 0 ? (
          <p className="text-gray-600">No invoices yet</p>
        ) : (
          <div className="overflow-x-auto">
            <table className="w-full">
              <thead>
                <tr className="border-b border-gray-200">
                  <th className="text-left py-3 px-4 font-semibold text-gray-700">Date</th>
                  <th className="text-left py-3 px-4 font-semibold text-gray-700">Amount</th>
                  <th className="text-left py-3 px-4 font-semibold text-gray-700">Status</th>
                  <th className="text-left py-3 px-4 font-semibold text-gray-700">Action</th>
                </tr>
              </thead>
              <tbody>
                {invoices.map((invoice) => (
                  <tr key={invoice.id} className="border-b border-gray-100 hover:bg-gray-50">
                    <td className="py-3 px-4 text-sm text-gray-900">
                      {new Date(invoice.created).toLocaleDateString()}
                    </td>
                    <td className="py-3 px-4 text-sm text-gray-900 font-medium">${(invoice.amount_paid / 100).toFixed(2)}</td>
                    <td className="py-3 px-4">
                      <span className="inline-block px-2 py-1 bg-green-100 text-green-700 rounded text-xs font-semibold">
                        {invoice.status}
                      </span>
                    </td>
                    <td className="py-3 px-4">
                      <a
                        href={invoice.invoice_pdf}
                        target="_blank"
                        rel="noopener noreferrer"
                        className="text-purple-600 hover:text-purple-700 font-medium text-sm"
                      >
                        Download PDF
                      </a>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </div>
    </div>
  );
}

export default function BillingPage() {
  if (!cloudRoutesEnabled()) return <CloudOnlyNotice page="Billing" />;
  return (
    <AuthGuard>
      <BillingContent />
    </AuthGuard>
  );
}

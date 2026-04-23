'use client';

import { useState } from 'react';
import Link from 'next/link';
import { useRouter, usePathname } from 'next/navigation';
import { useAuthStore } from '@/app/store/authStore';
import { useBrandingStore } from '@/app/store/brandingStore';
import { logout, logoutLocal } from '@/app/lib/api';
import { cloudRoutesEnabled } from '@/app/lib/cloudRoutes';

export default function Navigation() {
  const pathname = usePathname();
  const [dropdownOpen, setDropdownOpen] = useState(false);
  const router = useRouter();
  const { user, isAuthenticated } = useAuthStore();
  const branding = useBrandingStore((s) => s.branding);

  const cloudEnabled = cloudRoutesEnabled();

  const isAdmin = user?.role === 'admin';

  // Navigation differs between cloud and self-hosted builds:
  //  * Cloud: signed-out users see marketing + Login/Sign up; signed-in
  //    users see app pages behind auth.
  //  * Self-hosted: signed-out shows Download + Demo + Log-in CTA;
  //    signed-in surfaces the product pages. Admin-only pages
  //    (Users) are filtered out for non-admins.
  const selfHostedAnon = [
    { href: '/download', label: 'Download', active: pathname === '/download' },
    { href: '/assess', label: 'Demo', active: pathname === '/assess' },
  ];
  const selfHostedAuthed = [
    { href: '/assess', label: 'Assess', active: pathname === '/assess' },
    {
      href: '/migrations',
      label: 'Migrations',
      active: pathname.startsWith('/migrations'),
    },
    { href: '/runbook', label: 'Runbook', active: pathname === '/runbook' },
    {
      href: '/settings/instance',
      label: 'Settings',
      active: pathname === '/settings/instance',
    },
    ...(isAdmin
      ? [
          {
            href: '/settings/users',
            label: 'Users',
            active: pathname === '/settings/users',
          },
          {
            href: '/settings/sso',
            label: 'SSO',
            active: pathname === '/settings/sso',
          },
          {
            href: '/settings/webhooks',
            label: 'Webhooks',
            active: pathname === '/settings/webhooks',
          },
        ]
      : []),
    ...(user?.role === 'admin' || user?.role === 'viewer'
      ? [
          {
            href: '/settings/audit',
            label: 'Audit',
            active: pathname === '/settings/audit',
          },
        ]
      : []),
  ];
  const publicLinks = [
    { href: '/download', label: 'Download', active: pathname === '/download' },
    { href: '/assess', label: 'Demo', active: pathname === '/assess' },
    { href: '/pricing', label: 'Pricing', active: pathname === '/pricing' },
    { href: '/contact', label: 'Contact', active: pathname === '/contact' },
  ];
  const appLinks = [
    { href: '/analyzer', label: 'Analyzer', active: pathname === '/analyzer' },
    { href: '/app-impact', label: 'App Impact', active: pathname === '/app-impact' },
    { href: '/migration', label: 'Migration', active: pathname === '/migration' },
  ];

  const links = !cloudEnabled
    ? isAuthenticated
      ? selfHostedAuthed
      : selfHostedAnon
    : isAuthenticated
    ? appLinks
    : publicLinks;

  const handleLogout = async () => {
    // Self-hosted builds hit /api/v1/auth/logout; cloud builds hit
    // /api/v4/auth/logout. Both tear down the local session + cookies.
    if (cloudEnabled) {
      await logout();
    } else {
      await logoutLocal();
    }
    setDropdownOpen(false);
    router.push('/login');
  };

  return (
    <nav className="bg-white border-b border-gray-200 sticky top-0 z-50">
      <div className="container mx-auto px-4">
        <div className="flex items-center justify-between h-16">
          <Link
            href="/"
            className="text-2xl font-bold flex items-center gap-2"
            style={{ color: branding.primary_color }}
          >
            {branding.logo_url && (
              // eslint-disable-next-line @next/next/no-img-element -- operator-supplied URL, not a Next-Image candidate
              <img
                src={branding.logo_url}
                alt={`${branding.product_name} logo`}
                className="h-8 w-8 object-contain"
              />
            )}
            {branding.product_name}
          </Link>
          <div className="flex gap-8 items-center">
            {links.map((link) => (
              <Link
                key={link.href}
                href={link.href}
                className={`font-medium transition ${
                  link.active
                    ? 'text-purple-600 border-b-2 border-purple-600'
                    : 'text-gray-700 hover:text-purple-600'
                }`}
              >
                {link.label}
              </Link>
            ))}

            {!cloudEnabled && !isAuthenticated ? (
              // Self-hosted, signed out: show a single Log in button
              // so operators aren't stranded if they land on /download
              // before authenticating.
              <Link
                href="/login"
                className="px-4 py-2 bg-purple-600 text-white font-medium rounded-md hover:bg-purple-700"
              >
                Log in
              </Link>
            ) : isAuthenticated && user ? (
              <div className="relative">
                <button
                  onClick={() => setDropdownOpen(!dropdownOpen)}
                  className="flex items-center gap-2 px-3 py-2 text-gray-700 hover:text-purple-600 font-medium"
                >
                  {user.full_name || user.email}
                  <svg
                    className={`w-4 h-4 transition ${dropdownOpen ? 'rotate-180' : ''}`}
                    fill="none"
                    stroke="currentColor"
                    viewBox="0 0 24 24"
                  >
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 14l-7 7m0 0l-7-7m7 7V3" />
                  </svg>
                </button>

                {dropdownOpen && (
                  <div className="absolute right-0 mt-2 w-48 bg-white rounded-md shadow-lg py-2 z-10">
                    <Link
                      href="/dashboard"
                      className="block px-4 py-2 text-gray-700 hover:bg-gray-100"
                      onClick={() => setDropdownOpen(false)}
                    >
                      Dashboard
                    </Link>
                    <Link
                      href="/settings"
                      className="block px-4 py-2 text-gray-700 hover:bg-gray-100"
                      onClick={() => setDropdownOpen(false)}
                    >
                      Settings
                    </Link>
                    <Link
                      href="/billing"
                      className="block px-4 py-2 text-gray-700 hover:bg-gray-100"
                      onClick={() => setDropdownOpen(false)}
                    >
                      Billing
                    </Link>
                    <Link
                      href="/support"
                      className="block px-4 py-2 text-gray-700 hover:bg-gray-100"
                      onClick={() => setDropdownOpen(false)}
                    >
                      Support
                    </Link>
                    <hr className="my-2" />
                    <button
                      onClick={handleLogout}
                      className="w-full text-left px-4 py-2 text-gray-700 hover:bg-gray-100"
                    >
                      Log out
                    </button>
                  </div>
                )}
              </div>
            ) : (
              <div className="flex gap-3">
                <Link
                  href="/login"
                  className="px-4 py-2 text-purple-600 font-medium hover:bg-purple-50 rounded-md"
                >
                  Log in
                </Link>
                <Link
                  href="/signup"
                  className="px-4 py-2 bg-purple-600 text-white font-medium rounded-md hover:bg-purple-700"
                >
                  Sign up
                </Link>
              </div>
            )}
          </div>
        </div>
      </div>
    </nav>
  );
}

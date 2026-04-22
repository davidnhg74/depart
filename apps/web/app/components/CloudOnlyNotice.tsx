/**
 * Placeholder surface shown on cloud-only pages when the build is
 * configured as self-hosted (NEXT_PUBLIC_ENABLE_CLOUD_ROUTES unset/false).
 *
 * We deliberately render something — not a 404 — so operators who
 * bookmark an old URL get a breadcrumb back to the self-hosted flow
 * instead of a dead end.
 */

import Link from 'next/link';


export default function CloudOnlyNotice({
  page,
}: {
  /** Label of the page the operator was trying to reach. */
  page: string;
}) {
  return (
    <main className="min-h-screen bg-gray-50">
      <div className="container mx-auto max-w-2xl px-4 py-20">
        <div className="rounded-xl border border-gray-200 bg-white p-10 shadow-sm">
          <div className="text-sm font-semibold uppercase tracking-wide text-purple-600">
            Not in this build
          </div>
          <h1 className="mt-2 text-3xl font-bold text-gray-900">
            {page} isn&apos;t part of the self-hosted product.
          </h1>
          <p className="mt-4 text-gray-600">
            This depart install is running in self-hosted mode. User
            accounts, billing, and support tickets all live on{' '}
            <a
              href="https://depart.cloud"
              className="text-purple-600 underline"
            >
              depart.cloud
            </a>
            . Nothing in this local install phones home — including you.
          </p>

          <div className="mt-8 space-y-3 rounded-lg border border-gray-100 bg-gray-50 p-5 text-sm">
            <div className="font-semibold text-gray-800">
              Where to go instead
            </div>
            <ul className="space-y-1 text-gray-700">
              <li>
                →{' '}
                <Link href="/assess" className="text-purple-700 underline">
                  /assess
                </Link>{' '}
                — run the assessment (no signup needed)
              </li>
              <li>
                →{' '}
                <Link
                  href="/settings/instance"
                  className="text-purple-700 underline"
                >
                  /settings/instance
                </Link>{' '}
                — configure your Anthropic key and upload a license
              </li>
              <li>
                →{' '}
                <Link href="/download" className="text-purple-700 underline">
                  /download
                </Link>{' '}
                — deployment docs
              </li>
            </ul>
          </div>
        </div>
      </div>
    </main>
  );
}

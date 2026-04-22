/**
 * Status pill for migration rows. Lives outside page.tsx because
 * Next.js enforces that route page files only export the allowed
 * route-config symbols plus `default` — sharing a helper required
 * its own module.
 */

export function StatusBadge({ status }: { status: string }) {
  const styles: Record<string, string> = {
    pending: 'bg-gray-100 text-gray-800',
    queued: 'bg-blue-100 text-blue-800',
    in_progress: 'bg-yellow-100 text-yellow-800',
    completed: 'bg-green-100 text-green-800',
    completed_with_warnings: 'bg-orange-100 text-orange-800',
    failed: 'bg-red-100 text-red-800',
  };
  const cls = styles[status] || 'bg-gray-100 text-gray-800';
  return (
    <span
      className={`inline-flex items-center rounded-md px-2 py-0.5 text-xs font-semibold ${cls}`}
    >
      {status.replace(/_/g, ' ')}
    </span>
  );
}

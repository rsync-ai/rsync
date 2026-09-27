import { Skeleton } from '@/components/ui/skeleton';

/** Placeholder for the Connections + Recent Pipelines row while the first load is in flight. */
export function HomeSkeleton() {
  return (
    <div data-testid="home-skeleton" role="status" aria-busy="true" aria-label="Loading your workspace" className="grid grid-cols-1 xl:grid-cols-[minmax(0,1fr)_minmax(0,2fr)] gap-6 w-full">
      <div className="space-y-3">
        <Skeleton className="h-4 w-32" />
        <div className="rounded-xl border border-gray-200 dark:border-gray-700 p-4 space-y-3">
          <Skeleton className="h-3 w-16" />
          <div className="flex gap-2">
            <Skeleton className="h-6 w-28" />
            <Skeleton className="h-6 w-24" />
          </div>
          <Skeleton className="h-3 w-20" />
          <div className="flex gap-2">
            <Skeleton className="h-6 w-24" />
            <Skeleton className="h-6 w-20" />
          </div>
        </div>
      </div>
      <div className="space-y-3">
        <Skeleton className="h-4 w-36" />
        <div className="rounded-xl border border-gray-200 dark:border-gray-700 divide-y divide-gray-100 dark:divide-gray-700">
          {[0, 1, 2, 3].map((i) => (
            <div key={i} className="flex items-center gap-3 p-4">
              <Skeleton className="h-8 w-16" />
              <div className="flex-1 space-y-2">
                <Skeleton className="h-3.5 w-48" />
                <Skeleton className="h-3 w-64" />
              </div>
            </div>
          ))}
        </div>
      </div>
    </div>
  );
}

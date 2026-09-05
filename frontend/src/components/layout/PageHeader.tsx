import { cn } from "@/lib/utils"

interface PageHeaderProps {
  heading: string
  description?: string
  children?: React.ReactNode
  className?: string
}

export function PageHeader({
  heading,
  description,
  children,
  className,
}: PageHeaderProps) {
  return (
    <div className={cn("flex items-center justify-between", className)}>
      <div className="space-y-1">
        <h1 className="text-2xl font-bold tracking-tight text-zinc-900 dark:text-white">
          {heading}
        </h1>
        {description && (
          <p className="text-sm text-zinc-500 dark:text-zinc-400">
            {description}
          </p>
        )}
      </div>
      {children && <div className="flex items-center gap-3">{children}</div>}
    </div>
  )
}


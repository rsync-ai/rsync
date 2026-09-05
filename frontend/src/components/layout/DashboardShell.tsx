"use client"

import { cn } from "@/lib/utils"
import { useUIStore } from "@/lib/store/useUIStore"

interface DashboardShellProps {
  children: React.ReactNode
  className?: string
}

export function DashboardShell({ children, className }: DashboardShellProps) {
  const collapsed = useUIStore((state) => state.sidebarCollapsed)

  return (
    <main
      className={cn(
        "min-h-screen bg-zinc-50 pt-16 transition-all duration-300 dark:bg-zinc-900",
        "pl-0",
        collapsed ? "md:pl-16" : "md:pl-64",
        className
      )}
    >
      <div className="w-full p-4 sm:p-6">{children}</div>
    </main>
  )
}


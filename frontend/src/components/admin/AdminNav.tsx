"use client"

import { usePathname, useRouter } from "next/navigation"
import { cn } from "@/lib/utils"
import {
  LayoutDashboard,
  Users,
  Mail,
  ScrollText,
  Settings,
  Activity,
  GitBranch,
  Play,
  FileText,
  TestTube,
  Gauge,
} from "lucide-react"

const items = [
  { name: "Overview", href: "/admin", icon: LayoutDashboard },
  { name: "Usage", href: "/admin/usage", icon: Gauge },
  { name: "Users", href: "/admin/users", icon: Users },
  { name: "Invitations", href: "/admin/invitations", icon: Mail },
  { name: "Audit Log", href: "/admin/audit", icon: ScrollText },
  { name: "Settings", href: "/admin/settings", icon: Settings },
  { name: "Health", href: "/admin/health", icon: Activity },
  { name: "Pipelines", href: "/admin/pipelines", icon: GitBranch },
  { name: "Executions", href: "/admin/executions", icon: Play },
  { name: "Raw events", href: "/admin/events", icon: FileText },
  { name: "Test suite", href: "/admin/test-suite", icon: TestTube },
]

export function AdminNav() {
  const pathname = usePathname()
  const router = useRouter()

  return (
    <div className="flex flex-wrap gap-2">
      {items.map((it) => {
        const active =
          it.href === "/admin"
            ? pathname === "/admin"
            : pathname === it.href || pathname.startsWith(it.href + "/")
        const Icon = it.icon
        return (
          <button
            key={it.href}
            type="button"
            onClick={() => router.push(it.href)}
            className={cn(
              "rounded-md px-3 py-1.5 text-sm border transition-colors inline-flex items-center gap-1.5",
              active
                ? "bg-zinc-100 text-zinc-900 border-zinc-200 dark:bg-zinc-800 dark:text-white dark:border-zinc-700"
                : "bg-white text-zinc-600 border-zinc-200 hover:bg-zinc-50 hover:text-zinc-900 dark:bg-zinc-950 dark:text-zinc-300 dark:border-zinc-800 dark:hover:bg-zinc-900 dark:hover:text-white"
            )}
          >
            <Icon className="h-3.5 w-3.5" />
            {it.name}
          </button>
        )
      })}
    </div>
  )
}

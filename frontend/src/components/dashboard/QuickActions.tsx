"use client"

import Link from "next/link"
import { Button } from "@/components/ui/button"
import { 
  GitBranch, 
  Database, 
  Zap,
  ArrowRight,
  RefreshCw,
  Search,
} from "lucide-react"

const actions = [
  {
    title: "CDC Pipeline",
    description: "Create real-time CDC pipeline with AI",
    href: "/cdc/new",
    icon: RefreshCw,
    color: "text-violet-600",
    bgColor: "bg-violet-100 dark:bg-violet-900/30",
  },
  {
    title: "ETL Pipeline",
    description: "Build batch data pipelines",
    href: "/chat",
    icon: GitBranch,
    color: "text-blue-600",
    bgColor: "bg-blue-100 dark:bg-blue-900/30",
  },
  {
    title: "Data Explorer",
    description: "Browse your databases",
    href: "/explorer",
    icon: Search,
    color: "text-emerald-600",
    bgColor: "bg-emerald-100 dark:bg-emerald-900/30",
  },
  {
    title: "Add Connection",
    description: "Connect a new data source",
    href: "/connections",
    icon: Database,
    color: "text-amber-600",
    bgColor: "bg-amber-100 dark:bg-amber-900/30",
  },
]

export function QuickActions() {
  return (
    <div className="space-y-3">
      {actions.map((action) => (
        <Link
          key={action.title}
          href={action.href}
          className="flex items-center gap-4 rounded-lg border border-zinc-200 p-4 transition-all hover:border-zinc-300 hover:bg-zinc-50 dark:border-zinc-800 dark:hover:border-zinc-700 dark:hover:bg-zinc-800/50 group"
        >
          <div className={`rounded-full p-2.5 ${action.bgColor}`}>
            <action.icon className={`h-4 w-4 ${action.color}`} />
          </div>
          <div className="flex-1">
            <p className="font-medium text-zinc-900 dark:text-white">
              {action.title}
            </p>
            <p className="text-xs text-zinc-500 dark:text-zinc-400">
              {action.description}
            </p>
          </div>
          <ArrowRight className="h-4 w-4 text-zinc-400 transition-transform group-hover:translate-x-1" />
        </Link>
      ))}
    </div>
  )
}

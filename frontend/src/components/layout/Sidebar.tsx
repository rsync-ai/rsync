"use client"

import Link from "next/link"
import { usePathname } from "next/navigation"
import { cn } from "@/lib/utils"
import { useUIStore } from "@/lib/store/useUIStore"
import { Button } from "@/components/ui/button"
import { ScrollArea } from "@/components/ui/scroll-area"
import { Tooltip, TooltipContent, TooltipTrigger, TooltipProvider } from "@/components/ui/tooltip"
import {
  Home,
  GitBranch,
  Database,
  Settings,
  ChevronLeft,
  ChevronRight,
  History,
  Cable,
  Clock,
  Search,
  Zap,
  MessageSquareText,
  Sparkles,
  Shield,
  Building2,
  Gauge,
  X,
} from "lucide-react"
import { RsyncLogo } from "@/components/icons/RsyncLogo"

const navigation = [
  {
    title: "Home",
    items: [
      { name: "Home", href: "/", icon: Home },
    ],
  },
  {
    title: "Create",
    items: [
      { name: "Data Pipeline", href: "/chat", icon: Sparkles, highlight: true },
    ],
  },
  {
    title: "Pipelines",
    items: [
      { name: "All Pipelines", href: "/pipelines", icon: GitBranch },
    ],
  },
  {
    title: "Overview",
    items: [
      { name: "Executions", href: "/executions", icon: History },
    ],
  },
  {
    title: "Data",
    items: [
      // `exact` because /explorer/schedules is a sibling in the nav but a CHILD in
      // the URL: without it the prefix match lights up both rows at once.
      { name: "Explorer", href: "/explorer", icon: Search, exact: true },
      { name: "Scheduled Queries", href: "/explorer/schedules", icon: Clock },
      { name: "Connections", href: "/connections", icon: Cable },
      { name: "Connectors", href: "/connectors", icon: Zap },
    ],
  },
  {
    title: "Workspace",
    items: [
      { name: "Workspace", href: "/workspace/settings", icon: Building2 },
      { name: "Usage", href: "/usage", icon: Gauge },
    ],
  },
  {
    title: "Settings",
    items: [
      { name: "Settings", href: "/settings", icon: Settings },
    ],
  },
  {
    title: "Admin",
    items: [{ name: "Admin", href: "/admin", icon: Shield }],
  },
]

interface SidebarProps {
  role?: string
}

export function Sidebar({ role }: SidebarProps) {
  const pathname = usePathname()
  const collapsed = useUIStore((state) => state.sidebarCollapsed)
  const mobileOpen = useUIStore((state) => state.sidebarOpen)
  const setSidebarOpen = useUIStore((state) => state.setSidebarOpen)
  const toggleSidebarCollapsed = useUIStore((state) => state.toggleSidebarCollapsed)

  const isActiveLink = (href: string, exact = false) => {
    if (href === "/") return pathname === "/" || pathname === "/dashboard"
    if (exact) return pathname === href
    return pathname === href || pathname.startsWith(href + "/")
  }

  // NOTE: `role` here is the PLATFORM role from /auth/me (gates the operator-only
  // /admin area), NOT a workspace role. Both can be the string "admin" but they are
  // unrelated — a workspace admin is not a platform admin. Workspace-scoped gating
  // lives in useWorkspaceRole()/can(); never conflate the two.
  const isAdmin = role === "admin"

  const navContent = (isMobile = false) => (
    <ScrollArea className="flex-1 py-4">
      <nav className="space-y-6 px-2">
        {navigation.filter((section) => section.title !== "Admin" || isAdmin).map((section) => (
          <div key={section.title}>
            {(!collapsed || isMobile) && (
              <h4 className="mb-2 px-2 text-xs font-semibold uppercase tracking-wider text-zinc-500 dark:text-zinc-400">
                {section.title}
              </h4>
            )}
            <div className="space-y-1">
              {section.items.map((item) => {
                const isActive = isActiveLink(item.href, "exact" in item && item.exact)
                const Icon = item.icon
                const isHighlighted = 'highlight' in item && item.highlight

                const linkContent = (
                  <Link
                    href={item.href}
                    prefetch={false}
                    // The collapsed rail renders the icon ALONE — no <span>, and the
                    // Radix tooltip is only a description, and only while hovered — so
                    // without this every nav link is nameless (axe `link-name`, serious)
                    // for anyone who has collapsed the rail. Set unconditionally: in the
                    // expanded branch it is identical to the visible text.
                    aria-label={item.name}
                    onClick={() => isMobile && setSidebarOpen(false)}
                    className={cn(
                      "flex items-center gap-3 rounded-lg px-3 py-2 text-sm font-medium transition-all",
                      isHighlighted && !isActive
                        ? "text-violet-700 dark:text-violet-300 hover:bg-violet-50 dark:hover:bg-violet-950/30"
                        : isActive
                          ? "bg-zinc-100 text-zinc-900 dark:bg-zinc-800 dark:text-white"
                          : "text-zinc-600 hover:bg-zinc-50 hover:text-zinc-900 dark:text-zinc-400 dark:hover:bg-zinc-800/50 dark:hover:text-white",
                      collapsed && !isMobile && "justify-center px-2"
                    )}
                  >
                    <Icon className={cn("h-5 w-5 shrink-0", isActive && "text-violet-600", isHighlighted && "text-violet-600")} />
                    {(!collapsed || isMobile) && <span>{item.name}</span>}
                  </Link>
                )

                if (collapsed && !isMobile) {
                  return (
                    <Tooltip key={item.name}>
                      <TooltipTrigger asChild>{linkContent}</TooltipTrigger>
                      <TooltipContent side="right">{item.name}</TooltipContent>
                    </Tooltip>
                  )
                }

                return <div key={item.name}>{linkContent}</div>
              })}
            </div>
          </div>
        ))}
      </nav>
    </ScrollArea>
  )

  return (
    <TooltipProvider delayDuration={0}>
      {/* ── Desktop sidebar (md+) ── */}
      <aside
        className={cn(
          "fixed left-0 top-0 z-40 hidden md:flex h-screen flex-col border-r border-zinc-200 bg-white transition-all duration-300 dark:border-zinc-800 dark:bg-zinc-950",
          collapsed ? "w-16" : "w-64"
        )}
      >
        <div className="flex h-16 items-center justify-between border-b border-zinc-200 px-4 dark:border-zinc-800">
          {!collapsed ? (
            <Link href="/" className="flex items-center gap-2">
              <RsyncLogo size="md" showText />
            </Link>
          ) : (
            <Link href="/" className="mx-auto">
              <RsyncLogo size="md" />
            </Link>
          )}
        </div>

        {navContent()}

        <div className="border-t border-zinc-200 p-2 dark:border-zinc-800">
          <Button
            variant="ghost"
            size="sm"
            // `sidebarCollapsed` is persisted (useUIStore partialize), so the
            // collapsed branch — where this button is a bare chevron — is a state
            // the user carries onto every authenticated page.
            aria-label={collapsed ? "Expand sidebar" : "Collapse sidebar"}
            onClick={() => toggleSidebarCollapsed()}
            className={cn("w-full", collapsed && "px-2")}
          >
            {collapsed ? (
              <ChevronRight className="h-4 w-4" />
            ) : (
              <>
                <ChevronLeft className="h-4 w-4 mr-2" />
                Collapse
              </>
            )}
          </Button>
        </div>
      </aside>

      {/* ── Mobile drawer (< md) ── */}
      {mobileOpen && (
        <div className="fixed inset-0 z-50 md:hidden">
          {/* Backdrop */}
          <div
            className="absolute inset-0 bg-black/50"
            onClick={() => setSidebarOpen(false)}
          />
          {/* Drawer panel */}
          <aside className="absolute left-0 top-0 flex h-full w-72 flex-col border-r border-zinc-200 bg-white dark:border-zinc-800 dark:bg-zinc-950">
            <div className="flex h-16 items-center justify-between border-b border-zinc-200 px-4 dark:border-zinc-800">
              <Link href="/" onClick={() => setSidebarOpen(false)}>
                <RsyncLogo size="md" showText />
              </Link>
              <Button variant="ghost" size="icon-sm" aria-label="Close menu" onClick={() => setSidebarOpen(false)}>
                <X className="h-4 w-4" />
              </Button>
            </div>
            {navContent(true)}
          </aside>
        </div>
      )}
    </TooltipProvider>
  )
}

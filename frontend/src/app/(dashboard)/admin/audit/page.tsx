"use client"

import { useEffect, useState, useCallback } from "react"
import { PageHeader } from "@/components/layout/PageHeader"
import { AdminNav } from "@/components/admin/AdminNav"
import { Card } from "@/components/ui/card"
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table"
import { authFetch } from "@/lib/api/auth-fetch"
import { AccessDeniedState, LoadingState, RateLimitExceededState } from "@/components/admin/AdminStates"
import { toast } from "sonner"
import { Search, ChevronDown, ChevronRight } from "lucide-react"
import type { AdminAuditLog } from "@/lib/api/admin"

export default function AdminAuditPage() {
  const [logs, setLogs] = useState<AdminAuditLog[]>([])
  const [total, setTotal] = useState(0)
  const [loading, setLoading] = useState(true)
  const [pageStatus, setPageStatus] = useState<number | null>(null)
  const [retryAfter, setRetryAfter] = useState<number | null>(null)
  const [search, setSearch] = useState("")
  const [actionFilter, setActionFilter] = useState("")
  const [offset, setOffset] = useState(0)
  const [expandedId, setExpandedId] = useState<string | null>(null)

  const load = useCallback(async () => {
    setLoading(true)
    setPageStatus(null)
    try {
      const params = new URLSearchParams()
      params.set("limit", "50")
      params.set("offset", String(offset))
      if (search) params.set("q", search)
      if (actionFilter) params.set("action", actionFilter)

      const res = await authFetch(`/api/v1/admin/audit-logs?${params}`, { method: "GET" })
      if (res.status === 401) { window.location.href = `/logout?next=${encodeURIComponent(`${window.location.pathname}${window.location.search}`)}`; return }
      if (res.status === 403) { setPageStatus(403); setLoading(false); return }
      if (res.status === 429) {
        setPageStatus(429)
        setRetryAfter(parseInt(res.headers.get("Retry-After") || "", 10))
        setLoading(false)
        return
      }
      const json = await res.json()
      setLogs(json.data || [])
      setTotal(json.total || 0)
    } catch {
      toast.error("Failed to load audit logs")
    }
    setLoading(false)
  }, [offset, search, actionFilter])

  useEffect(() => { load() }, [load])

  return (
    <div className="space-y-6">
      <PageHeader heading="Admin" description="Audit log viewer" />
      <AdminNav />

      {loading ? (
        <LoadingState />
      ) : pageStatus === 403 ? (
        <AccessDeniedState />
      ) : pageStatus === 429 ? (
        <RateLimitExceededState retryAfterSeconds={retryAfter} />
      ) : (
        <Card className="p-4">
          <div className="flex flex-wrap items-center gap-3 mb-4">
            <div className="relative flex-1 min-w-[200px]">
              <Search className="absolute left-2.5 top-2.5 h-4 w-4 text-zinc-400" />
              <Input
                placeholder="Search actions, users, resources..."
                value={search}
                onChange={(e) => { setSearch(e.target.value); setOffset(0) }}
                className="pl-8"
              />
            </div>
            <Input
              placeholder="Filter by action..."
              value={actionFilter}
              onChange={(e) => { setActionFilter(e.target.value); setOffset(0) }}
              className="w-[200px]"
            />
            <Button variant="outline" onClick={load}>Refresh</Button>
          </div>

          <Table>
            <TableHeader>
              <TableRow>
                <TableHead className="w-[30px]"></TableHead>
                <TableHead>Timestamp</TableHead>
                <TableHead>User</TableHead>
                <TableHead>Action</TableHead>
                <TableHead className="hidden md:table-cell">Resource</TableHead>
                <TableHead className="hidden lg:table-cell">IP</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {logs.length === 0 ? (
                <TableRow>
                  <TableCell colSpan={6} className="text-center text-sm text-zinc-500 py-8">
                    No audit logs found
                  </TableCell>
                </TableRow>
              ) : (
                logs.flatMap((log) => {
                  const isExpanded = expandedId === log.id
                  const rows = [
                    <TableRow
                      key={log.id}
                      className="cursor-pointer hover:bg-zinc-50 dark:hover:bg-zinc-900"
                      onClick={() => setExpandedId(isExpanded ? null : log.id)}
                    >
                      <TableCell>
                        {isExpanded ? (
                          <ChevronDown className="h-3.5 w-3.5" />
                        ) : (
                          <ChevronRight className="h-3.5 w-3.5" />
                        )}
                      </TableCell>
                      <TableCell className="text-sm text-zinc-500 font-mono">
                        {new Date(log.created_at).toLocaleString()}
                      </TableCell>
                      <TableCell className="text-sm">
                        {log.user_email || "-"}
                      </TableCell>
                      <TableCell>
                        <Badge variant="outline" className="font-mono text-xs">
                          {log.action}
                        </Badge>
                      </TableCell>
                      <TableCell className="hidden md:table-cell text-sm text-zinc-500">
                        {log.resource_type}
                        {log.resource_id ? ` / ${log.resource_id.slice(0, 8)}...` : ""}
                      </TableCell>
                      <TableCell className="hidden lg:table-cell text-sm text-zinc-500 font-mono">
                        {log.ip_address || "-"}
                      </TableCell>
                    </TableRow>,
                  ]
                  if (isExpanded) {
                    rows.push(
                      <TableRow key={`${log.id}-detail`}>
                        <TableCell colSpan={6} className="bg-zinc-50 dark:bg-zinc-900/50">
                          <pre className="text-xs font-mono p-3 rounded bg-white dark:bg-zinc-950 border border-zinc-200 dark:border-zinc-800 overflow-x-auto max-h-[200px]">
                            {log.details ? JSON.stringify(log.details, null, 2) : "No details"}
                          </pre>
                        </TableCell>
                      </TableRow>
                    )
                  }
                  return rows
                })
              )}
            </TableBody>
          </Table>

          {total > 50 && (
            <div className="flex items-center justify-between mt-4 text-sm text-zinc-500">
              <span>Showing {offset + 1}-{Math.min(offset + 50, total)} of {total}</span>
              <div className="flex gap-2">
                <Button variant="outline" size="sm" disabled={offset === 0} onClick={() => setOffset(Math.max(0, offset - 50))}>
                  Previous
                </Button>
                <Button variant="outline" size="sm" disabled={offset + 50 >= total} onClick={() => setOffset(offset + 50)}>
                  Next
                </Button>
              </div>
            </div>
          )}
        </Card>
      )}
    </div>
  )
}

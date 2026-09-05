"use client"

import { useEffect, useState, useCallback, useRef } from "react"
import { PageHeader } from "@/components/layout/PageHeader"
import { AdminNav } from "@/components/admin/AdminNav"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import { authFetch } from "@/lib/api/auth-fetch"
import { AccessDeniedState, LoadingState, RateLimitExceededState } from "@/components/admin/AdminStates"
import { toast } from "sonner"
import { RefreshCw, Database, Server, Radio, Clock } from "lucide-react"
import type { ServiceHealth } from "@/lib/api/admin"

const serviceIcons: Record<string, React.ElementType> = {
  postgresql: Database,
  redis: Server,
  kafka: Radio,
  temporal: Clock,
}

const statusColors: Record<string, string> = {
  up: "bg-green-500",
  down: "bg-red-500",
  degraded: "bg-yellow-500",
}

export default function AdminHealthPage() {
  const [services, setServices] = useState<ServiceHealth[]>([])
  const [loading, setLoading] = useState(true)
  const [pageStatus, setPageStatus] = useState<number | null>(null)
  const [retryAfter, setRetryAfter] = useState<number | null>(null)
  const [lastChecked, setLastChecked] = useState<Date | null>(null)
  const intervalRef = useRef<ReturnType<typeof setInterval> | null>(null)

  const load = useCallback(async () => {
    try {
      const res = await authFetch("/api/v1/admin/health", { method: "GET" })
      if (res.status === 401) { window.location.href = `/logout?next=${encodeURIComponent(`${window.location.pathname}${window.location.search}`)}`; return }
      if (res.status === 403) { setPageStatus(403); setLoading(false); return }
      if (res.status === 429) {
        setPageStatus(429)
        setRetryAfter(parseInt(res.headers.get("Retry-After") || "", 10))
        setLoading(false)
        return
      }
      const json = await res.json()
      setServices(json.services || [])
      setLastChecked(new Date())
    } catch {
      toast.error("Failed to check health")
    }
    setLoading(false)
  }, [])

  useEffect(() => {
    load()
    intervalRef.current = setInterval(load, 30000)
    return () => {
      if (intervalRef.current) clearInterval(intervalRef.current)
    }
  }, [load])

  return (
    <div className="space-y-6">
      <PageHeader heading="Admin" description="System health" />
      <AdminNav />

      {loading && services.length === 0 ? (
        <LoadingState />
      ) : pageStatus === 403 ? (
        <AccessDeniedState />
      ) : pageStatus === 429 ? (
        <RateLimitExceededState retryAfterSeconds={retryAfter} />
      ) : (
        <>
          <div className="flex items-center justify-between">
            <div className="text-sm text-zinc-500">
              {lastChecked && `Last checked: ${lastChecked.toLocaleTimeString()}`}
              <span className="ml-2 text-xs">(auto-refreshes every 30s)</span>
            </div>
            <Button variant="outline" onClick={load}>
              <RefreshCw className="h-4 w-4 mr-1.5" />
              Refresh
            </Button>
          </div>

          <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-4">
            {services.map((svc) => {
              const Icon = serviceIcons[svc.service] || Server
              return (
                <Card key={svc.service}>
                  <CardHeader className="pb-2">
                    <CardTitle className="flex items-center justify-between text-base">
                      <span className="flex items-center gap-2">
                        <Icon className="h-4 w-4 text-zinc-500" />
                        {svc.service.charAt(0).toUpperCase() + svc.service.slice(1)}
                      </span>
                      <span className="flex items-center gap-1.5">
                        <span className={`inline-block h-2.5 w-2.5 rounded-full ${statusColors[svc.status] || "bg-zinc-400"}`} />
                        <Badge
                          variant={svc.status === "up" ? "default" : svc.status === "down" ? "destructive" : "secondary"}
                        >
                          {svc.status}
                        </Badge>
                      </span>
                    </CardTitle>
                  </CardHeader>
                  <CardContent>
                    <div className="text-sm text-zinc-500">
                      Latency: <span className="font-mono">{svc.latency_ms}ms</span>
                    </div>
                    {svc.error && (
                      <div className="mt-2 text-xs text-red-600 dark:text-red-400 break-all">
                        {svc.error}
                      </div>
                    )}
                  </CardContent>
                </Card>
              )
            })}
          </div>
        </>
      )}
    </div>
  )
}

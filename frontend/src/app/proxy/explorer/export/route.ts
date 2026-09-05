// This route lives under /proxy/ (not /api/) so Traefik does NOT intercept it.
// Traefik routes PathPrefix('/api') → api-gateway, everything else → Next.js.
// The export needs server-side format conversion (CSV/TSV/JSON/XLSX), so it must
// run in Next.js. Moving to /proxy/ keeps it out of Traefik's api-gateway route.
import { NextResponse } from "next/server"
import { cookies } from "next/headers"
import { API_GATEWAY_URL, API_GATEWAY_URL_INTERNAL, getApiUrl } from "@/lib/config/api"
import { activeWorkspaceCookiePair } from "@/lib/workspace/server-workspace"

export const dynamic = "force-dynamic"
export const runtime = "nodejs"

const API_URL = getApiUrl(API_GATEWAY_URL, API_GATEWAY_URL_INTERNAL)

type ExportFormat = "csv" | "tsv" | "json" | "xlsx"

function isSelectOnlySql(sql: string): boolean {
  const s = String(sql || "").trim().toUpperCase()
  return s.startsWith("SELECT") || s.startsWith("WITH")
}

function clampInt(n: unknown, min: number, max: number): number {
  const x = typeof n === "number" ? n : Number(n)
  if (!Number.isFinite(x)) return min
  const v = Math.trunc(x)
  if (v < min) return min
  if (v > max) return max
  return v
}

function toCellValue(v: unknown): string | number | boolean | null {
  if (v === null || v === undefined) return null
  if (typeof v === "number" || typeof v === "boolean") return v
  if (typeof v === "string") return v
  try {
    return JSON.stringify(v)
  } catch {
    return String(v)
  }
}

function escapeDelimited(v: unknown, delimiter: string): string {
  const raw = toCellValue(v)
  if (raw === null) return ""
  const s = String(raw)
  if (s.includes('"')) {
    const escaped = s.replace(/"/g, '""')
    return `"${escaped}"`
  }
  if (s.includes("\n") || s.includes("\r") || s.includes(delimiter)) {
    return `"${s}"`
  }
  return s
}

function formatTimestampForFilename(d: Date): string {
  const pad = (n: number) => String(n).padStart(2, "0")
  return `${d.getFullYear()}${pad(d.getMonth() + 1)}${pad(d.getDate())}-${pad(d.getHours())}${pad(
    d.getMinutes()
  )}${pad(d.getSeconds())}`
}

export async function POST(request: Request) {
  try {
    const cookieStore = await cookies()
    const sessionToken = cookieStore.get("session_token")?.value
    // The upstream /api/v1/explorer/query is workspace-scoped; forward the
    // active-workspace selection alongside auth so an export of a shared-workspace
    // connection doesn't fall back to the personal workspace and 404. Merge into
    // ONE Cookie header (a separate `cookie` header would clobber session_token).
    const workspacePair = activeWorkspaceCookiePair(cookieStore)
    const upstreamCookie = [
      sessionToken ? `session_token=${sessionToken}` : null,
      workspacePair,
    ]
      .filter(Boolean)
      .join("; ")

    const body = await request.json()
    const connection_id = String(body?.connection_id || "")
    const sql = String(body?.sql || "")
    const format = String(body?.format || "csv").toLowerCase() as ExportFormat
    const requestedLimit = body?.limit

    const MAX_EXPORT_ROWS = 10000
    const limit = clampInt(requestedLimit ?? MAX_EXPORT_ROWS, 1, MAX_EXPORT_ROWS)

    if (!connection_id) {
      return NextResponse.json({ error: "connection_id is required" }, { status: 400 })
    }
    if (!sql) {
      return NextResponse.json({ error: "sql is required" }, { status: 400 })
    }
    if (!isSelectOnlySql(sql)) {
      return NextResponse.json({ error: "Only SELECT/WITH queries can be exported" }, { status: 400 })
    }
    if (!["csv", "tsv", "json", "xlsx"].includes(format)) {
      return NextResponse.json({ error: "Unsupported export format" }, { status: 400 })
    }

    const resp = await fetch(`${API_URL}/api/v1/explorer/query`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        ...(upstreamCookie ? { Cookie: upstreamCookie } : {}),
      },
      body: JSON.stringify({ connection_id, sql, limit }),
    })

    const raw = await resp.text()
    let data: any = {}
    try {
      data = raw ? JSON.parse(raw) : {}
    } catch {
      data = { error: raw || "Export failed" }
    }

    if (!resp.ok) {
      const error =
        data?.error ||
        data?.message ||
        (typeof data?.detail === "string" ? data.detail : undefined) ||
        "Export failed"
      return NextResponse.json({ ...data, error }, { status: resp.status })
    }

    const columns: string[] = Array.isArray(data?.columns) ? data.columns.map((c: any) => String(c)) : []
    const rows: Array<Record<string, unknown>> = Array.isArray(data?.rows) ? data.rows : []

    if (columns.length === 0) {
      return NextResponse.json({ error: "No columns returned from query" }, { status: 422 })
    }

    const now = new Date()
    const stamp = formatTimestampForFilename(now)
    const baseName = `explorer-export-${stamp}`

    if (format === "json") {
      const payload = {
        exported_at: now.toISOString(),
        connection_id,
        limit,
        columns,
        rows,
        row_count: typeof data?.row_count === "number" ? data.row_count : rows.length,
        truncated: Boolean(data?.truncated),
        warnings: Array.isArray(data?.warnings) ? data.warnings : undefined,
      }
      const json = JSON.stringify(payload, null, 2)
      return new NextResponse(json, {
        status: 200,
        headers: {
          "Content-Type": "application/json",
          "Content-Disposition": `attachment; filename=${baseName}.json`,
        },
      })
    }

    if (format === "xlsx") {
      let XLSX: any
      try {
        XLSX = await import("xlsx")
      } catch {
        return NextResponse.json(
          {
            error: "Excel export is not available on this deployment (missing dependency: xlsx).",
            hint: "Install the 'xlsx' package in the frontend and redeploy.",
          },
          { status: 501 }
        )
      }
      const aoa: any[][] = [columns]
      for (const r of rows) {
        aoa.push(columns.map((c) => toCellValue(r[c])))
      }
      const ws = XLSX.utils.aoa_to_sheet(aoa)
      const wb = XLSX.utils.book_new()
      XLSX.utils.book_append_sheet(wb, ws, "Results")
      const buf = XLSX.write(wb, { type: "buffer", bookType: "xlsx" }) as Buffer
      return new NextResponse(new Uint8Array(buf), {
        status: 200,
        headers: {
          "Content-Type": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
          "Content-Disposition": `attachment; filename=${baseName}.xlsx`,
        },
      })
    }

    // csv / tsv
    const delimiter = format === "tsv" ? "\t" : ","
    const contentType = format === "tsv" ? "text/tab-separated-values" : "text/csv"
    const ext = format === "tsv" ? "tsv" : "csv"

    let out = columns.join(delimiter) + "\n"
    for (const r of rows) {
      const line = columns.map((c) => escapeDelimited(r[c], delimiter)).join(delimiter)
      out += line + "\n"
    }

    return new NextResponse(out, {
      status: 200,
      headers: {
        "Content-Type": contentType,
        "Content-Disposition": `attachment; filename=${baseName}.${ext}`,
      },
    })
  } catch (error) {
    console.error("Explorer export error:", error)
    return NextResponse.json({ error: "Failed to export" }, { status: 500 })
  }
}

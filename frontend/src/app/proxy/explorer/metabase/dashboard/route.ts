// Lives under /proxy/ (not /api/) so Traefik does NOT intercept it.
import { NextResponse } from "next/server"
import { cookies } from "next/headers"
import { API_GATEWAY_URL, API_GATEWAY_URL_INTERNAL, getApiUrl } from "@/lib/config/api"
import { activeWorkspaceCookiePair } from "@/lib/workspace/server-workspace"

export const dynamic = "force-dynamic"

const API_URL = getApiUrl(API_GATEWAY_URL, API_GATEWAY_URL_INTERNAL)

export async function POST(request: Request) {
  try {
    const cookieStore = await cookies()
    const sessionToken = cookieStore.get("session_token")?.value
    // /api/v1/explorer/metabase/dashboard is workspace-scoped; forward the
    // active-workspace selection alongside auth so creating a dashboard from a
    // shared-workspace connection doesn't fall back to the personal workspace.
    // Merge into ONE Cookie header (a separate `cookie` header clobbers it).
    const workspacePair = activeWorkspaceCookiePair(cookieStore)
    const upstreamCookie = [
      sessionToken ? `session_token=${sessionToken}` : null,
      workspacePair,
    ]
      .filter(Boolean)
      .join("; ")

    const body = await request.json()
    const { sql, name, description, connection_id, source_database } = body

    if (!sql) {
      return NextResponse.json({ error: "sql is required" }, { status: 400 })
    }
    if (!name) {
      return NextResponse.json({ error: "name is required" }, { status: 400 })
    }

    const payload: Record<string, unknown> = { sql, name }
    if (description) payload.description = description
    if (connection_id) payload.connection_id = connection_id
    if (source_database) payload.source_database = source_database

    const response = await fetch(`${API_URL}/api/v1/explorer/metabase/dashboard`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        ...(upstreamCookie ? { Cookie: upstreamCookie } : {}),
      },
      body: JSON.stringify(payload),
    })

    const data = await response.json()

    if (!response.ok) {
      return NextResponse.json(
        { error: data.error || "Failed to create dashboard", hint: data.hint },
        { status: response.status }
      )
    }

    return NextResponse.json(data)
  } catch (error) {
    const msg = error instanceof Error ? error.message : "Failed to create dashboard"
    const isNetwork =
      msg.includes("fetch") || msg.includes("ECONNREFUSED") || msg.includes("ENOTFOUND")
    return NextResponse.json(
      {
        error: isNetwork ? "Could not reach the API Gateway" : msg,
        hint: isNetwork
          ? "Make sure Docker Desktop is running and the API Gateway is up: `docker compose up -d`"
          : undefined,
      },
      { status: isNetwork ? 503 : 500 }
    )
  }
}

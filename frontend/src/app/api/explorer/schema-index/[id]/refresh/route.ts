import { NextRequest, NextResponse } from "next/server"
import { cookies } from "next/headers"
import { API_GATEWAY_URL, API_GATEWAY_URL_INTERNAL, getApiUrl } from "@/lib/config/api"

export const dynamic = "force-dynamic"

export async function POST(
  request: NextRequest,
  { params }: { params: Promise<{ id: string }> }
) {
  try {
    const { id } = await params
    const cookieStore = await cookies()
    const sessionToken = cookieStore.get("session_token")?.value

    const apiUrl = getApiUrl(API_GATEWAY_URL, API_GATEWAY_URL_INTERNAL)
    const url = `${apiUrl}/api/v1/explorer/connections/${id}/schema-index/refresh`

    const res = await fetch(url, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        ...(sessionToken ? { Cookie: `session_token=${sessionToken}` } : {}),
      },
    })

    const data = await res.json()
    return NextResponse.json(data, { status: res.status })
  } catch (error) {
    console.error("Schema index refresh error:", error)
    return NextResponse.json(
      { error: "Failed to refresh schema index" },
      { status: 500 }
    )
  }
}

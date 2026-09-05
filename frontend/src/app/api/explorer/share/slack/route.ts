import { NextRequest, NextResponse } from "next/server"
import { cookies } from "next/headers"
import { API_GATEWAY_URL, API_GATEWAY_URL_INTERNAL, getApiUrl } from "@/lib/config/api"

export const dynamic = "force-dynamic"

export async function POST(request: NextRequest) {
  try {
    const body = await request.json()
    const cookieStore = await cookies()
    const sessionToken = cookieStore.get("session_token")?.value

    const apiUrl = getApiUrl(API_GATEWAY_URL, API_GATEWAY_URL_INTERNAL)
    const url = `${apiUrl}/api/v1/explorer/share/slack`

    const res = await fetch(url, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        ...(sessionToken ? { Cookie: `session_token=${sessionToken}` } : {}),
      },
      body: JSON.stringify(body),
    })

    const data = await res.json()
    return NextResponse.json(data, { status: res.status })
  } catch (error) {
    console.error("Slack share error:", error)
    return NextResponse.json(
      { error: "Failed to share to Slack" },
      { status: 500 }
    )
  }
}

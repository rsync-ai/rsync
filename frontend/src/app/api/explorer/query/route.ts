import { NextResponse } from "next/server"
import { cookies } from "next/headers"
import { API_GATEWAY_URL, API_GATEWAY_URL_INTERNAL, getApiUrl } from '@/lib/config/api'

const API_URL = getApiUrl(API_GATEWAY_URL, API_GATEWAY_URL_INTERNAL)

export async function POST(request: Request) {
  try {
    const cookieStore = await cookies()
    const sessionToken = cookieStore.get("session_token")?.value

    const body = await request.json()
    const { connection_id, sql, limit = 100 } = body

    if (!connection_id) {
      return NextResponse.json({ error: 'connection_id is required' }, { status: 400 })
    }

    if (!sql) {
      return NextResponse.json({ error: 'sql is required' }, { status: 400 })
    }

    // Call API Gateway
    const response = await fetch(`${API_URL}/api/v1/explorer/query`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        ...(sessionToken ? { Cookie: `session_token=${sessionToken}` } : {}),
      },
      body: JSON.stringify({ connection_id, sql, limit }),
    })

    const data = await response.json()

    if (!response.ok) {
      return NextResponse.json(
        { error: data.error || 'Query execution failed' },
        { status: response.status }
      )
    }

    return NextResponse.json(data)

  } catch (error) {
    console.error('Explorer query error:', error)
    return NextResponse.json(
      { error: 'Failed to execute query' },
      { status: 500 }
    )
  }
}

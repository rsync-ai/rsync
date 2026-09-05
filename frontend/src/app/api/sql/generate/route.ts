import { NextResponse } from 'next/server'
import { API_GATEWAY_URL, API_GATEWAY_URL_INTERNAL, getApiUrl } from '@/lib/config/api'

const API_URL = getApiUrl(API_GATEWAY_URL, API_GATEWAY_URL_INTERNAL)

export async function POST(request: Request) {
  try {
    const body = await request.json()
    const { question, dialect = 'postgresql', schema, tables, tables_typed, foreign_keys, max_tokens, temperature } = body

    if (!question) {
      return NextResponse.json({ error: 'Question is required' }, { status: 400 })
    }

    // Build request payload
    const payload: Record<string, unknown> = { question, dialect }
    if (schema) payload.schema = schema
    if (tables) payload.tables = tables
    if (tables_typed) payload.tables_typed = tables_typed
    if (foreign_keys) payload.foreign_keys = foreign_keys
    if (max_tokens) payload.max_tokens = max_tokens
    if (temperature !== undefined) payload.temperature = temperature

    // Call API Gateway (which proxies to LLM Service)
    const response = await fetch(`${API_URL}/api/v1/sql/generate`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify(payload),
    })

    const raw = await response.text()
    let data: any = {}
    try {
      data = raw ? JSON.parse(raw) : {}
    } catch {
      data = { error: raw || 'Failed to generate SQL' }
    }

    if (!response.ok) {
      // Preserve structured error details from the gateway/LLM-service (e.g. `details`, `hint`).
      const error =
        data?.error ||
        data?.message ||
        (typeof data?.detail === 'string' ? data.detail : undefined) ||
        'Failed to generate SQL'
      return NextResponse.json({ ...data, error }, { status: response.status })
    }

    return NextResponse.json(data)

  } catch (error) {
    console.error('SQL generation error:', error)
    return NextResponse.json(
      { error: 'Failed to generate SQL' },
      { status: 500 }
    )
  }
}

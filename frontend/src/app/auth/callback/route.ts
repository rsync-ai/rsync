import { NextResponse } from 'next/server'

// Auth callback - no longer needed, redirect to dashboard
export async function GET(request: Request) {
  const requestUrl = new URL(request.url)
  return NextResponse.redirect(new URL('/dashboard', requestUrl.origin))
}

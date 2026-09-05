import { NextResponse, type NextRequest } from "next/server"

const publicPaths = ['/login', '/signup', '/logout']
const protectedPaths = ['/dashboard', '/connections', '/pipelines', '/executions', '/connectors', '/explorer', '/settings', '/admin']

export async function middleware(request: NextRequest) {
  const { pathname } = request.nextUrl
  
  // Route protection here is PRESENCE-based: we only check whether an `auth_token`
  // cookie exists. That cookie is an HttpOnly cookie set by the API gateway on login
  // (not readable or writable from JS). Middleware does NOT validate the token — token
  // *validity* is enforced server-side by the (dashboard) layout, which calls the
  // gateway and redirects to /logout (NOT /login) on a 401 so a stale cookie gets
  // cleared. Keep that invariant: any redirect of a possibly-invalid session must go
  // through /logout, or the /login -> / bounce below will ping-pong forever
  // (ERR_TOO_MANY_REDIRECTS).
  const isPublicPath = publicPaths.some(path => pathname.startsWith(path))
  const isProtectedPath = protectedPaths.some(path => pathname.startsWith(path)) || pathname === '/'
  
  // For protected paths, redirect to login when no auth cookie is present at all.
  if (isProtectedPath && !isPublicPath) {
    const authToken = request.cookies.get('auth_token')?.value
  
    if (!authToken) {
      const url = request.nextUrl.clone()
      url.pathname = '/login'
      return NextResponse.redirect(url)
    }
  }
  
  // If authenticated user tries to access login/signup, redirect to dashboard
  if (isPublicPath) {
    const authToken = request.cookies.get('auth_token')?.value
    const hasInvite = pathname.startsWith('/signup') && request.nextUrl.searchParams.has('invite')
    // Allow invite links to be opened even if a user is currently logged in (or has a stale cookie).
    // This helps real-world "invite link opened in existing session" cases and makes testing possible.
    if (authToken && pathname !== '/logout' && !hasInvite) {
      const url = request.nextUrl.clone()
      url.pathname = '/'
      return NextResponse.redirect(url)
    }
  }
  
  const response = NextResponse.next()
  
  // Generate trace ID for distributed tracing
  let traceId = request.headers.get('X-Trace-ID')
  if (!traceId) {
    const array = new Uint8Array(16)
    crypto.getRandomValues(array)
    traceId = Array.from(array, (byte) => byte.toString(16).padStart(2, '0')).join('')
  }
  
  response.headers.set('X-Trace-ID', traceId)
  
  return response
}

export const config = {
  matcher: [
    '/((?!_next/static|_next/image|favicon.ico|public|api).*)',
  ],
}

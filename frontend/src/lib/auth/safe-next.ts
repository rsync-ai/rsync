// Shared return-url ("next") guard for the auth pages (login + signup) and the
// post-verification redirect. A SAFE next is an app-relative PATH: it starts with a
// single "/" that is NOT followed by another "/" or a "\". A naive startsWith("/")
// check lets protocol-relative ("//evil.com") and backslash-smuggled ("/\evil.com")
// values through; a browser resolves both to an EXTERNAL origin, which is an
// open-redirect / phishing vector. Anything else (absolute URL, "javascript:", empty,
// null) falls back to "/".
export function safeNextPath(next: string | null | undefined): string {
  return next && /^\/(?![/\\])/.test(next) ? next : "/"
}

// sessionStorage key used to carry a safe next ACROSS the email-verification round
// trip. signup stashes it when email_verified=false; the verify-email page consumes
// and clears it on success. Best-effort same-browser only (a verification link opened
// on another device won't have it) — see BACKLOG for the robust backend-redirect form.
export const POST_VERIFY_NEXT_KEY = "rsync_post_verify_next"

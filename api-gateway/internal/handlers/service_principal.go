package handlers

import (
	"context"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
)

// KI-CDC-STATUS-401-FORCES-LOGOUT (backend half).
//
// Every api-gateway → orchestrator proxy authorizes the caller HERE first
// (requirePipelineWorkspaceRole and friends) and then calls an orchestrator
// route that re-checks the same resource (assertPipelineOwner /
// assertConnectionOwner in backend-orchestrator). The orchestrator accepts the
// call when it carries EITHER X-Internal-Secret (trusted service) OR a user
// principal. With INTERNAL_SERVICE_SECRET unset — the dev default — the proxy
// used to carry neither, so every gated CDC route answered 401 and the gateway
// forwarded that 401 to the browser as if the session had died.
//
// The fix has two halves, both in this file:
//
//  1. The authenticated caller is bound into the REQUEST context when the auth
//     middleware resolves it (bindCallerToRequest), and setInternalServiceSecret
//     forwards it as X-User-ID on every proxied call built from that context.
//     The orchestrator honours X-User-ID only outside production
//     (requirePrincipal step 3) and then runs its own workspace-role gate for
//     that user, so this adds no authority: in production the shared secret is
//     still the only service credential, and in dev the orchestrator re-checks
//     ownership for the same user the gateway already checked.
//  2. browserStatusForUpstream: a 401 from a service-to-service call is never a
//     statement about the browser's session, so it is never relayed as one.

type callerUserIDKey struct{}

// bindCallerToRequest records the authenticated user id on the request context
// so outbound service calls built from c.Request.Context() can name the caller.
// Called by the auth middleware at every point it sets "user_id".
func bindCallerToRequest(c *gin.Context, userID string) {
	userID = strings.TrimSpace(userID)
	if c == nil || c.Request == nil || userID == "" {
		return
	}
	c.Request = c.Request.WithContext(context.WithValue(c.Request.Context(), callerUserIDKey{}, userID))
}

// callerUserIDFromContext returns the user bound by bindCallerToRequest, or "".
func callerUserIDFromContext(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	v, _ := ctx.Value(callerUserIDKey{}).(string)
	return v
}

// browserStatusForUpstream maps the status of a service-to-service answer to
// the status the browser may see. Only 401 changes: the upstream refused the
// GATEWAY's credentials (a missing or mismatched INTERNAL_SERVICE_SECRET), which
// is a server misconfiguration, so the browser gets 502 rather than a 401 its
// auth layer would read as "your session is gone". 403 is forwarded: it is a
// real authorization answer about this user and never ends a session.
func browserStatusForUpstream(status int) int {
	if status == http.StatusUnauthorized {
		return http.StatusBadGateway
	}
	return status
}

// upstreamAuthFailedMessage is the body a remapped 401 carries.
const upstreamAuthFailedMessage = "the orchestrator refused the api-gateway's service credentials (HTTP 401). " +
	"This is a server configuration problem, not your session: check that INTERNAL_SERVICE_SECRET " +
	"is identical on api-gateway and orchestrator."

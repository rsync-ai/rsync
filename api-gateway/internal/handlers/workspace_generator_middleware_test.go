package handlers

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
)

// wsGeneratorEngine stands in for the real router chain: a pre-middleware pins
// workspace_role into the context exactly as WorkspaceContextMiddleware would
// after re-verifying membership, then WorkspaceGeneratorMiddleware gates the
// route. A 200 from the probe means the caller passed the gate.
func wsGeneratorEngine() *gin.Engine {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.Use(func(c *gin.Context) {
		// Only set the role when the header is present, so the empty-header
		// case exercises the "no workspace resolved" fail-closed path.
		if role := c.GetHeader("X-Test-WS-Role"); role != "" {
			c.Set(ctxWorkspaceRole, role)
		}
		c.Next()
	})
	r.POST("/gen", WorkspaceGeneratorMiddleware(), func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"ok": true})
	})
	return r
}

// Connector generation is a TENANT capability: every self-serve signup owns their
// personal workspace, so owner/admin pass and member/viewer are denied. Unknown /
// missing roles fail closed. This is the workspace-axis replacement for the old
// global-role PowerUserOrAdmin gate.
func TestWorkspaceGeneratorMiddleware(t *testing.T) {
	r := wsGeneratorEngine()
	cases := []struct {
		name     string
		role     string
		wantCode int
	}{
		{"owner allowed", "owner", http.StatusOK},
		{"admin allowed", "admin", http.StatusOK},
		{"member denied", "member", http.StatusForbidden},
		{"viewer denied", "viewer", http.StatusForbidden},
		{"unset role denied (fail-closed)", "", http.StatusForbidden},
		{"unknown role denied (fail-closed)", "superuser", http.StatusForbidden},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "/gen", nil)
			if tc.role != "" {
				req.Header.Set("X-Test-WS-Role", tc.role)
			}
			w := httptest.NewRecorder()
			r.ServeHTTP(w, req)
			if w.Code != tc.wantCode {
				t.Fatalf("role %q: want %d, got %d (body %s)", tc.role, tc.wantCode, w.Code, w.Body.String())
			}
		})
	}
}

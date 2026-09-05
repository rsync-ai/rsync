package handlers

import (
	"net/http"
	"os"
	"time"

	"github.com/gin-gonic/gin"
)

// versionStartedAt records when the process started so /version can report
// uptime — useful for spotting "we deployed but the old container is still
// answering" cases.
var versionStartedAt = time.Now().UTC()

// VersionInfo is the canonical /version response shape, identical across
// every Go service in this repo. The drift checker compares these.
type VersionInfo struct {
	Service    string `json:"service"`
	Commit     string `json:"commit"`
	BuiltAt    string `json:"built_at"`
	StartedAt  string `json:"started_at"`
	UptimeSecs int64  `json:"uptime_secs"`
}

// GetVersion is a generic /version handler. Services pass their own name.
//
// Build pipeline must inject GIT_COMMIT and BUILD_TIME as env vars (most
// commonly via Dockerfile ARG → ENV). Falling back to "dev"/"unknown" is
// intentional so local dev still works; the drift check just flags those.
func GetVersion(serviceName string) gin.HandlerFunc {
	return func(c *gin.Context) {
		commit := os.Getenv("GIT_COMMIT")
		if commit == "" {
			commit = "dev"
		}
		builtAt := os.Getenv("BUILD_TIME")
		if builtAt == "" {
			builtAt = "unknown"
		}
		c.JSON(http.StatusOK, VersionInfo{
			Service:    serviceName,
			Commit:     commit,
			BuiltAt:    builtAt,
			StartedAt:  versionStartedAt.Format(time.RFC3339),
			UptimeSecs: int64(time.Since(versionStartedAt).Seconds()),
		})
	}
}

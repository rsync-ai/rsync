package metrics

import (
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
)

// runRoute is the gin route pattern for user-initiated pipeline runs.
// Kept in sync with cmd/server/main.go: POST /pipelines/:id/run, mounted
// under the /api/v1 group, so the full pattern is /api/v1/pipelines/:id/run.
const runRoute = "/api/v1/pipelines/:id/run"

// HTTPMetricsMiddleware records request count + latency for every request,
// and derives auth-failure and pipeline-run-trigger metrics from the
// response. Install it once, early in the chain (after tracing) so it
// observes the final status code of every handler and middleware.
//
// Route cardinality is bounded to gin route *patterns* via c.FullPath();
// unmatched requests (404 with no matched route) are bucketed under
// "unmatched" so arbitrary URLs cannot explode label cardinality.
func HTTPMetricsMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()

		c.Next()

		route := c.FullPath()
		if route == "" {
			route = "unmatched"
		}
		status := c.Writer.Status()
		statusStr := strconv.Itoa(status)

		HTTPRequestsTotal.WithLabelValues(c.Request.Method, route, statusStr).Inc()
		HTTPRequestDurationSeconds.WithLabelValues(c.Request.Method, route).
			Observe(time.Since(start).Seconds())

		switch status {
		case 401:
			AuthFailuresTotal.WithLabelValues("unauthorized").Inc()
		case 403:
			AuthFailuresTotal.WithLabelValues("forbidden").Inc()
		}

		if route == runRoute && c.Request.Method == "POST" {
			if status >= 200 && status < 300 {
				PipelineRunTriggersTotal.WithLabelValues("accepted").Inc()
			} else {
				PipelineRunTriggersTotal.WithLabelValues("rejected").Inc()
			}
		}
	}
}

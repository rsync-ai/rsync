package mcp

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
)

// ConnectorDeployingMarker is a stable phrase present in every "still being set up"
// message. The api-gateway and the frontend key off it to show the result as a
// retryable wait rather than a credential/connectivity failure — keep it in lockstep
// with api-gateway handlers.isConnectorDeployingMessage and the frontend's
// isConnectorDeployingMessage (src/lib/errors/connector-deploying.ts).
const ConnectorDeployingMarker = "connector is still being set up"

// ConnectorDeployingMessage is the user-facing text for a connector whose container
// is still being built or started. It names no transport, interpreter or module: the
// only thing the user can do is wait and retry.
func ConnectorDeployingMessage(displayName string) string {
	displayName = strings.TrimSpace(displayName)
	if displayName == "" {
		displayName = "This"
	}
	return fmt.Sprintf("The %s %s for first use — this can take a minute or two. Please try again shortly.",
		displayName, ConnectorDeployingMarker)
}

// ConnectorDeployingError is returned by StartServer (for callers that set
// NoStdioWhileDeploying) when a container deploy was requested but the container is
// not reachable yet. It is retryable by definition.
type ConnectorDeployingError struct {
	Connector   string
	Version     string
	DisplayName string
}

func (e *ConnectorDeployingError) Error() string {
	return ConnectorDeployingMessage(e.DisplayName)
}

// IsConnectorDeploying reports whether err is (or wraps) a *ConnectorDeployingError.
func IsConnectorDeploying(err error) bool {
	var de *ConnectorDeployingError
	return errors.As(err, &de)
}

// IsConnectorDeployingMessage reports whether a flattened error string carries the
// "still being set up" message (errors cross HTTP/JSON boundaries as strings).
func IsConnectorDeployingMessage(msg string) bool {
	return strings.Contains(msg, ConnectorDeployingMarker)
}

// missingModuleRe matches a Python import failure for a third-party dependency:
// "No module named 'pymongo'" / "ModuleNotFoundError: No module named pymongo.errors".
var missingModuleRe = regexp.MustCompile(`(?i)(ModuleNotFoundError|No module named\s)`)

// IsMissingModuleError reports whether msg is a Python "No module named" failure.
func IsMissingModuleError(msg string) bool {
	return missingModuleRe.MatchString(msg)
}

// FriendlyTestConnectionError maps a failed connection test's raw error to the
// user-facing "still being set up" message when the failure is a symptom of the
// connector's container not being ready: either StartServer said so directly, or
// the request reached a stdio fallback whose interpreter lacks the connector's
// dependencies ("No module named 'X'"). A connector running in its own image always
// has its dependencies, so an import error on a test is never the user's to fix.
// Returns (mapped, true) when mapped; (errMsg, false) otherwise.
func FriendlyTestConnectionError(displayName, errMsg string) (string, bool) {
	if IsConnectorDeployingMessage(errMsg) || IsMissingModuleError(errMsg) {
		return ConnectorDeployingMessage(displayName), true
	}
	return errMsg, false
}

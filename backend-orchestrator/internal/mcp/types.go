package mcp

import (
	"encoding/json"
	"io"
	"os/exec"
	"sync"
	"time"
)

// ServerInfo holds information about a running MCP server
type ServerInfo struct {
	Name            string           // Connector name (e.g., "mysql", "s3")
	Process         *exec.Cmd        // Running process (for stdio mode)
	PID             int              // Process ID (for stdio mode)
	StartedAt       time.Time        // Start time
	Status          string           // "running", "stopped", "error"
	Port            int              // HTTP port (if applicable)
	Host            string           // HTTP host (for Docker containers)
	ConnType        string           // "stdio" or "http"
	VenvPath        string           // Path to isolated venv
	ContainerID     string           // Docker container ID (for http mode)
	StdinPipe       io.WriteCloser   // Pipe to write to server stdin
	StdoutPipe      io.ReadCloser    // Pipe to read from server stdout
	ResponseDecoder *json.Decoder    // Decoder for stdout
	// DepsError records that the stdio runtime fell back to the system interpreter
	// because dependency setup failed (e.g. the connectors mount is read-only, so the
	// venv could not be created). The process still starts, so every third-party import
	// inside the connector fails with a bare "No module named 'X'" that reads as a broken
	// connector. Callers MUST attach this to any failure they surface.
	DepsError string
	mu        sync.RWMutex // Protect concurrent access
}

// ServerConfig holds configuration for starting an MCP server
type ServerConfig struct {
	Name       string            // Connector name
	Version    string            // Connector version (e.g., "v1.1.0" or "latest")
	ScriptPath string            // Path to connector.py
	Config     map[string]string // Configuration (will be env vars)
	ConnType   string            // "stdio" or "http"
	Port       int               // HTTP port (if http type)
	// RequireHTTP forces the server to be reachable over Docker HTTP. When set,
	// StartServer skips the stdio subprocess fallback and returns an error if the
	// Docker container path cannot be established. Required for callers like the
	// kafka-mcp-sink worker that live in a separate container and cannot reach
	// in-process stdio MCPs.
	RequireHTTP bool
	// DeployWaitTimeout is how long StartServer polls for the container to become
	// reachable after asking tool-generator to deploy/start it. 0 means use the
	// default (60s). Only relevant when RequireHTTP=true or the caller wants to
	// be sure a Docker container is up before proceeding.
	DeployWaitTimeout time.Duration
	// NoStdioWhileDeploying is a per-request opt-in for interactive callers (the UI's
	// Test Connection). When a deploy of the connector's container was requested and
	// the container is still not reachable after DeployWaitTimeout, StartServer returns
	// a *ConnectorDeployingError instead of falling back to a stdio subprocess — that
	// subprocess runs on the orchestrator's interpreter, which has none of the
	// connector's dependencies, so it can only fail with "No module named 'X'".
	// The cold-build deadline extension is also capped at DeployWaitTimeout so the
	// caller answers within its own HTTP budget. When no deploy was possible at all
	// (no tool-generator — e.g. Docker-less Helm batch), stdio is still used.
	// See KI-FIRST-CONNECTION-TEST-FALLS-BACK-TO-AN-UNUSABLE-STDIO-INTERPRETER.
	NoStdioWhileDeploying bool
}

// JSONRPCRequest represents a JSON-RPC 2.0 request
type JSONRPCRequest struct {
	JSONRPC string                 `json:"jsonrpc"` // Must be "2.0"
	ID      int                    `json:"id"`
	Method  string                 `json:"method"`
	Params  map[string]interface{} `json:"params,omitempty"`
}

// JSONRPCResponse represents a JSON-RPC 2.0 response
type JSONRPCResponse struct {
	JSONRPC string                 `json:"jsonrpc"`
	ID      int                    `json:"id"`
	Result  map[string]interface{} `json:"result,omitempty"`
	Error   *JSONRPCError          `json:"error,omitempty"`
}

// JSONRPCError represents a JSON-RPC error
type JSONRPCError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Data    interface{} `json:"data,omitempty"`
}

// ExecuteRequest represents a request to execute an MCP operation
type ExecuteRequest struct {
	Connector string                 `json:"connector"` // e.g., "mysql"
	Version   string                 `json:"version"`   // e.g., "v1.1.0" or "latest" (defaults to "latest")
	Operation string                 `json:"operation"` // e.g., "query", "execute"
	Config    map[string]string      `json:"config"`    // Connection config
	Params    map[string]interface{} `json:"params"`    // Operation parameters

	// NoStdioWhileDeploying / DeployWaitTimeout are forwarded to ServerConfig
	// (see there). In-process only; never serialized.
	NoStdioWhileDeploying bool          `json:"-"`
	DeployWaitTimeout     time.Duration `json:"-"`
}

// ExecuteResponse represents the response from an MCP operation
type ExecuteResponse struct {
	Success bool                   `json:"success"`
	Result  map[string]interface{} `json:"result,omitempty"`
	Error   string                 `json:"error,omitempty"`
}


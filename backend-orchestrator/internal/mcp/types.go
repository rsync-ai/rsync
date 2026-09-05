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
	mu              sync.RWMutex     // Protect concurrent access
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
}

// ExecuteResponse represents the response from an MCP operation
type ExecuteResponse struct {
	Success bool                   `json:"success"`
	Result  map[string]interface{} `json:"result,omitempty"`
	Error   string                 `json:"error,omitempty"`
}


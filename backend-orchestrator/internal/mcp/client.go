package mcp

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/rsync-ai/backend-orchestrator/internal/security"
	"github.com/rsync-ai/backend-orchestrator/internal/telemetry"
	log "github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

var mcpTracer = otel.Tracer("mcp-client")

// GENERIC OPERATION NORMALIZATION
// Maps common operation variations to standard MCP connector operations.
// This ensures plans using any terminology work with all connectors.
var operationAliases = map[string]string{
	// Source operations - reading data
	"extract":      "export",
	"read":         "export",
	"fetch":        "export",
	"pull":         "export",
	"select":       "query",
	"get":          "query",
	"read_data":    "export",
	"extract_data": "export",
	"discover":     "discover_schema",
	"list_tables":  "discover_schema",
	"get_schema":   "discover_schema",
	"schema":       "discover_schema",

	// Destination operations - writing data
	// NOTE: Our Python MCP connectors standardize on `import_data`.
	// normalizing to `import` causes "Unknown tool: <connector>_import".
	"import":      "import_data",
	// Warehouses: prefer warehouse-native operations when explicitly requested.
	// (Connectors may still implement these as safe fallbacks to import_data/upsert_data.)
	"load":        "load",
	"write":       "import_data",
	"push":        "import_data",
	"insert":      "import_data",
	"upload":      "import_data",
	"import_data": "import_data",
	"write_data":  "import_data",
	"load_data":   "load",
	"upsert":      "import_data",
	"merge":       "merge",

	// Connection operations
	"test":     "test_connection",
	"ping":     "test_connection",
	"connect":  "test_connection",
	"validate": "validate_config",
	"check":    "test_connection",

	// CDC convenience aliases (older code paths)
	"start_connector":  "start_sync",
	"stop_connector":   "stop_sync",
	"pause_connector":  "pause",
	"resume_connector": "resume",
	"status":           "get_status",
	"restart":          "restart_connector",
}

// normalizeOperation maps operation variations to standard MCP operations
func normalizeOperation(op string) string {
	// Convert to lowercase for matching
	opLower := strings.ToLower(strings.TrimSpace(op))

	// Check if there's a standard alias
	if normalized, exists := operationAliases[opLower]; exists {
		log.Debugf("Normalized operation: %s -> %s", op, normalized)
		return normalized
	}

	// Return original if no alias found
	return op
}

// Client handles JSON-RPC communication with MCP servers
type Client struct {
	serverManager *ServerManager
	nextRequestID int
	mu            sync.Mutex
}

// NewClient creates a new MCP client
func NewClient(serverManager *ServerManager) *Client {
	return &Client{
		serverManager: serverManager,
		nextRequestID: 1,
	}
}

// Execute executes an operation on an MCP connector
func (c *Client) Execute(req ExecuteRequest) (*ExecuteResponse, error) {
	return c.ExecuteWithContext(context.Background(), req)
}

// ExecuteWithContext executes an operation on an MCP connector with tracing context
func (c *Client) ExecuteWithContext(ctx context.Context, req ExecuteRequest) (*ExecuteResponse, error) {
	// Start a span for the MCP operation
	ctx, span := mcpTracer.Start(ctx, fmt.Sprintf("mcp.%s.%s", req.Connector, req.Operation),
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(
			attribute.String("mcp.connector", req.Connector),
			attribute.String("mcp.operation", req.Operation),
		),
	)
	defer span.End()

	// Determine requested connector version early (needed for correct config validation).
	version := req.Version
	if version == "" {
		version = "latest" // Default to latest if not specified
	}

	// Validate config against the requested connector version (NOT always latest).
	if err := c.serverManager.ValidateConfig(req.Connector, version, req.Config); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "Config validation failed")
		return &ExecuteResponse{
			Success: false,
			Error:   err.Error(),
		}, nil
	}

	// Inject trace_id into params for propagation to MCP server
	traceID := telemetry.TraceIDFromContext(ctx)
	if traceID != "" {
		if req.Params == nil {
			req.Params = make(map[string]interface{})
		}
		req.Params["trace_id"] = traceID
		req.Params["_trace_id"] = traceID // Redundant key to ensure visibility
	}

	// Start MCP server if not running
	serverConfig := ServerConfig{
		Name:    req.Connector,
		Version: version,
		Config:  req.Config,
	}

	server, err := c.serverManager.StartServer(serverConfig)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "Failed to start server")
		return &ExecuteResponse{
			Success: false,
			Error:   fmt.Sprintf("Failed to start server: %v", err),
		}, nil
	}

	log.WithFields(log.Fields{
		"connector":  req.Connector,
		"version":    version,
		"server_pid": server.PID,
	}).Debug("MCP server ready for execution")

	span.SetAttributes(
		attribute.Int("mcp.server.pid", server.PID),
		attribute.String("mcp.version", version),
	)

	if server.Status != "running" {
		span.SetStatus(codes.Error, "Server not running")
		return &ExecuteResponse{
			Success: false,
			Error:   "Server not running",
		}, nil
	}

	// Build JSON-RPC request
	c.mu.Lock()
	requestID := c.nextRequestID
	c.nextRequestID++
	c.mu.Unlock()

	// GENERIC: Normalize operation names for cross-connector compatibility
	// Maps common variations to standard MCP operations
	normalizedOp := normalizeOperation(req.Operation)

	// MCP method naming convention: tools/call
	// The actual operation is passed as an argument "name" (e.g., "mysql_query")
	// and "arguments" (params)
	// IMPORTANT: Tool names are always lowercase (connectors register lowercase tools)
	// IMPORTANT: Use the connector's canonical tool namespace, not the filesystem/container id.
	// Many connectors live under underscore folders (e.g. aws_s3) but register tools using
	// kebab-case connector_type (e.g. aws-s3) via BaseMCPConnector (tool = "{connector_type}_{op}").
	toolPrefix := strings.ToLower(req.Connector)
	if meta, metaErr := c.serverManager.GetConnectorMetadata(req.Connector, version); metaErr == nil {
		if ct := strings.ToLower(strings.TrimSpace(meta.ConnectorType)); ct != "" {
			toolPrefix = ct
		}
	}
	toolName := fmt.Sprintf("%s_%s", toolPrefix, normalizedOp)

	// IMPORTANT: Most connectors expect connection settings under params.config.
	// Do NOT rely on process env vars for per-connection config, since servers are reused.
	// Avoid logging config values (secrets).
	if req.Params == nil {
		req.Params = make(map[string]interface{})
	}
	if _, exists := req.Params["config"]; !exists && req.Config != nil {
		req.Params["config"] = req.Config
	}

	// EXECUTION POLICY: Connection config overrides Plan params for specific infrastructure keys
	// This prevents Agent hallucinations (defaults) from breaking explicit connection settings.
	// We force these keys from Config into Params (top-level) so the connector sees them as explicit instructions.
	// This list is COMPREHENSIVE to cover standard MCP infrastructure patterns.
	enforcedKeys := []string{
		// Format & Encoding
		"format", "file_format", "output_format", "compression", "delimiter", "encoding", "header",
		// Cloud / Region
		"region", "zone", "endpoint", "endpoint_url",
		// Storage
		"bucket", "bucket_name", "container", "container_name",
		// Database / Warehouse
		"database", "schema", "warehouse", "account", "role", "ssl", "sslmode", "verify_ssl",
		// Network
		"host", "hostname", "port",
		// Auth (usually hidden in env, but if passed in config, must be respected)
		"user", "username", "api_key", "token", "client_id",
	}
	if req.Config != nil {
		for _, key := range enforcedKeys {
			if val, ok := req.Config[key]; ok && val != "" {
				// If param exists and is different, log warning and overwrite
				if paramVal, paramExists := req.Params[key]; paramExists {
					// Check if different (need string comparison)
					if strVal, isStr := paramVal.(string); isStr && strVal != val {
						// SECURITY: Mask sensitive values in logs to prevent credential leaks
						if security.IsSensitiveKey(key) {
							log.Warnf("⚠️ Overriding plan param '%s' with connection config value (sensitive - masked)", key)
						} else {
							log.Warnf("⚠️ Overriding plan param '%s'='%s' with connection config value '%s'", key, strVal, val)
						}
					}
				}
				req.Params[key] = val
			}
		}

		// ---------------------------------------------------------------------
		// Canonical format override (GENERIC for ALL destination connectors)
		//
		// Many UIs store destination format as file_format/output_format, while
		// connectors (and plans) may use "format". If a plan explicitly sets
		// format=json but the connection config says csv, we MUST respect the
		// connection config to keep the system predictable and connector-agnostic.
		//
		// NOTE: This intentionally does NOT rely on params.config being present,
		// because some plan steps may already use a "config" object for other
		// purposes (e.g., meta-connectors). We enforce the canonical "format"
		// at the top-level for maximum compatibility.
		// ---------------------------------------------------------------------
		cfgFormat := ""
		for _, k := range []string{"output_format", "file_format", "format", "outputFormat", "fileFormat"} {
			if v, ok := req.Config[k]; ok {
				v = strings.TrimSpace(v)
				if v != "" {
					cfgFormat = v
					break
				}
			}
		}
		if cfgFormat != "" {
			// Only warn when we are changing an existing explicit plan value.
			if existing, ok := req.Params["format"].(string); ok && existing != "" && existing != cfgFormat {
				log.Warnf("⚠️ Overriding plan param 'format'='%s' with connection format '%s'", existing, cfgFormat)
			}
			req.Params["format"] = cfgFormat
		}
	}

	rpcReq := JSONRPCRequest{
		JSONRPC: "2.0",
		ID:      requestID,
		Method:  "tools/call",
		Params: map[string]interface{}{
			"name":      toolName,
			"arguments": req.Params,
		},
	}

	span.SetAttributes(
		attribute.String("mcp.tool", toolName),
		attribute.Int("mcp.request_id", requestID),
	)

	log.Infof("Executing %s operation on %s connector via %s", req.Operation, req.Connector, server.ConnType)

	// Route based on connection type
	var resp *ExecuteResponse
	var execErr error
	if server.ConnType == "http" {
		resp, execErr = c.executeViaHTTP(ctx, server, rpcReq)
	} else {
		resp, execErr = c.executeViaStdio(ctx, server, rpcReq)
	}

	if execErr != nil {
		span.RecordError(execErr)
		span.SetStatus(codes.Error, execErr.Error())
		return resp, execErr
	}

	if resp != nil && !resp.Success {
		span.SetStatus(codes.Error, resp.Error)
	} else {
		span.SetStatus(codes.Ok, "")
	}

	return resp, nil
}

// executeViaStdio executes operation using persistent stdio connection
func (c *Client) executeViaStdio(ctx context.Context, server *ServerInfo, req JSONRPCRequest) (*ExecuteResponse, error) {
	// Lock server for exclusive access during request/response cycle
	server.mu.Lock()
	defer server.mu.Unlock()

	// Check if pipes are available
	if server.StdinPipe == nil || server.ResponseDecoder == nil {
		return &ExecuteResponse{
			Success: false,
			Error:   "Server communication pipes not available",
		}, nil
	}

	// Send request
	// Append newline as many JSON-RPC over stdio implementations expect line-delimited JSON
	reqBytes, err := json.Marshal(req)
	if err != nil {
		return &ExecuteResponse{Success: false, Error: fmt.Sprintf("Failed to marshal request: %v", err)}, nil
	}

	if _, err := server.StdinPipe.Write(reqBytes); err != nil {
		return &ExecuteResponse{Success: false, Error: fmt.Sprintf("Failed to write to stdin: %v", err)}, nil
	}
	if _, err := server.StdinPipe.Write([]byte("\n")); err != nil {
		return &ExecuteResponse{Success: false, Error: fmt.Sprintf("Failed to write newline: %v", err)}, nil
	}

	// Read response
	var rpcResp JSONRPCResponse

	// IMPORTANT: stdio mode MUST respect ctx deadlines/cancellation.
	// Without this, a slow/hung connector can block orchestration indefinitely.
	timeout := 60 * time.Second
	if deadline, ok := ctx.Deadline(); ok {
		remaining := time.Until(deadline) + 250*time.Millisecond // small buffer
		if remaining > 0 && remaining < timeout {
			timeout = remaining
		}
	}

	type decodeResult struct {
		resp JSONRPCResponse
		err  error
	}
	ch := make(chan decodeResult, 1)
	go func() {
		var out JSONRPCResponse
		err := server.ResponseDecoder.Decode(&out)
		ch <- decodeResult{resp: out, err: err}
	}()

	select {
	case <-ctx.Done():
		// Best-effort: terminate the stdio process so we don't leave unread bytes that
		// would corrupt subsequent decodes.
		if server.Process != nil && server.Process.Process != nil {
			_ = server.Process.Process.Kill()
		}
		server.Status = "error"
		return &ExecuteResponse{
			Success: false,
			Error:   fmt.Sprintf("operation cancelled: %v", ctx.Err()),
		}, nil
	case <-time.After(timeout):
		if server.Process != nil && server.Process.Process != nil {
			_ = server.Process.Process.Kill()
		}
		server.Status = "error"
		return &ExecuteResponse{
			Success: false,
			Error:   fmt.Sprintf("stdio response timeout after %s", timeout),
		}, nil
	case r := <-ch:
		rpcResp = r.resp
		if r.err != nil {
			// If decode fails, it might be that the server crashed or closed stdout
			log.Errorf("Failed to decode response from %s: %v", server.Name, r.err)
			return &ExecuteResponse{
				Success: false,
				Error:   fmt.Sprintf("Communication error: %v", r.err),
			}, nil
		}
	}

	// Protocol-level decode succeeded; continue with normal parsing/validation.
	//
	// NOTE: We intentionally do not attempt to drain extra bytes on timeout/cancel; we kill
	// the process above to avoid stream corruption.

	// Check for protocol errors
	if rpcResp.Error != nil {
		return &ExecuteResponse{
			Success: false,
			Error:   rpcResp.Error.Message,
		}, nil
	}

	// Check if the result itself contains a success field
	// Many MCP connectors return {"success": false, "error": "..."} in the result
	if success, hasSuccess := rpcResp.Result["success"]; hasSuccess {
		if successBool, ok := success.(bool); ok && !successBool {
			// The operation failed according to the connector
			errorMsg := "Operation failed"
			if errVal, hasErr := rpcResp.Result["error"]; hasErr {
				if errStr, ok := errVal.(string); ok {
					errorMsg = errStr
				}
			} else if errs, ok := rpcResp.Result["errors"]; ok {
				// ExportResult / ImportResult style: {"success": false, "errors": [...], ...}
				// Pull the first error for a better message.
				switch v := errs.(type) {
				case []interface{}:
					if len(v) > 0 {
						if first, ok := v[0].(map[string]interface{}); ok {
							if sc, ok := first["status_code"]; ok {
								errorMsg = fmt.Sprintf("Operation failed (HTTP %v)", sc)
							}
							if em, ok := first["error"].(string); ok && em != "" {
								errorMsg = em
							}
							if d, ok := first["data"].(string); ok && d != "" {
								// Include a small snippet for debugging
								if len(d) > 200 {
									d = d[:200]
								}
								errorMsg = fmt.Sprintf("%s: %s", errorMsg, d)
							}
						}
					}
				}
			}
			return &ExecuteResponse{
				Success: false,
				Error:   errorMsg,
				Result:  rpcResp.Result,
			}, nil
		}
	}

	return &ExecuteResponse{
		Success: true,
		Result:  rpcResp.Result,
	}, nil
}

// executeViaHTTP executes operation via HTTP to a Docker container.
// IMPORTANT: this must respect ctx cancellation/deadlines so orchestration doesn't "hang" on slow connectors.
func (c *Client) executeViaHTTP(ctx context.Context, server *ServerInfo, req JSONRPCRequest) (*ExecuteResponse, error) {
	// Build URL - Docker containers expose /mcp endpoint
	url := fmt.Sprintf("http://%s:%d/mcp", server.Host, server.Port)

	// Marshal request
	reqBytes, err := json.Marshal(req)
	if err != nil {
		return &ExecuteResponse{Success: false, Error: fmt.Sprintf("Failed to marshal request: %v", err)}, nil
	}

	// Create HTTP request
	httpReq, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewBuffer(reqBytes))
	if err != nil {
		return &ExecuteResponse{Success: false, Error: fmt.Sprintf("Failed to create HTTP request: %v", err)}, nil
	}
	httpReq.Header.Set("Content-Type", "application/json")

	// Execute request
	timeout := 30 * time.Second
	if deadline, ok := ctx.Deadline(); ok {
		remaining := time.Until(deadline) + 250*time.Millisecond // small buffer
		if remaining > 0 && remaining < timeout {
			timeout = remaining
		}
	}
	client := &http.Client{Timeout: timeout}
	resp, err := client.Do(httpReq)
	if err != nil {
		return &ExecuteResponse{Success: false, Error: fmt.Sprintf("HTTP request failed: %v", err)}, nil
	}
	defer resp.Body.Close()

	// Read response body
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return &ExecuteResponse{Success: false, Error: fmt.Sprintf("Failed to read response: %v", err)}, nil
	}

	// Parse JSON-RPC response
	var rpcResp JSONRPCResponse
	if err := json.Unmarshal(body, &rpcResp); err != nil {
		// Try parsing as direct result (some connectors don't wrap in JSON-RPC)
		var directResult map[string]interface{}
		if json.Unmarshal(body, &directResult) == nil {
			// Check if it has success/error fields
			if success, ok := directResult["success"].(bool); ok {
				if !success {
					errMsg := "Operation failed"
					if e, ok := directResult["error"].(string); ok {
						errMsg = e
					}
					return &ExecuteResponse{Success: false, Error: errMsg, Result: directResult}, nil
				}
				return &ExecuteResponse{Success: true, Result: directResult}, nil
			}
			// Return as success if no success field
			return &ExecuteResponse{Success: true, Result: directResult}, nil
		}
		return &ExecuteResponse{Success: false, Error: fmt.Sprintf("Failed to parse response: %v", err)}, nil
	}

	// Check for protocol errors
	if rpcResp.Error != nil {
		return &ExecuteResponse{Success: false, Error: rpcResp.Error.Message}, nil
	}

	// Check if the result contains success field
	if success, hasSuccess := rpcResp.Result["success"]; hasSuccess {
		if successBool, ok := success.(bool); ok && !successBool {
			errorMsg := "Operation failed"
			if errVal, hasErr := rpcResp.Result["error"]; hasErr {
				if errStr, ok := errVal.(string); ok {
					errorMsg = errStr
				}
			} else if errs, ok := rpcResp.Result["errors"]; ok {
				switch v := errs.(type) {
				case []interface{}:
					if len(v) > 0 {
						if first, ok := v[0].(map[string]interface{}); ok {
							if sc, ok := first["status_code"]; ok {
								errorMsg = fmt.Sprintf("Operation failed (HTTP %v)", sc)
							}
							if em, ok := first["error"].(string); ok && em != "" {
								errorMsg = em
							}
							if d, ok := first["data"].(string); ok && d != "" {
								if len(d) > 200 {
									d = d[:200]
								}
								errorMsg = fmt.Sprintf("%s: %s", errorMsg, d)
							}
						}
					}
				}
			}
			return &ExecuteResponse{Success: false, Error: errorMsg, Result: rpcResp.Result}, nil
		}
	}

	return &ExecuteResponse{Success: true, Result: rpcResp.Result}, nil
}

// CallMethod calls a method on an MCP connector using JSON-RPC
func (c *Client) CallMethod(connector string, method string, params map[string]interface{}) (map[string]interface{}, error) {
	// Map direct method calls (like list_tools) to execute requests
	// This might need adjustment if CallMethod is used for things other than tools/call

	// If method starts with "tools/", we treat it as a direct method call, not a tool execution
	if strings.HasPrefix(method, "tools/") || strings.HasPrefix(method, "notifications/") {
		// We need a lower-level execute that supports arbitrary methods
		// But ExecuteRequest struct is opinionated about Connector/Operation.
		// For now, we hack it:

		// TODO: Refactor ExecuteRequest to be more generic or add a RawExecute method
		return nil, fmt.Errorf("direct method calls like %s not yet fully supported in this client wrapper", method)
	}

	req := ExecuteRequest{
		Connector: connector,
		Operation: method,
		Params:    params,
	}

	resp, err := c.Execute(req)
	if err != nil {
		return nil, err
	}

	if !resp.Success {
		return nil, fmt.Errorf("operation failed: %s", resp.Error)
	}

	return resp.Result, nil
}

// TestConnection tests a connection via MCP for a specific connector+version.
// If version is empty, defaults to "latest".
func (c *Client) TestConnection(connector string, version string, config map[string]string) (bool, string) {
	return c.serverManager.TestConnection(connector, version, config)
}

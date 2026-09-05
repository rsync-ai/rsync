package mcp

import (
	"fmt"
)

// NewToolRequest creates a standardized JSON-RPC request for an MCP tool call.
// It handles:
// 1. Operation normalization (e.g. "read" -> "export")
// 2. Tool name formatting (connector_operation)
// 3. Config injection (ensuring config is passed to tool)
// 4. Trace ID propagation
// 5. Parameter initialization
func NewToolRequest(connector, operation string, params map[string]interface{}, config map[string]string, traceID string, requestID int) JSONRPCRequest {
	// 1. Initialize params if nil
	if params == nil {
		params = make(map[string]interface{})
	}

	// 2. Inject Config (GENERIC FIX)
	// Always ensure config is passed, as connectors rely on it for connection settings.
	// We check if it's already there to allow overrides, but default to the connection config.
	if _, exists := params["config"]; !exists && config != nil {
		// Convert map[string]string to map[string]interface{} for consistency
		cfgMap := make(map[string]interface{})
		for k, v := range config {
			cfgMap[k] = v
		}
		params["config"] = cfgMap
	}

	// 3. Inject Trace ID
	if traceID != "" {
		params["trace_id"] = traceID
		params["_trace_id"] = traceID
	}

	// 4. Normalize Operation
	normalizedOp := normalizeOperation(operation)

	// 5. Construct Tool Name
	toolName := fmt.Sprintf("%s_%s", connector, normalizedOp)

	// 6. Return Request
	return JSONRPCRequest{
		JSONRPC: "2.0",
		ID:      requestID,
		Method:  "tools/call",
		Params: map[string]interface{}{
			"name":      toolName,
			"arguments": params,
		},
	}
}

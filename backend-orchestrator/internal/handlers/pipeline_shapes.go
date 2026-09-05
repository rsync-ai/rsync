package handlers

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

// This file is what is left of mcp_connectors.go after the orchestrator's
// connector-metadata routes were deleted (2026-08-29).
//
// Removed: ListMCPConnectors / GetMCPConnector / GetMCPConnectorCapabilities,
// the MCPConnectorMetadata DTO and the loadConnectorMetadata helper, together
// with GET /api/v1/mcp/connectors[/:name[/capabilities]] in
// cmd/orchestrator/main.go. They read `<toolsDir>/<entry>/metadata.json` — a
// depth-1 layout that has not existed since the connector root copies were
// deleted — so the listing always answered `{"connectors":[],"total":0}` and
// both by-name routes always answered 404. See the tombstone comment on the
// route group in cmd/orchestrator/main.go for why deletion rather than repair.
//
// Connector metadata is served, correctly and behind authentication, by
// api-gateway `GET /api/v1/connectors` (api-gateway/internal/handlers/tools.go).

// GetPipelineShapes returns available pipeline shapes/patterns.
//
// Static data: it reads no disk and holds no state, so it is a plain handler
// func rather than a method on a struct carrying a connectors directory.
func GetPipelineShapes(c *gin.Context) {
	traceID := getTraceID(c)

	// Pipeline shapes define the available data flow patterns
	shapes := []gin.H{
		{
			"id":          "batch",
			"name":        "Batch Transfer",
			"description": "One-time or scheduled data extraction and loading",
			"flow":        "Source → Transform → Destination",
			"supports": gin.H{
				"scheduling":   true,
				"incremental":  true,
				"full_refresh": true,
			},
		},
		{
			"id":             "cdc_kafka",
			"name":           "CDC to Kafka",
			"description":    "Real-time Change Data Capture via Debezium to Kafka",
			"flow":           "Source (DB) → Debezium → Kafka",
			"default_engine": "debezium",
			"supports": gin.H{
				"real_time": true,
				"cdc":       true,
			},
			"supported_sources": []string{"mysql", "postgresql", "mongodb"},
		},
		{
			"id":             "cdc_kafka_sink",
			"name":           "CDC to Any Destination",
			"description":    "Real-time CDC via Kafka to any supported destination",
			"flow":           "Source (DB) → Debezium → Kafka → Sink Connector → Destination",
			"default_engine": "debezium",
			"supports": gin.H{
				"real_time": true,
				"cdc":       true,
			},
			"supported_sources":      []string{"mysql", "postgresql", "mongodb"},
			"supported_destinations": []string{"aws-s3", "snowflake", "bigquery", "postgresql", "elasticsearch"},
		},
		{
			"id":          "streaming",
			"name":        "Streaming",
			"description": "Continuous streaming from Kafka to any MCP destination",
			"flow":        "Kafka → MCP Sink → Any Destination",
			"supports": gin.H{
				"real_time":    true,
				"backpressure": true,
			},
			"supported_sinks": []string{"kafka-mcp-sink"},
		},
		{
			"id":          "reverse_etl",
			"name":        "Reverse ETL",
			"description": "Sync data from warehouse to operational systems",
			"flow":        "Warehouse → Transform → Destination (API/DB)",
			"supports": gin.H{
				"incremental": true,
				"scheduling":  true,
			},
		},
	}

	c.JSON(http.StatusOK, gin.H{
		"shapes":   shapes,
		"total":    len(shapes),
		"trace_id": traceID,
	})
}

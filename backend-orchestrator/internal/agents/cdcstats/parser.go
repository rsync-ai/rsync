package cdcstats

import (
	"strings"
	"time"
)

type TableUpdate struct {
	QualifiedName string
	SchemaName    string
	TableName     string
	Op            string // c,u,d,r
	Timestamp     time.Time
}

// ParseDebeziumChange extracts (table, op, ts) from a Debezium message payload.
// Returns ok=false if the payload doesn't look like a Debezium change event.
func ParseDebeziumChange(payload map[string]interface{}, topic string) (TableUpdate, bool) {
	if payload == nil {
		return TableUpdate{}, false
	}
	op, _ := payload["op"].(string)
	op = strings.TrimSpace(op)
	if op == "" {
		return TableUpdate{}, false
	}

	ts := time.Now()
	if v, ok := payload["ts_ms"].(float64); ok && v > 0 {
		ts = time.Unix(0, int64(v)*int64(time.Millisecond)).UTC()
	}

	// Prefer payload.source.{schema|db} and payload.source.table
	schemaName := ""
	tableName := ""
	if src, ok := payload["source"].(map[string]interface{}); ok && src != nil {
		if v, ok := src["schema"].(string); ok {
			schemaName = strings.TrimSpace(v)
		}
		if schemaName == "" {
			if v, ok := src["db"].(string); ok {
				schemaName = strings.TrimSpace(v)
			}
		}
		if v, ok := src["table"].(string); ok {
			tableName = strings.TrimSpace(v)
		}
	}

	// Fallback: parse table from topic suffix (<prefix>.<schema/db>.<table>)
	if tableName == "" {
		parts := strings.Split(topic, ".")
		if len(parts) >= 2 {
			tableName = strings.TrimSpace(parts[len(parts)-1])
		}
		if schemaName == "" && len(parts) >= 3 {
			schemaName = strings.TrimSpace(parts[len(parts)-2])
		}
	}

	if tableName == "" {
		return TableUpdate{}, false
	}

	q := tableName
	if schemaName != "" {
		q = schemaName + "." + tableName
	}

	return TableUpdate{
		QualifiedName: q,
		SchemaName:    schemaName,
		TableName:     tableName,
		Op:            op,
		Timestamp:     ts,
	}, true
}


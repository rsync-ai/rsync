package main

import "strings"

// kafkaConnectStatusSummary is the flat view of a Kafka Connect
// GET /connectors/<name>/status payload that the UI (frontend CDCStatusResponse:
// result.connector_state / result.healthy / result.tasks) and emitCDCStatusMetrics
// (result["connector_state"] / ["task_states"] / ["health_status"]) read.
//
// Before this existed the status handler returned only the raw payload under
// "status", so result.connector_state was always empty and the chat CDC chip showed
// "Status unavailable" while the connector was RUNNING (issue #20, 2026-09-16).
type kafkaConnectStatusSummary struct {
	ConnectorState string
	Tasks          []interface{}
	TaskStates     []string
	Healthy        bool
}

// summarizeKafkaConnectStatus flattens a Kafka Connect status payload
// ({name, connector:{state}, tasks:[{id,state,trace?}]}).
//
// Health uses the same rule as the dependency probe's debezium_task check
// (internal/workers/dependency_probe.go): the connector is RUNNING, it has at least
// one task, and every task is RUNNING. A connector with no tasks is not streaming.
func summarizeKafkaConnectStatus(payload map[string]interface{}) kafkaConnectStatusSummary {
	out := kafkaConnectStatusSummary{ConnectorState: "UNKNOWN", Tasks: []interface{}{}, TaskStates: []string{}}
	if payload == nil {
		return out
	}
	if c, ok := payload["connector"].(map[string]interface{}); ok {
		if s, ok := c["state"].(string); ok && strings.TrimSpace(s) != "" {
			out.ConnectorState = strings.ToUpper(strings.TrimSpace(s))
		}
	}
	allTasksRunning := true
	anyTaskHasTrace := false
	if tasks, ok := payload["tasks"].([]interface{}); ok {
		out.Tasks = tasks
		for _, t := range tasks {
			state := ""
			if tm, ok := t.(map[string]interface{}); ok {
				if s, ok := tm["state"].(string); ok {
					state = strings.ToUpper(strings.TrimSpace(s))
				}
				// A task that carries a stack trace has hit an error, whatever it
				// calls its state. Connect normally only fills trace on a FAILED
				// task, so this is usually implied by the state check below — but
				// "RUNNING with a trace" is precisely the shape that reported
				// healthy while a pipeline moved no rows, and the cost of not
				// trusting it is nothing.
				if tr, ok := tm["trace"].(string); ok && strings.TrimSpace(tr) != "" {
					anyTaskHasTrace = true
				}
			}
			out.TaskStates = append(out.TaskStates, state)
			if state != "RUNNING" {
				allTasksRunning = false
			}
		}
	}
	out.Healthy = out.ConnectorState == "RUNNING" && len(out.TaskStates) > 0 && allTasksRunning && !anyTaskHasTrace
	return out
}

func (s kafkaConnectStatusSummary) healthStatus() string {
	if s.Healthy {
		return "healthy"
	}
	return "unhealthy"
}

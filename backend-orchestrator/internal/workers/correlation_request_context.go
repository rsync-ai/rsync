package workers

import (
	"context"
	"time"
)

// A correlation worker's request has two deadlines that must not share a context.
//
// Doing the work is bounded by how long the waiting Temporal activity listens
// (waitForResponseWithHeartbeats in backend-temporal-adapter
// nl_pipeline_v2_activities.go). Delivering the result is bounded by nothing but
// Redis. With one 30 s context for both, an LLM-backed step that ran past 30 s
// (planner: 41 s on a provider 500 + retry) failed with "context deadline
// exceeded" AND could not route that failure — the routing call reused the dead
// context — so the activity sat out its full 3 min wait before retrying, and the
// user saw "Pipeline May Be Stuck".

// correlationWorkBudget is the time a worker may spend producing a response:
// the activity's wait minus correlationDeliverySlack, so the answer lands while
// someone is still listening. Keep in step with the waits in
// nl_pipeline_v2_activities.go; an agent missing here gets the old 30 s.
var correlationWorkBudget = map[string]time.Duration{
	"intent":               2 * time.Minute,
	"connector_resolver":   3 * time.Minute,
	"connection_validator": 1 * time.Minute,
	"planner":              3 * time.Minute,
	"validator":            2 * time.Minute,
	"cost_estimator":       30 * time.Second,
}

const (
	correlationDeliverySlack   = 5 * time.Second
	correlationDefaultWait     = 30 * time.Second
	correlationDeliveryTimeout = 15 * time.Second
)

// correlationWorkContext bounds the work for one request of agentType.
func correlationWorkContext(parent context.Context, agentType string) (context.Context, context.CancelFunc) {
	wait, ok := correlationWorkBudget[agentType]
	if !ok {
		wait = correlationDefaultWait
	}
	return context.WithTimeout(parent, wait-correlationDeliverySlack)
}

// correlationDeliveryContext bounds routing the result and deleting the request.
// It is detached from the work context on purpose: a result — above all a
// failure caused by the work deadline — must still reach the waiting activity.
func correlationDeliveryContext() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), correlationDeliveryTimeout)
}

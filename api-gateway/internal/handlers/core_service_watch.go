package handlers

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"
)

// The orchestrator's sentinel alerts every admin when a core service it depends on stops
// answering (backend-orchestrator sentinel/service_probes.go). Two outages it cannot
// report, and this watch covers from the gateway side:
//
//   - the orchestrator itself. A process cannot alert on its own death, and every
//     pipeline goes quiet with it.
//   - Kafka. The sentinel sees Kafka go down, but its alert is a Kafka message to
//     rsync.notifications — the one path Kafka being down takes out. This watch
//     delivers in-process through the notifier (Publish persists, dedups and sends),
//     so it needs no broker.
//
// Postgres and the gateway are the two it cannot cover: the notifier stores the alert
// and looks its recipients up in Postgres before sending, and a dead gateway runs no
// watch. Those need an uptime check from outside the deployment (self-hosting.md).

const (
	// coreServiceWatchInterval matches the sentinel's infrastructure tick, so both
	// sides take about the same time to call something down.
	coreServiceWatchInterval = 30 * time.Second
	// coreServiceWatchThreshold is how many consecutive failed checks make an outage.
	// One refused dial during a restart or a rolling deploy is not one.
	coreServiceWatchThreshold = 3
	// coreServiceWatchGrace is how long after the gateway starts before a failure
	// counts. On a stack coming up, the gateway can be serving before the orchestrator
	// has finished starting, and that is not an outage to page every admin about.
	coreServiceWatchGrace = 2 * time.Minute
)

// coreServiceCheck watches one service. failures and alerted belong to the single
// goroutine StartCoreServiceWatch runs, so they need no lock.
type coreServiceCheck struct {
	// subject is the alert's dedup subject, in the sentinel's component-id form so the
	// two producers' alerts about different services never share a dedup key.
	subject string
	name    string
	impact  string
	probe   func() serviceHealth

	failures int
	// alerted is set once this outage has been delivered, so one outage is one alert.
	// It clears when the service answers again.
	alerted bool
}

func defaultCoreServiceChecks() []*coreServiceCheck {
	return []*coreServiceCheck{
		{
			subject: "infrastructure:orchestrator",
			name:    "orchestrator",
			// Scoped to what is certain. Capture (Debezium) and the sink workers are
			// separate processes and may keep moving data; what stops is control through
			// the orchestrator (pipeline_cdc.go) and the sentinel that watches pipelines.
			impact: "CDC pipelines cannot be paused, resumed or restarted while it is down, and nothing watches the running ones: " +
				"the checks that detect a stalled pipeline and alert on it run inside it.",
			// /health answers 200 whenever the process is serving, whatever state its
			// subsystems are in, so a failure here is the process, not a dependency.
			probe: func() serviceHealth {
				return probeHTTPService("orchestrator", strings.TrimRight(orchestratorBaseURL(), "/")+"/health")
			},
		},
		{
			subject: "infrastructure:kafka",
			name:    "Kafka",
			impact:  "Pipelines cannot move data while it is down, and nothing will resume on its own once it is back.",
			probe:   checkKafka,
		},
	}
}

// StartCoreServiceWatch runs the watch until ctx is cancelled. With no publisher it does
// nothing: an outage it could not deliver is not worth probing for.
func StartCoreServiceWatch(ctx context.Context, p assessmentPublisher) {
	if p == nil {
		return
	}
	checks := defaultCoreServiceChecks()
	go func() {
		select {
		case <-ctx.Done():
			return
		case <-time.After(coreServiceWatchGrace):
		}
		ticker := time.NewTicker(coreServiceWatchInterval)
		defer ticker.Stop()
		for {
			for _, c := range checks {
				c.observe(ctx, p)
			}
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}
	}()
}

// observe runs one check and delivers the alert when this is the threshold-th failure in a
// row and the outage has not been delivered yet.
func (c *coreServiceCheck) observe(ctx context.Context, p assessmentPublisher) {
	h := c.probe()
	if h.Status == "up" {
		if c.alerted {
			log.WithField("service", c.name).Info("core service watch: responding again")
		}
		c.failures = 0
		c.alerted = false
		return
	}

	c.failures++
	if c.failures < coreServiceWatchThreshold || c.alerted {
		return
	}

	pctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if err := p.Publish(pctx, coreServiceDownPayload(c)); err != nil {
		// Not marked delivered, so the next tick tries again. When the notifier cannot
		// store the alert, Postgres is usually the reason, and that is the one outage
		// no alert from inside the deployment can reach anybody about.
		log.WithError(err).WithField("service", c.name).Warn("core service watch: could not deliver the outage alert")
		return
	}
	c.alerted = true
	log.WithFields(log.Fields{"service": c.name, "failures": c.failures, "error": h.Error}).
		Error("core service watch: service is not responding; admins alerted")
}

// coreServiceDownPayload is the rsync.notifications-shaped event the sentinel publishes for
// INFRASTRUCTURE_DOWN (sentinel/notify.go buildSentinelNotification + publishInstanceAlert):
// instance scope, so it reaches every admin, and the same code, so it has the same copy
// and category and is muted by the same setting.
//
// The probe's error is not in it. For Kafka it names broker addresses, and neither
// producer puts a raw error in a notification.
func coreServiceDownPayload(c *coreServiceCheck) []byte {
	message := "The " + c.name + " service this deployment depends on is not responding. " + c.impact
	raw, _ := json.Marshal(map[string]interface{}{
		"type":        "infrastructure_down",
		"pipeline_id": "instance",
		"message":     message,
		"timestamp":   time.Now().UTC().Format(time.RFC3339),
		"action_url":  "/admin/health",
		"severity":    "critical",
		"error": map[string]interface{}{
			"failure_type":  "system_error",
			"code":          "INFRASTRUCTURE_DOWN",
			"severity":      "error",
			"audience":      "user",
			"user_message":  message,
			"dedup_subject": c.subject,
		},
	})
	return raw
}

package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
)

type recordingPublisher struct {
	sent [][]byte
	err  error
}

func (r *recordingPublisher) Publish(_ context.Context, raw []byte) error {
	if r.err != nil {
		return r.err
	}
	r.sent = append(r.sent, raw)
	return nil
}

func fakeCheck(up *bool) *coreServiceCheck {
	return &coreServiceCheck{
		subject: "infrastructure:orchestrator",
		name:    "orchestrator",
		impact:  "Impact sentence.",
		probe: func() serviceHealth {
			if *up {
				return serviceHealth{Service: "orchestrator", Status: "up"}
			}
			return serviceHealth{Service: "orchestrator", Status: "down",
				Error: "dial tcp kafka-user:sup3rs3cret@10.0.0.7:9092: connection refused"}
		},
	}
}

// One refused dial is not an outage, and one outage is one alert: nothing before the
// threshold, exactly one at it, nothing more while it lasts, and a fresh one for the
// next outage after a recovery.
func TestTheWatchAlertsOncePerOutageAfterTheThreshold(t *testing.T) {
	up := false
	c := fakeCheck(&up)
	pub := &recordingPublisher{}
	ctx := context.Background()

	for i := 1; i < coreServiceWatchThreshold; i++ {
		c.observe(ctx, pub)
		if len(pub.sent) != 0 {
			t.Fatalf("alerted after %d failure(s); the threshold is %d", i, coreServiceWatchThreshold)
		}
	}
	c.observe(ctx, pub)
	if len(pub.sent) != 1 {
		t.Fatalf("%d alerts at the threshold, want 1", len(pub.sent))
	}
	c.observe(ctx, pub)
	c.observe(ctx, pub)
	if len(pub.sent) != 1 {
		t.Fatalf("%d alerts for one continuing outage, want 1", len(pub.sent))
	}

	up = true
	c.observe(ctx, pub)
	up = false
	for i := 0; i < coreServiceWatchThreshold-1; i++ {
		c.observe(ctx, pub)
	}
	if len(pub.sent) != 1 {
		t.Fatalf("alerted before the threshold after a recovery; the failure count was not reset")
	}
	c.observe(ctx, pub)
	if len(pub.sent) != 2 {
		t.Fatalf("%d alerts after a second outage, want 2 — the recovery did not re-arm the alert", len(pub.sent))
	}
}

// A delivery that failed is not a delivered alert. Marking it sent anyway would make the
// one attempt that failed the only attempt.
func TestAFailedDeliveryIsRetriedNextTick(t *testing.T) {
	up := false
	c := fakeCheck(&up)
	pub := &recordingPublisher{err: errors.New("notifier: persist: connection refused")}
	ctx := context.Background()

	for i := 0; i < coreServiceWatchThreshold; i++ {
		c.observe(ctx, pub)
	}
	if c.alerted {
		t.Fatal("a failed delivery was recorded as delivered")
	}

	pub.err = nil
	c.observe(ctx, pub)
	if len(pub.sent) != 1 {
		t.Fatalf("%d alerts after the notifier recovered, want 1", len(pub.sent))
	}
}

// The payload has to be the sentinel's INFRASTRUCTURE_DOWN shape: instance scope so it
// reaches every admin, the catalog code for its copy and category, a per-service dedup
// subject so two outages are two alerts — and never the probe's raw error.
func TestTheOutageAlertIsAnInstanceWideInfrastructureDown(t *testing.T) {
	up := false
	c := fakeCheck(&up)
	pub := &recordingPublisher{}
	for i := 0; i < coreServiceWatchThreshold; i++ {
		c.observe(context.Background(), pub)
	}
	if len(pub.sent) != 1 {
		t.Fatalf("no alert to inspect (%d sent)", len(pub.sent))
	}
	raw := pub.sent[0]
	if strings.Contains(string(raw), "sup3rs3cret") || strings.Contains(string(raw), "10.0.0.7") {
		t.Errorf("the alert carries the probe's raw error: %s", raw)
	}

	var got struct {
		Type       string `json:"type"`
		PipelineID string `json:"pipeline_id"`
		Message    string `json:"message"`
		ActionURL  string `json:"action_url"`
		Error      struct {
			Code         string `json:"code"`
			Audience     string `json:"audience"`
			UserMessage  string `json:"user_message"`
			DedupSubject string `json:"dedup_subject"`
		} `json:"error"`
	}
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatal(err)
	}
	for name, pair := range map[string][2]string{
		"type":          {got.Type, "infrastructure_down"},
		"pipeline_id":   {got.PipelineID, "instance"},
		"action_url":    {got.ActionURL, "/admin/health"},
		"code":          {got.Error.Code, "INFRASTRUCTURE_DOWN"},
		"audience":      {got.Error.Audience, "user"},
		"dedup_subject": {got.Error.DedupSubject, "infrastructure:orchestrator"},
	} {
		if pair[0] != pair[1] {
			t.Errorf("%s = %q, want %q", name, pair[0], pair[1])
		}
	}
	if !strings.Contains(got.Message, "orchestrator") || !strings.Contains(got.Message, "Impact sentence.") {
		t.Errorf("message does not name the service and what stops: %q", got.Message)
	}
	if got.Error.UserMessage != got.Message {
		t.Errorf("user_message %q differs from message %q", got.Error.UserMessage, got.Message)
	}
}

// Both watched services, each with copy and a subject of its own. A shared subject would
// fold "Kafka is down" into "the orchestrator is down" inside the notifier's dedup window.
func TestEveryWatchedServiceHasItsOwnSubjectAndCopy(t *testing.T) {
	checks := defaultCoreServiceChecks()
	want := map[string]bool{"infrastructure:orchestrator": false, "infrastructure:kafka": false}
	for _, c := range checks {
		if _, ok := want[c.subject]; !ok {
			t.Errorf("unexpected watched subject %q", c.subject)
			continue
		}
		if want[c.subject] {
			t.Errorf("subject %q is watched twice", c.subject)
		}
		want[c.subject] = true
		if c.name == "" || c.impact == "" || c.probe == nil {
			t.Errorf("%s has no name, impact or probe", c.subject)
		}
	}
	for s, seen := range want {
		if !seen {
			t.Errorf("%s is not watched", s)
		}
	}
}

func TestTheWatchDoesNothingWithoutAPublisher(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	StartCoreServiceWatch(ctx, nil) // must not panic or start probing
}

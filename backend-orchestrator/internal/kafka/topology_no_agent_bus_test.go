package kafka

import (
	"context"
	"errors"
	"sort"
	"strings"
	"testing"

	"github.com/IBM/sarama"
	kafkaclient "github.com/rsync-ai/shared/kafkaclient"
)

// The agent command bus is gone. The orchestrator's workers take their requests from
// the Redis correlation store and write their results back there
// (workers/correlation_router.go RouteResult), so an agent command, result, response or
// DLQ topic — or the task.assignments/task.results pair — would be created with no
// producer and no consumer. So would the other topics retired with it:
// pipeline.failed.dlq (no producer), rsync.healer.actions (no producer),
// rsync.sentinel.audit (no consumer; the audit trail is the sentinel_audit_logs table)
// and pipeline.agent.telemetry (no consumer).
//
// EnsurePlatformTopics is the platform's one creator of that topology
// (topology_single_creator_test.go holds that invariant), so it is the place a retired
// topic would quietly come back: re-adding a single entry here provisions a topic on
// every install, including customer-managed clusters where it then sits forever.
func TestEnsurePlatformTopicsCreatesNoAgentBusTopic(t *testing.T) {
	// Flag on: the configuration that creates the most, so a retired name hiding
	// behind the drift gate is caught too.
	t.Setenv("RSYNC_SCHEMA_DRIFT_ENABLED", "true")
	tm, admin := newFakeManager(1)
	if err := tm.EnsurePlatformTopics(context.Background()); err != nil {
		t.Fatalf("EnsurePlatformTopics: %v", err)
	}

	// Non-vacuity: "nothing forbidden was created" is trivially true of a call that
	// created nothing at all.
	if _, ok := admin.created[kafkaclient.Topic("pipeline.domain.events")]; !ok {
		t.Fatalf("EnsurePlatformTopics did not create %s — the fake admin is not "+
			"seeing its creates, so the check below would prove nothing",
			kafkaclient.Topic("pipeline.domain.events"))
	}

	agentPrefix := kafkaclient.TopicPrefix() + "agent."
	removed := map[string]bool{}
	for _, name := range []string{
		"task.assignments",
		"task.results",
		"pipeline.failed.dlq",
		"rsync.healer.actions",
		"rsync.sentinel.audit",
		"pipeline.agent.telemetry",
	} {
		removed[kafkaclient.Topic(name)] = true
	}

	var offending []string
	for name := range admin.created {
		if strings.HasPrefix(name, agentPrefix) || removed[name] {
			offending = append(offending, name)
		}
	}
	if len(offending) > 0 {
		sort.Strings(offending)
		t.Errorf("EnsurePlatformTopics created %d retired topic(s), which nothing "+
			"produces to or consumes from any more:\n  %s",
			len(offending), strings.Join(offending, "\n  "))
	}
}

// TestEnsurePlatformTopicsCreatesExactlyThePlatformSet pins the whole set, both ways
// of the drift flag. A default install gets four platform topics; the three
// rsync.healer.* topics exist only when RSYNC_SCHEMA_DRIFT_ENABLED=true, because only
// then does anything produce to or consume from them.
func TestEnsurePlatformTopicsHealerTopicsFollowTheDriftFlag(t *testing.T) {
	always := []string{
		"pipeline.domain.events",
		"rsync.notifications",
		"pii.scan.request",
		"pii.scan.response",
	}
	healer := []string{
		"rsync.healer.schema-changes",
		"rsync.healer.approved-changes",
		"rsync.healer.results",
	}

	for _, tc := range []struct {
		flag string
		want []string
	}{
		{"", always},
		{"false", always},
		{"TRUE", always}, // exact "true" only, like api-gateway's SchemaDriftEnabled
		{"true", append(append([]string{}, always...), healer...)},
	} {
		t.Run("flag="+tc.flag, func(t *testing.T) {
			t.Setenv("RSYNC_SCHEMA_DRIFT_ENABLED", tc.flag)
			tm, admin := newFakeManager(1)
			if err := tm.EnsurePlatformTopics(context.Background()); err != nil {
				t.Fatalf("EnsurePlatformTopics: %v", err)
			}
			var got, want []string
			for name := range admin.created {
				got = append(got, name)
			}
			for _, name := range tc.want {
				want = append(want, kafkaclient.Topic(name))
			}
			sort.Strings(got)
			sort.Strings(want)
			if strings.Join(got, ",") != strings.Join(want, ",") {
				t.Fatalf("created %v\nwant    %v", got, want)
			}
		})
	}
}

// TestPlatformTopicPIIRequestMatchesResponse: the PII scan round trip is one pair,
// created with one config. pii.scan.request used to be left to the llm-service
// consumer's auto-create, at the broker's defaults.
func TestPlatformTopicPIIRequestMatchesResponse(t *testing.T) {
	t.Setenv("RSYNC_SCHEMA_DRIFT_ENABLED", "")
	tm, admin := newFakeManager(1)
	if err := tm.EnsurePlatformTopics(context.Background()); err != nil {
		t.Fatalf("EnsurePlatformTopics: %v", err)
	}
	req := admin.created[kafkaclient.Topic("pii.scan.request")]
	resp := admin.created[kafkaclient.Topic("pii.scan.response")]
	if req == nil || resp == nil {
		t.Fatalf("pii.scan.request created=%v, pii.scan.response created=%v; want both",
			req != nil, resp != nil)
	}
	if req.NumPartitions != resp.NumPartitions || req.ReplicationFactor != resp.ReplicationFactor {
		t.Errorf("request %dp/rf%d, response %dp/rf%d; want identical geometry",
			req.NumPartitions, req.ReplicationFactor, resp.NumPartitions, resp.ReplicationFactor)
	}
	if len(req.ConfigEntries) != len(resp.ConfigEntries) {
		t.Errorf("request config %v, response config %v", derefAll(req.ConfigEntries), derefAll(resp.ConfigEntries))
	}
	for k, v := range resp.ConfigEntries {
		if rv := req.ConfigEntries[k]; rv == nil || *rv != *v {
			t.Errorf("pii.scan.request %s = %v, pii.scan.response has %s", k, rv, *v)
		}
	}
}

// failingOnAdmin refuses CreateTopic for the named topics only, so a test can see
// whether a failure on one topic stops the others.
type failingOnAdmin struct {
	*fakeAdmin
	failOn   map[string]bool
	attempts []string
}

func (f *failingOnAdmin) CreateTopic(name string, d *sarama.TopicDetail, v bool) error {
	f.attempts = append(f.attempts, name)
	if f.failOn[name] {
		return errors.New("broker refused " + name)
	}
	return f.fakeAdmin.CreateTopic(name, d, v)
}

// TestEnsurePlatformTopicsContinuesPastAFailedCreate: one topic the broker refuses
// (an ACL on that name, say) must not leave every topic after it uncreated. Every
// topic is attempted, and each failure comes back in the joined error.
func TestEnsurePlatformTopicsContinuesPastAFailedCreate(t *testing.T) {
	t.Setenv("RSYNC_SCHEMA_DRIFT_ENABLED", "true")
	tm, base := newFakeManager(1)
	first := kafkaclient.Topic("pipeline.domain.events")
	middle := kafkaclient.Topic("pii.scan.request")
	admin := &failingOnAdmin{fakeAdmin: base, failOn: map[string]bool{first: true, middle: true}}
	tm.admin = admin

	err := tm.EnsurePlatformTopics(context.Background())
	if err == nil {
		t.Fatal("EnsurePlatformTopics returned nil although two creates failed")
	}
	for _, name := range []string{first, middle} {
		if !strings.Contains(err.Error(), name) {
			t.Errorf("joined error %q does not name the failed topic %s", err, name)
		}
	}

	names := PlatformTopicNames()
	if len(admin.attempts) != len(names) {
		t.Fatalf("attempted %d creates %v, want all %d platform topics: a failure stopped "+
			"the rest", len(admin.attempts), admin.attempts, len(names))
	}
	for _, name := range names {
		q := kafkaclient.Topic(name)
		if q == first || q == middle {
			continue
		}
		if _, ok := base.created[q]; !ok {
			t.Errorf("%s was not created after an earlier topic failed", q)
		}
	}
}

func derefAll(m map[string]*string) map[string]string {
	out := map[string]string{}
	for k, v := range m {
		if v != nil {
			out[k] = *v
		}
	}
	return out
}

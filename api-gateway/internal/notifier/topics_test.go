package notifier

import (
	"reflect"
	"strings"
	"testing"
)

// The notifier's sarama consumer group auto-creates every topic it subscribes
// to, so its subscription list decides which topics exist on the broker as much
// as any topic creator does.
//
//   - rsync.notifications is always consumed: it is a platform topic and the
//     delivery path for every Slack and email alert.
//   - rsync.healer.results is one of the three schema-drift topics, created and
//     produced by the orchestrator only when RSYNC_SCHEMA_DRIFT_ENABLED=true, so
//     the notifier subscribes to it only then. The flag is "true" or nothing,
//     the same test the orchestrator applies.
//   - rsync.healer.actions was removed from the platform; nothing subscribes.
func TestNotifierTopicsFollowTheSchemaDriftFlag(t *testing.T) {
	cases := []struct {
		name   string
		prefix string
		drift  string
		want   []string
	}{
		{"drift unset", "rsync.", "", []string{"rsync.notifications"}},
		{"drift false", "rsync.", "false", []string{"rsync.notifications"}},
		{"drift is not exactly true", "rsync.", "1", []string{"rsync.notifications"}},
		{"drift on", "rsync.", "true", []string{"rsync.notifications", "rsync.healer.results"}},
		{"drift on, operator prefix", "acme.", "true", []string{"acme.rsync.notifications", "acme.rsync.healer.results"}},
		{"drift off, operator prefix", "acme.", "", []string{"acme.rsync.notifications"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("KAFKA_TOPIC_PREFIX", tc.prefix)
			t.Setenv("RSYNC_SCHEMA_DRIFT_ENABLED", tc.drift)

			topics := resolveNotifierTopics()
			got := topics.all()
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("resolveNotifierTopics().all() = %v, want %v", got, tc.want)
			}
			for _, topic := range got {
				if strings.Contains(topic, "healer.actions") {
					t.Errorf("the notifier still subscribes to the removed topic %q", topic)
				}
			}
			// The router compares against this field; it must be empty exactly
			// when the subscription is absent, or handleMessage could route a
			// notification through the healing-result normalizer.
			if wantResults := tc.drift == "true"; (topics.healerResults != "") != wantResults {
				t.Errorf("healerResults = %q with RSYNC_SCHEMA_DRIFT_ENABLED=%q", topics.healerResults, tc.drift)
			}
		})
	}
}

// An emitter never consumes, but it resolves the same topics so Publish lands on
// the notification route. It must not carry a healer route with drift off.
func TestNewEmitterResolvesTopicsUnderTheFlag(t *testing.T) {
	t.Setenv("KAFKA_TOPIC_PREFIX", "rsync.")
	t.Setenv("RSYNC_SCHEMA_DRIFT_ENABLED", "")

	e := NewEmitter(nil)
	if e.topics.notify != "rsync.notifications" {
		t.Errorf("emitter notify topic = %q, want rsync.notifications", e.topics.notify)
	}
	if e.topics.healerResults != "" {
		t.Errorf("emitter healerResults = %q with schema drift off, want empty", e.topics.healerResults)
	}
}

// The copy catalog keeps a headline for every topic the notifier can consume,
// and only those. rsync.healer.actions no longer exists, so a headline for it is
// dead copy.
func TestTopicDefaultsCoverOnlyLiveTopics(t *testing.T) {
	for topic := range topicDefaults {
		if strings.Contains(topic, "healer.actions") {
			t.Errorf("topicDefaults still has an entry for the removed topic %q", topic)
		}
	}
	if _, ok := topicDefaults[healerResults]; !ok {
		t.Errorf("topicDefaults lost its %q headline; stored rows from that topic would render the topic name", healerResults)
	}
}

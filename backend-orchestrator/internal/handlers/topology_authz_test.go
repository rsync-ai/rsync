package handlers

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
)

// TestIsPlatformOwnedTopic pins the namespace boundary for a CURRENT deployment
// — one that mints every topic through kafkaclient.Topic(), so the wire names
// carry the "rsync." prefix. The "foreign" cases are the ones that matter: on a
// customer's shared broker these are somebody else's topics, and before the
// guard existed DELETE /topics/:name passed them straight through to the Kafka
// admin client.
func TestIsPlatformOwnedTopic(t *testing.T) {
	t.Setenv("KAFKA_TOPIC_PREFIX", "rsync.")
	t.Setenv("KAFKA_OWNED_TOPIC_PREFIXES", "")
	t.Setenv(envAllowLegacyUnprefixedTopics, "")

	owned := []string{
		"rsync.agent.control.commands.executor",
		"rsync.agent.control.results",
		"rsync.agent.failed.dlq",
		"rsync.pipeline.abd8a64d.data",
		"rsync.pipeline.abd8a64d.data.dlq",
		"rsync.cdc.abd8a64d",
		"rsync.cdc-abd8a64d",
		"rsync.cdc-abd8a64d.inventory.orders",
		"rsync.cdc-abd8a64d.inventory.orders.dlq",
		"rsync.schemahistory.cdc-abd8a64d",
		"rsync.pii.scan.request",
		"rsync.signals.abd8a64d",
		// Branded: owned whatever the configured prefix is.
		"_rsync-connect-offsets",
	}
	for _, name := range owned {
		if !isPlatformOwnedTopic(name) {
			t.Errorf("expected platform-owned, got foreign: %q", name)
		}
	}

	foreign := []string{
		"",
		"   ",
		"orders",
		"payments.transactions",
		"__consumer_offsets",
		"_schemas",
		"customer.billing.events",
		"connect-configs",
		"my-cdc-topic",       // "cdc-" not at the start
		"prod.agent.control", // "agent." not at the start

		// The point of narrowing the allowlist: on a BYO cluster these bare
		// namespaces are generic enough to be another team's topics, and they
		// are no longer inside rsync-ai's delete blast radius by default.
		"task.orders.created",
		"pipeline.abd8a64d.data",
		"agent.control.results",
		"cdc.abd8a64d",
		"cdc-abd8a64d",
		"schemahistory.cdc-abd8a64d",
		"pii.scan.request",
	}
	for _, name := range foreign {
		if isPlatformOwnedTopic(name) {
			t.Errorf("expected foreign, got platform-owned: %q", name)
		}
	}
}

// TestLegacyBarePrefixesNeedAnExplicitWindow pins the two — and only two —
// conditions under which the pre-namespace bare names are back in the
// allowlist. Silently keeping them is what put a BYO customer's own `task.`
// topic inside our delete radius.
func TestLegacyBarePrefixesNeedAnExplicitWindow(t *testing.T) {
	t.Run("empty prefix: the bare names ARE the platform's names", func(t *testing.T) {
		// A deployment that opts out of namespacing has no other name for its
		// own topics; dropping them here would strand the platform outside its
		// own allowlist.
		t.Setenv("KAFKA_TOPIC_PREFIX", "")
		t.Setenv("KAFKA_OWNED_TOPIC_PREFIXES", "")
		t.Setenv(envAllowLegacyUnprefixedTopics, "")

		for _, name := range []string{"pipeline.abd8a64d.data", "cdc-abd8a64d", "agent.control.results"} {
			if !isPlatformOwnedTopic(name) {
				t.Errorf("un-namespaced deployment lost its own topic: %q", name)
			}
		}
		if isPlatformOwnedTopic("payments.transactions") {
			t.Error("the empty-prefix case widened the allowlist to an unrelated topic")
		}
	})

	t.Run("explicit opt-out re-admits them during migration", func(t *testing.T) {
		t.Setenv("KAFKA_TOPIC_PREFIX", "rsync.")
		t.Setenv("KAFKA_OWNED_TOPIC_PREFIXES", "")
		t.Setenv(envAllowLegacyUnprefixedTopics, "true")

		if !isPlatformOwnedTopic("pipeline.abd8a64d.data") {
			t.Error("the documented migration window did not re-admit a legacy topic")
		}
		if !isPlatformOwnedTopic("rsync.pipeline.abd8a64d.data") {
			t.Error("the migration window dropped the namespaced name")
		}
	})

	t.Run("only an explicitly truthy value opens the window", func(t *testing.T) {
		t.Setenv("KAFKA_TOPIC_PREFIX", "rsync.")
		t.Setenv("KAFKA_OWNED_TOPIC_PREFIXES", "")
		for _, v := range []string{"", "0", "false", "no", "off", "maybe", " "} {
			t.Setenv(envAllowLegacyUnprefixedTopics, v)
			if isPlatformOwnedTopic("task.orders.created") {
				t.Errorf("%s=%q opened the legacy window", envAllowLegacyUnprefixedTopics, v)
			}
		}
		for _, v := range []string{"1", "true", "TRUE", " yes ", "on"} {
			t.Setenv(envAllowLegacyUnprefixedTopics, v)
			if !isPlatformOwnedTopic("task.orders.created") {
				t.Errorf("%s=%q did not open the legacy window", envAllowLegacyUnprefixedTopics, v)
			}
		}
	})
}

// TestOwnedTopicPrefixesEnvExtendsNeverReplaces proves the override widens the
// allowlist without dropping a built-in — a deployment that sets its own prefix
// must not lose the ability to manage its namespaced topics.
func TestOwnedTopicPrefixesEnvExtendsNeverReplaces(t *testing.T) {
	t.Setenv("KAFKA_TOPIC_PREFIX", "rsync.")
	t.Setenv(envAllowLegacyUnprefixedTopics, "")
	t.Setenv("KAFKA_OWNED_TOPIC_PREFIXES", "acme-rsync., other.ns.")

	if !isPlatformOwnedTopic("acme-rsync.orders") {
		t.Error("env-declared prefix was not honored")
	}
	if !isPlatformOwnedTopic("other.ns.orders") {
		t.Error("second env-declared prefix was not honored (comma split/trim)")
	}
	if !isPlatformOwnedTopic("rsync.agent.control.results") {
		t.Error("built-in prefix was lost when the env override was set")
	}
	if !isPlatformOwnedTopic("_rsync-connect-offsets") {
		t.Error("branded prefix was lost when the env override was set")
	}
	if isPlatformOwnedTopic("payments.transactions") {
		t.Error("env override widened the allowlist to an unrelated topic")
	}
}

// TestForeignTopicMessageAdvertisesOnlyTheEffectiveAllowlist keeps the 403 body
// honest: an error naming prefixes the guard does not actually accept sends an
// operator chasing a permission problem that is really a naming problem.
func TestForeignTopicMessageAdvertisesOnlyTheEffectiveAllowlist(t *testing.T) {
	gin.SetMode(gin.TestMode)
	t.Setenv("KAFKA_TOPIC_PREFIX", "rsync.")
	t.Setenv("KAFKA_OWNED_TOPIC_PREFIXES", "")
	t.Setenv(envAllowLegacyUnprefixedTopics, "")

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodDelete, "/api/v1/topology/topics/task.orders", nil)

	if !rejectForeignTopic(c, "task.orders") {
		t.Fatal("a bare task. topic was accepted under a namespaced deployment")
	}
	body := w.Body.String()
	if !strings.Contains(body, "rsync.") {
		t.Errorf("message does not advertise the configured prefix: %s", body)
	}
	if strings.Contains(body, "task.") || strings.Contains(body, "pipeline.") {
		t.Errorf("message advertises a legacy prefix the guard rejects: %s", body)
	}
}

// TestDestructiveTopologyRoutesRejectForeignTopics drives the real handlers
// through a real gin router. It asserts 403 *and* that the Kafka manager was
// never reached: the handler holds a nil *kafka.TopologyManager, so any call
// that got past the guard would panic instead of returning a status.
func TestDestructiveTopologyRoutesRejectForeignTopics(t *testing.T) {
	gin.SetMode(gin.TestMode)
	t.Setenv("KAFKA_TOPIC_PREFIX", "rsync.")
	t.Setenv("KAFKA_OWNED_TOPIC_PREFIXES", "")
	t.Setenv(envAllowLegacyUnprefixedTopics, "")

	h := NewTopologyHandler(nil, nil) // nil manager: reaching it panics, which is the point
	r := gin.New()
	h.RegisterRoutes(r.Group("/api/v1/topology"))

	cases := []struct {
		name   string
		method string
		path   string
		body   string
	}{
		{"delete foreign topic", http.MethodDelete, "/api/v1/topology/topics/payments.transactions", ""},
		{"delete kafka internal", http.MethodDelete, "/api/v1/topology/topics/__consumer_offsets", ""},
		{"repartition foreign topic", http.MethodPut, "/api/v1/topology/topics/payments.transactions/partitions", `{"partitions":64}`},
		{"create foreign topic", http.MethodPost, "/api/v1/topology/topics", `{"topic_name":"payments.transactions","partitions":3,"replication_factor":1}`},
		// GET /topics/:name consulted no allowlist at all before this change.
		{"read foreign topic", http.MethodGet, "/api/v1/topology/topics/payments.transactions", ""},
		// A BYO customer's own topic under a bare pre-namespace name.
		{"delete a customer's task. topic", http.MethodDelete, "/api/v1/topology/topics/task.orders.created", ""},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var body *strings.Reader
			if tc.body == "" {
				body = strings.NewReader("")
			} else {
				body = strings.NewReader(tc.body)
			}
			req := httptest.NewRequest(tc.method, tc.path, body)
			req.Header.Set("Content-Type", "application/json")
			w := httptest.NewRecorder()
			r.ServeHTTP(w, req)

			if w.Code != http.StatusForbidden {
				t.Fatalf("%s %s: got %d, want 403", tc.method, tc.path, w.Code)
			}
			if !strings.Contains(w.Body.String(), "outside the rsync-ai namespace") {
				t.Errorf("unexpected body: %s", w.Body.String())
			}
		})
	}
}

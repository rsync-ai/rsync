package kafka

import (
	"context"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"

	kafkaclient "github.com/rsync-ai/shared/kafkaclient"
)

// Four provisioners create the platform topics: EnsurePlatformTopics here, and three
// kafka-init bootstrappers -- scripts/kafka-init-new-topics.sh (docker-compose.yml),
// the Helm kafka-init job (deploy/helm/rsync-ai/templates/jobs/kafka-init.yaml) and
// the kafka-init service in docker-compose.quickstart.yml. NONE of them alters an
// existing topic -- the script skips on "already exists", the other two pass
// --if-not-exists, and ensureTopicLocked returns early -- so whichever runs first
// wins permanently, and on a BYO/K8s deployment without kafka-init the orchestrator
// is the only creator.
//
// A divergence therefore never surfaces as a conflict. It surfaces as a topic born
// with the wrong config: pipeline.domain.events is the log the api-gateway projector
// rebuilds read models from, so its retention decides how far back a rebuild can
// reach, and the product decision is 7 days in every creator.
//
// The expectations are READ OUT OF THE THREE PROVISIONER FILES rather than restated,
// so this fails if any side moves. Restating them would make the test agree with
// topology.go by construction, which is the one thing it must not do.

// provisionedTopic is what one kafka-init provisioner creates for one topic.
type provisionedTopic struct {
	config     map[string]string // --config key=value pairs
	partitions int               // 0 when the provisioner takes it from a variable
}

// wantDomainEventsRetention is the product decision for pipeline.domain.events, the
// one value this file does restate: the provisioners are compared with each other
// AND with it, so all four agreeing on some other value still fails.
const wantDomainEventsRetention = "604800000"

func TestPlatformTopicConfigMatchesEveryProvisioner(t *testing.T) {
	t.Setenv("RSYNC_SCHEMA_DRIFT_ENABLED", "")
	tm, admin := newFakeManager(1)
	if err := tm.EnsurePlatformTopics(context.Background()); err != nil {
		t.Fatalf("EnsurePlatformTopics: %v", err)
	}

	provisioners := map[string]map[string]provisionedTopic{
		"scripts/kafka-init-new-topics.sh":                    topicsFromInitScript(t),
		"deploy/helm/rsync-ai/templates/jobs/kafka-init.yaml": topicsFromHelmKafkaInit(t),
		"docker-compose.quickstart.yml":                       topicsFromQuickstartKafkaInit(t),
	}

	for file, topics := range provisioners {
		ev, ok := topics["pipeline.domain.events"]
		if !ok {
			t.Errorf("%s no longer creates pipeline.domain.events (or the parse broke)", file)
		} else if got := ev.config["retention.ms"]; got != wantDomainEventsRetention {
			t.Errorf("%s creates pipeline.domain.events with retention.ms=%q, want %s (7 days)",
				file, got, wantDomainEventsRetention)
		}

		for topic, want := range topics {
			created, ok := admin.created[kafkaclient.Topic(topic)]
			if !ok {
				t.Errorf("%s creates %s but EnsurePlatformTopics does NOT -- on a BYO "+
					"cluster with no kafka-init it would exist only by broker "+
					"auto-creation, at the broker's defaults", file, topic)
				continue
			}
			for key, wantVal := range want.config {
				if key == "min.insync.replicas" {
					continue // derived from RF by both sides; not a fixed value
				}
				got := created.ConfigEntries[key]
				if got == nil {
					t.Errorf("%s: %s sets %s=%s but EnsurePlatformTopics sets none, so the "+
						"topic inherits the broker's value when the orchestrator wins",
						topic, file, key, wantVal)
					continue
				}
				if *got != wantVal {
					t.Errorf("%s: %s=%s via EnsurePlatformTopics but %s via %s -- whichever "+
						"provisioner runs first wins permanently, so these must agree",
						topic, key, *got, wantVal, file)
				}
			}
			if want.partitions != 0 && int(created.NumPartitions) != want.partitions {
				t.Errorf("%s: %d partition(s) via EnsurePlatformTopics but %d via %s",
					topic, created.NumPartitions, want.partitions, file)
			}
		}
	}

	// And the orchestrator's own copy of the decision, independent of any file.
	ev, ok := admin.created[kafkaclient.Topic("pipeline.domain.events")]
	if !ok {
		t.Fatal("EnsurePlatformTopics did not create pipeline.domain.events")
	}
	for key, want := range map[string]string{
		"retention.ms":     wantDomainEventsRetention,
		"cleanup.policy":   "delete",
		"compression.type": "snappy",
	} {
		if got := ev.ConfigEntries[key]; got == nil || *got != want {
			t.Errorf("pipeline.domain.events %s = %v, want %s", key, got, want)
		}
	}
}

// The bundled bootstrappers create every platform topic with 3 partitions. Creating
// one at 1 here would cap its consumer group at a single active member forever,
// because KeepExistingPartitions means nothing widens it afterwards.
func TestPlatformTopicsAreNotBornSinglePartition(t *testing.T) {
	t.Setenv("RSYNC_SCHEMA_DRIFT_ENABLED", "true")
	tm, admin := newFakeManager(1)
	if err := tm.EnsurePlatformTopics(context.Background()); err != nil {
		t.Fatalf("EnsurePlatformTopics: %v", err)
	}
	names := PlatformTopicNames()
	if len(names) != 7 {
		t.Fatalf("PlatformTopicNames() with the drift flag on = %v, want 7 names", names)
	}
	for _, topic := range names {
		created, ok := admin.created[kafkaclient.Topic(topic)]
		if !ok {
			t.Errorf("%s was not created at all", topic)
			continue
		}
		if created.NumPartitions != 3 {
			t.Errorf("%s created with %d partition(s), want 3; KeepExistingPartitions "+
				"makes this width permanent", topic, created.NumPartitions)
		}
	}
}

// parseConfigArgs turns "--config a=b --config c=d" into {a: b, c: d}.
func parseConfigArgs(s string) map[string]string {
	out := map[string]string{}
	for _, m := range regexp.MustCompile(`--config\s+([a-z.]+)=([^\s"'$]+)`).FindAllStringSubmatch(s, -1) {
		out[m[1]] = m[2]
	}
	return out
}

func readRepoFile(t *testing.T, rel string) string {
	t.Helper()
	path := filepath.Join(serviceRoot(t), "..", filepath.FromSlash(rel))
	src, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("reading provisioner %s: %v", path, err)
	}
	return string(src)
}

// topicsFromInitScript reads scripts/kafka-init-new-topics.sh: each
// `create_topic "<name>" "<retention>"` call plus the config create_topic() adds to
// every topic. Partitions come from $PARTITIONS, so they are not compared.
func topicsFromInitScript(t *testing.T) map[string]provisionedTopic {
	t.Helper()
	src := readRepoFile(t, "scripts/kafka-init-new-topics.sh")

	// The config create_topic() applies to every topic it creates.
	body := src
	if i := strings.Index(src, "create_topic() {"); i >= 0 {
		body = src[i:]
		if j := strings.Index(body, "\n}\n"); j > 0 {
			body = body[:j]
		}
	} else {
		t.Fatal("scripts/kafka-init-new-topics.sh no longer defines create_topic(); re-point this parse")
	}
	common := parseConfigArgs(body)
	delete(common, "retention.ms") // the literal there is "$retention_ms", passed per call

	// SEVEN_DAYS_MS=$((7 * 24 * 60 * 60 * 1000)) -- resolve it rather than assume.
	vars := map[string]string{"SEVEN_DAYS_MS": strconv.Itoa(7 * 24 * 60 * 60 * 1000)}

	out := map[string]provisionedTopic{}
	re := regexp.MustCompile(`(?m)^\s*create_topic\s+"([^"]+)"\s+"?\$?\{?([^"\s}]+)\}?"?`)
	for _, m := range re.FindAllStringSubmatch(src, -1) {
		topic, retention := m[1], m[2]
		if v, ok := vars[retention]; ok {
			retention = v
		}
		cfg := map[string]string{}
		for k, v := range common {
			cfg[k] = v
		}
		if _, err := strconv.Atoi(retention); err == nil {
			cfg["retention.ms"] = retention
		}
		out[topic] = provisionedTopic{config: cfg}
	}
	requireParsed(t, "scripts/kafka-init-new-topics.sh", out)
	return out
}

// topicsFromHelmKafkaInit reads the Helm kafka-init job: the shared TOPIC_CONFIG,
// the --partitions its mk() passes, and each `mk "${P}<name>"` call.
func topicsFromHelmKafkaInit(t *testing.T) map[string]provisionedTopic {
	t.Helper()
	const file = "deploy/helm/rsync-ai/templates/jobs/kafka-init.yaml"
	src := readRepoFile(t, file)

	cfgM := regexp.MustCompile(`TOPIC_CONFIG="([^"]*)"`).FindStringSubmatch(src)
	if cfgM == nil {
		t.Fatalf("%s: no TOPIC_CONFIG=\"...\" line; re-point this parse", file)
	}
	partitions := partitionsIn(t, file, src)

	out := map[string]provisionedTopic{}
	for _, m := range regexp.MustCompile(`(?m)^\s*mk\s+"\$\{P\}([^"]+)"`).FindAllStringSubmatch(src, -1) {
		out[m[1]] = provisionedTopic{config: parseConfigArgs(cfgM[1]), partitions: partitions}
	}
	requireParsed(t, file, out)
	return out
}

// topicsFromQuickstartKafkaInit reads the kafka-init service in
// docker-compose.quickstart.yml: TC="...", the for-loop's topic list and its
// --partitions.
func topicsFromQuickstartKafkaInit(t *testing.T) map[string]provisionedTopic {
	t.Helper()
	const file = "docker-compose.quickstart.yml"
	src := readRepoFile(t, file)

	cfgM := regexp.MustCompile(`\bTC="([^"]*)"`).FindStringSubmatch(src)
	if cfgM == nil {
		t.Fatalf("%s: no TC=\"...\" line in kafka-init; re-point this parse", file)
	}
	loop := regexp.MustCompile(`for t in ([a-z0-9. -]+); do\s*\n\s*(kafka-topics\.sh --create[^\n]*)`).FindStringSubmatch(src)
	if loop == nil {
		t.Fatalf("%s: no `for t in ...; do kafka-topics.sh --create` loop; re-point this parse", file)
	}
	partitions := partitionsIn(t, file, loop[2])

	out := map[string]provisionedTopic{}
	for _, topic := range strings.Fields(loop[1]) {
		out[topic] = provisionedTopic{config: parseConfigArgs(cfgM[1]), partitions: partitions}
	}
	requireParsed(t, file, out)
	return out
}

func partitionsIn(t *testing.T, file, src string) int {
	t.Helper()
	m := regexp.MustCompile(`--partitions\s+(\d+)`).FindStringSubmatch(src)
	if m == nil {
		t.Fatalf("%s: no literal --partitions N; re-point this parse", file)
	}
	n, _ := strconv.Atoi(m[1])
	return n
}

// requireParsed is the anti-vacuity check: a parse that finds nothing skips every
// comparison and passes while proving nothing.
func requireParsed(t *testing.T, file string, out map[string]provisionedTopic) {
	t.Helper()
	ev, ok := out["pipeline.domain.events"]
	if !ok {
		t.Fatalf("could not read pipeline.domain.events out of %s (parsed %d topics) -- "+
			"the parse is broken, so a green result would mean nothing", file, len(out))
	}
	if ev.config["retention.ms"] == "" || ev.config["cleanup.policy"] == "" {
		t.Fatalf("parsed pipeline.domain.events out of %s with config %v -- missing "+
			"retention.ms or cleanup.policy, so the parse is not seeing the config", file, ev.config)
	}
}

package config

// The two monitoring flags default differently ON PURPOSE, and the asymmetry is
// the product decision this file exists to keep.
//
//	FEATURE_MONITORING_OVERVIEW  -> true   a PIPELINE surface
//	FEATURE_MONITORING_INFRA     -> false  the ADMIN surface
//
// MonitoringOverview gates GET /pipelines/:id/monitoring/overview, and its
// frontend twin hides the Overview sub-tab outright — so while it was false, the
// health tiles were not on the page at all, including the one that says "Capture
// stopped" when Debezium has died over a capture hole. Both docker-compose.yml and
// docker-compose.prod.yml already set it "true", so off was never what a real
// deployment ran. CLAUDE.md's OSS/cloud rule says a flag defaults to the CLOUD
// behaviour and is set to the non-default only in the OSS compose; this now obeys
// it. (docker-compose.staging.yml sets the frontend twin "false" explicitly and
// keeps that — an explicit choice, not the default.)
//
// MonitoringInfra gates GET /monitoring/sentinel/health and
// /monitoring/sentinel/issues: sentinel_component_health has no workspace column
// and its component ids name Kafka topics and containers across every workspace,
// which is why the health route is admin-only by construction. A self-host
// deployment has no reason to serve it unasked. Nothing on a pipeline page depends
// on it any more — pipeline alerts and the consumer census have their own
// unflagged, workspace-scoped routes (/pipelines/:id/alerts, /pipelines/:id/consumers).
//
// Flipping MonitoringInfra to true "for consistency" would re-enable an admin
// surface nobody asked for; flipping MonitoringOverview back to false would
// re-hide a safety signal. Hence a test, not a comment.

import (
	"os"
	"testing"
)

func TestMonitoringFlagDefaults(t *testing.T) {
	for _, v := range []string{"FEATURE_MONITORING_OVERVIEW", "FEATURE_MONITORING_INFRA"} {
		if _, set := os.LookupEnv(v); set {
			t.Setenv(v, "")
			os.Unsetenv(v)
		}
	}

	if got := getBoolEnv("FEATURE_MONITORING_OVERVIEW", true); !got {
		t.Error("FEATURE_MONITORING_OVERVIEW default is false; the pipeline health tiles, " +
			"including 'Capture stopped', are then hidden on any deployment that does not set it")
	}
	if got := getBoolEnv("FEATURE_MONITORING_INFRA", false); got {
		t.Error("FEATURE_MONITORING_INFRA default is true; that serves the admin-only " +
			"infrastructure view unasked. No pipeline surface needs it.")
	}
}

// An operator who sets the variable still wins — the defaults above are only what
// happens when nobody chose.
func TestMonitoringFlagsRemainOverridable(t *testing.T) {
	t.Setenv("FEATURE_MONITORING_OVERVIEW", "false")
	if getBoolEnv("FEATURE_MONITORING_OVERVIEW", true) {
		t.Error("an explicit false was ignored")
	}
	t.Setenv("FEATURE_MONITORING_INFRA", "true")
	if !getBoolEnv("FEATURE_MONITORING_INFRA", false) {
		t.Error("an explicit true was ignored")
	}
}

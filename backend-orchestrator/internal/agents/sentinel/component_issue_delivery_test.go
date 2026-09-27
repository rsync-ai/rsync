package sentinel

import (
	"context"
	"testing"
	"time"
)

// These tests cover KI-COMPONENT-HEALTH-ALERTS-NOBODY's first half: the detector
// never SAW the components the HealthMonitor polls, so no amount of publishing
// downstream would have produced an alert for a down connector, a dead queue
// reader or a failed infrastructure component.
//
// The monitor's polling loops write mcp_connector:*, kafka_consumer:* and
// infrastructure:* into their own map. The detector used to read only a second map,
// filled from agent heartbeats that nothing published; that map is gone and the
// monitor's view is the whole snapshot.

func TestTheDetectorSeesTheComponentsTheMonitorPolls(t *testing.T) {
	monitor := &HealthMonitor{
		componentHealth: map[string]*ComponentHealth{
			"infrastructure:postgres": {ComponentID: "infrastructure:postgres", ComponentType: ComponentTypeInfrastructure, Status: HealthStatusUnhealthy},
			"infrastructure:redis":    {ComponentID: "infrastructure:redis", ComponentType: ComponentTypeInfrastructure, Status: HealthStatusHealthy},
			"rsync.cdc-orders":        {ComponentID: "rsync.cdc-orders", ComponentType: ComponentTypeKafkaConsumer, Status: HealthStatusHealthy},
		},
	}
	agent := &Agent{healthMonitor: monitor}

	got := map[string]ComponentType{}
	for _, c := range agent.snapshotComponents() {
		got[c.ComponentID] = c.ComponentType
	}
	for id, want := range map[string]ComponentType{
		"infrastructure:postgres": ComponentTypeInfrastructure,
		"infrastructure:redis":    ComponentTypeInfrastructure,
		"rsync.cdc-orders":        ComponentTypeKafkaConsumer,
	} {
		if got[id] != want {
			t.Errorf("component %s is not visible to the detector (got type %q); "+
				"faults on it can never be detected, let alone alerted", id, got[id])
		}
	}
	if len(got) != 3 {
		t.Errorf("snapshot has %d components, want 3: %v", len(got), got)
	}
}

// The snapshot must be a COPY. detectIssues reads these structs while the monitor's
// poll loops keep writing to them.
func TestTheSnapshotDoesNotAliasTheLiveComponents(t *testing.T) {
	live := &ComponentHealth{
		ComponentID: "infrastructure:kafka", ComponentType: ComponentTypeInfrastructure,
		Status: HealthStatusHealthy, Metadata: map[string]interface{}{"brokers": 1},
	}
	agent := &Agent{healthMonitor: &HealthMonitor{componentHealth: map[string]*ComponentHealth{live.ComponentID: live}}}

	snap := agent.snapshotComponents()
	if len(snap) != 1 {
		t.Fatalf("snapshot len = %d", len(snap))
	}
	if snap[0] == live {
		t.Fatal("snapshot returned the live pointer; the detector would race the poll loops")
	}
	snap[0].Status = HealthStatusDead
	snap[0].Metadata["brokers"] = 0
	if live.Status != HealthStatusHealthy {
		t.Error("mutating the snapshot changed the live component's status")
	}
	if live.Metadata["brokers"] != 1 {
		t.Error("the metadata map is shared with the live component")
	}
}

func TestSnapshotComponentsToleratesNoMonitor(t *testing.T) {
	agent := &Agent{}
	if n := len(agent.snapshotComponents()); n != 0 {
		t.Fatalf("got %d components with a nil healthMonitor, want 0", n)
	}
}

// detectorFor builds a detector whose cooldown cannot silently swallow the second
// issue in a table-driven test: each subtest gets a fresh recentIssues map.
func detectorFor(t *testing.T) *IssueDetector {
	t.Helper()
	cfg := DefaultSentinelConfig()
	return NewIssueDetector(cfg, nil)
}

// TestOneComponentFaultProducesOneIssue is the second half of the delivery fix.
//
// Publishing every detected issue means overlapping detectors become duplicate
// pages. A consumer group that has closed is written unhealthy AND tagged
// issue_type "consumer_group_closed", so the specific detector and the unhealthy
// catch-all both fire — one dead reader, two criticals, two dedup keys.
func TestOneComponentFaultProducesOneIssue(t *testing.T) {
	cases := []struct {
		name      string
		component *ComponentHealth
		want      []IssueType
	}{
		{
			name: "a closed consumer group is one issue, not two",
			component: &ComponentHealth{
				ComponentID: "rsync.cdc-orders", ComponentType: ComponentTypeKafkaConsumer,
				Status:    HealthStatusUnhealthy,
				LastError: "Consumer group closed",
				Metadata:  map[string]interface{}{"issue_type": "consumer_group_closed"},
			},
			want: []IssueType{IssueTypeConsumerGroupClosed},
		},
		{
			// The catch-all must stay reachable: this is the case the suppression
			// above could have silently swallowed.
			name: "a consumer unhealthy for another reason still reaches the catch-all",
			component: &ComponentHealth{
				ComponentID: "rsync.cdc-invoices", ComponentType: ComponentTypeKafkaConsumer,
				Status:    HealthStatusUnhealthy,
				LastError: "dial tcp 10.0.0.4:9092: connect: connection refused",
			},
			want: []IssueType{IssueTypeMissingHeartbeat},
		},
		{
			name: "infrastructure down is critical and detected",
			component: &ComponentHealth{
				ComponentID: "infrastructure:postgres", ComponentType: ComponentTypeInfrastructure,
				Status: HealthStatusUnhealthy, LastError: "dial error",
			},
			want: []IssueType{IssueTypeInfrastructureDown},
		},
		{
			name: "a healthy component produces nothing",
			component: &ComponentHealth{
				ComponentID: "infrastructure:kafka", ComponentType: ComponentTypeInfrastructure,
				Status: HealthStatusHealthy, LastHeartbeat: time.Now(),
			},
			want: nil,
		},
		{
			name: "an undeployed connector is not in the map at all, but a failing one is a warning",
			component: &ComponentHealth{
				ComponentID: "mcp_connector:rsync-ai-postgresql-v1-0-0-mcp", ComponentType: ComponentTypeMCPConnector,
				Status: HealthStatusUnhealthy,
			},
			want: []IssueType{IssueTypeConnectorDown},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			issues := detectorFor(t).DetectIssues(context.Background(), []*ComponentHealth{c.component})
			got := make([]IssueType, 0, len(issues))
			for _, i := range issues {
				got = append(got, i.Type)
			}
			if len(got) != len(c.want) {
				t.Fatalf("got %d issues %v, want %d %v", len(got), got, len(c.want), c.want)
			}
			for i := range got {
				if got[i] != c.want[i] {
					t.Errorf("issue %d = %q, want %q", i, got[i], c.want[i])
				}
			}
		})
	}
}

// The severity is what decides whether an admin is paged, so pin it rather than
// leaving it to the delivery matrix in notify.go to imply.
func TestComponentIssueSeveritiesMatchTheDeliveryMatrix(t *testing.T) {
	cases := []struct {
		component *ComponentHealth
		want      IssueSeverity
		why       string
	}{
		{
			component: &ComponentHealth{ComponentID: "infrastructure:postgres", ComponentType: ComponentTypeInfrastructure, Status: HealthStatusUnhealthy},
			want:      IssueSeverityCritical,
			why:       "no pipeline can move data without it",
		},
		{
			component: &ComponentHealth{ComponentID: "mcp_connector:x", ComponentType: ComponentTypeMCPConnector, Status: HealthStatusUnhealthy},
			want:      IssueSeverityWarning,
			why:       "a connector a pipeline actually uses is paged by the pipeline lane (CDC_CONNECTOR_DOWN)",
		},
		{
			component: &ComponentHealth{ComponentID: "mcp_connector:x", ComponentType: ComponentTypeMCPConnector, Status: HealthStatusDead},
			want:      IssueSeverityCritical,
			why:       "dead is dead regardless of type",
		},
	}
	for _, c := range cases {
		issues := detectorFor(t).DetectIssues(context.Background(), []*ComponentHealth{c.component})
		if len(issues) != 1 {
			t.Fatalf("%s: got %d issues, want 1", c.component.ComponentID, len(issues))
		}
		if issues[0].Severity != c.want {
			t.Errorf("%s (%s): severity = %q, want %q — %s",
				c.component.ComponentID, c.component.Status, issues[0].Severity, c.want, c.why)
		}
	}
}

package mcp

import (
	"testing"
)

// A container that is up but was never started by this process (orchestrator
// restarted after the CDC stream began) must be found — and registered — without
// deploying anything. Before FindRunningServer the dependency probe used GetServer
// and showed a healthy kafka-mcp-sink as "not registered".
func TestFindRunningServer_DiscoversUnregisteredContainer(t *testing.T) {
	var asked []string
	orig := checkDockerContainerFn
	t.Cleanup(func() { checkDockerContainerFn = orig })
	checkDockerContainerFn = func(_ *ServerManager, containerName, connectorName string) *ServerInfo {
		asked = append(asked, containerName)
		if containerName == StackPrefix()+"-kafka-mcp-sink-v1-0-0-mcp" {
			return &ServerInfo{Name: connectorName, Status: "running", ConnType: "http", Host: containerName, Port: 8000}
		}
		return nil
	}

	sm := &ServerManager{servers: map[string]*ServerInfo{}}
	if _, ok := sm.GetServer("kafka-mcp-sink", "v1.0.0"); ok {
		t.Fatal("precondition: registry must start empty")
	}

	server, ok := sm.FindRunningServer("kafka-mcp-sink", "v1.0.0")
	if !ok || server == nil {
		t.Fatalf("running container not found; asked %v", asked)
	}
	if server.ConnType != "http" || server.Port != 8000 {
		t.Fatalf("got %+v, want the discovered http server", server)
	}
	if again, ok := sm.GetServer("kafka-mcp-sink", "v1.0.0"); !ok || again != server {
		t.Fatal("discovered server was not registered under its versioned key")
	}
}

func TestFindRunningServer_NoContainerIsNotFound(t *testing.T) {
	orig := checkDockerContainerFn
	t.Cleanup(func() { checkDockerContainerFn = orig })
	checkDockerContainerFn = func(*ServerManager, string, string) *ServerInfo { return nil }

	sm := &ServerManager{servers: map[string]*ServerInfo{}}
	if server, ok := sm.FindRunningServer("kafka-mcp-sink", "v1.0.0"); ok || server != nil {
		t.Fatalf("got %+v, want not found", server)
	}
	if len(sm.servers) != 0 {
		t.Fatal("a miss must not register anything")
	}
}

// A registered server wins and the container check is not run at all (the probe
// calls this every 15s).
func TestFindRunningServer_RegisteredServerSkipsDiscovery(t *testing.T) {
	orig := checkDockerContainerFn
	t.Cleanup(func() { checkDockerContainerFn = orig })
	checkDockerContainerFn = func(*ServerManager, string, string) *ServerInfo {
		t.Fatal("container check ran for an already-registered server")
		return nil
	}

	registered := &ServerInfo{Name: "kafka-mcp-sink", Status: "running", ConnType: "http", Host: "h", Port: 8000}
	sm := &ServerManager{servers: map[string]*ServerInfo{makeServerKey("kafka-mcp-sink", "v1.0.0"): registered}}
	if server, ok := sm.FindRunningServer("kafka-mcp-sink", "v1.0.0"); !ok || server != registered {
		t.Fatalf("got %+v, want the registered server", server)
	}
}

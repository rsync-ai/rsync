package mcp

import (
	"errors"
	"os/exec"
	"testing"
	"time"
)

// An http entry is a Docker-hosted connector found by checkDockerContainer: it has
// no Process, and the orchestrator does not own the container. StopAll ran
// server.Process.Process.Kill() on every running entry, so executor.Agent.Stop()
// panicked on shutdown as soon as one http connector was cached.
func httpEntry() *ServerInfo {
	name := StackPrefix() + "-kafka-mcp-sink-v1-0-0-mcp"
	return &ServerInfo{Name: "kafka-mcp-sink", Status: "running", ConnType: "http", Host: name, Port: 8000, ContainerID: name}
}

// stdioEntry starts a real child process, so the tests can also prove the stdio
// kill still happens — a fix that skipped every entry would pass the http half.
func stdioEntry(t *testing.T) (*ServerInfo, <-chan error) {
	t.Helper()
	if _, err := exec.LookPath("sleep"); err != nil {
		t.Skip("sleep not on PATH")
	}
	cmd := exec.Command("sleep", "60")
	stdin, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	exited := make(chan error, 1)
	go func() { exited <- cmd.Wait() }()
	t.Cleanup(func() { _ = cmd.Process.Kill() })
	return &ServerInfo{Name: "postgresql", Status: "running", ConnType: "stdio", Process: cmd, PID: cmd.Process.Pid, StdinPipe: stdin}, exited
}

func requireKilled(t *testing.T, exited <-chan error) {
	t.Helper()
	select {
	case err := <-exited:
		var exitErr *exec.ExitError
		if !errors.As(err, &exitErr) {
			t.Fatalf("stdio process exited with %v, want killed", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("stdio process still running: it was not killed")
	}
}

func noPanic(t *testing.T, what string, fn func() error) error {
	t.Helper()
	var err error
	func() {
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("%s panicked: %v", what, r)
			}
		}()
		err = fn()
	}()
	return err
}

func TestStopAll_DropsHTTPEntryAndKillsStdio(t *testing.T) {
	stdio, exited := stdioEntry(t)
	sm := &ServerManager{servers: map[string]*ServerInfo{
		makeServerKey("kafka-mcp-sink", "v1.0.0"): httpEntry(),
		makeServerKey("postgresql", "v1.0.0"):     stdio,
	}}

	if err := noPanic(t, "StopAll", sm.StopAll); err != nil {
		t.Fatalf("StopAll: %v", err)
	}
	if len(sm.servers) != 0 {
		t.Fatalf("cache not emptied: %v", sm.servers)
	}
	requireKilled(t, exited)
}

func TestStopServer_DropsHTTPEntry(t *testing.T) {
	sm := &ServerManager{servers: map[string]*ServerInfo{
		makeServerKey("kafka-mcp-sink", "v1.0.0"): httpEntry(),
	}}

	err := noPanic(t, "StopServer", func() error { return sm.StopServer("kafka-mcp-sink", "v1.0.0") })
	if err != nil {
		t.Fatalf("StopServer: %v", err)
	}
	if len(sm.servers) != 0 {
		t.Fatalf("cache not emptied: %v", sm.servers)
	}
}

func TestStopServer_KillsStdio(t *testing.T) {
	stdio, exited := stdioEntry(t)
	sm := &ServerManager{servers: map[string]*ServerInfo{
		makeServerKey("postgresql", "v1.0.0"): stdio,
	}}

	if err := sm.StopServer("postgresql", "v1.0.0"); err != nil {
		t.Fatalf("StopServer: %v", err)
	}
	if len(sm.servers) != 0 {
		t.Fatalf("cache not emptied: %v", sm.servers)
	}
	requireKilled(t, exited)
}

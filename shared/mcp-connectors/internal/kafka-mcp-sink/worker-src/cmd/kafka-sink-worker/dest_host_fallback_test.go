package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"

	"github.com/segmentio/kafka-go"
)

// Every destination round-trip in this worker walks destinationHostCandidates: the
// versioned container name first, then compose-era aliases. On Helm only the
// versioned name exists, and on compose several names reach ONE container.
//
// The bug class: a host that ANSWERED — its tool ran and said no — was treated like
// a host that was never reached, and the loop moved on to the next candidate.
//   - The real error was overwritten by the fallback's transport error. On Helm a
//     `duplicate key` from the versioned host surfaced as `lookup
//     rsync-ai-postgresql-mcp: no such host`, which classifyDestFault reads as an
//     infrastructure fault — so a row fault was held as an outage and a refused
//     credential never reached the auth halt.
//   - On compose, where the aliases are the same container, a write that had
//     already run was sent again.
//
// Only a transport error (no answer at all) may try the next candidate; the first
// host that answers is the destination, and its answer is final.

// hostScript answers as a set of candidate hosts would. answers maps a host to its
// JSON body (HTTP 200); reply, when set, answers instead and may vary by tool or
// status. A host neither answers fails exactly as an absent Docker/K8s name does.
// Capability probes are refused so they never consume a scripted answer.
type hostScript struct {
	answers map[string]string
	reply   func(host, tool string) (status int, body string, reachable bool)

	mu    sync.Mutex
	hosts []string // hosts that received a write-tool request, in order
	tools []string // the tool each of those requests called
}

func (h *hostScript) RoundTrip(r *http.Request) (*http.Response, error) {
	host := r.URL.Hostname()
	raw, _ := io.ReadAll(r.Body)
	var req struct {
		Params struct {
			Name string `json:"name"`
		} `json:"params"`
	}
	_ = json.Unmarshal(raw, &req)
	status, body, reachable := 200, "", false
	if h.reply != nil {
		status, body, reachable = h.reply(host, req.Params.Name)
	} else {
		body, reachable = h.answers[host]
	}
	if !reachable {
		return nil, &net.DNSError{Err: "no such host", Name: host, IsNotFound: true}
	}
	if strings.HasSuffix(req.Params.Name, "_get_capabilities") {
		status, body = 200, `{"success":false,"error":"unknown tool"}`
	} else {
		h.mu.Lock()
		h.hosts = append(h.hosts, host)
		h.tools = append(h.tools, req.Params.Name)
		h.mu.Unlock()
	}
	return &http.Response{
		StatusCode: status,
		Status:     fmt.Sprintf("%d %s", status, http.StatusText(status)),
		Body:       io.NopCloser(strings.NewReader(body)),
		Header:     http.Header{},
	}, nil
}

func (h *hostScript) writeHosts() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]string(nil), h.hosts...)
}

func (h *hostScript) writeTools() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]string(nil), h.tools...)
}

// everyHost is the compose shape: every candidate name reaches the same container.
func everyHost(status int, body string) func(host, tool string) (int, string, bool) {
	return func(string, string) (int, string, bool) { return status, body, true }
}

// destEntryPoint is one of the four functions that walk the host candidates.
type destEntryPoint struct {
	name     string
	destType string
	call     func(client *http.Client) error
}

func destEntryPoints() []destEntryPoint {
	const version = "1.0.0"
	pg := &WorkerConfig{PipelineID: "p-host-fallback", DestinationConnector: "postgresql",
		DestinationVersion: version, DestinationConfig: map[string]interface{}{}}
	gcs := &WorkerConfig{PipelineID: "p-host-fallback", DestinationConnector: "gcs",
		DestinationVersion: version, DestinationConfig: map[string]interface{}{"bucket": "b"}}

	return []destEntryPoint{
		{"callDestinationTool", "postgresql", func(c *http.Client) error {
			_, err := callDestinationTool(context.Background(), c, pg, "postgresql", "upsert_data",
				map[string]interface{}{"table": "orders"})
			return err
		}},
		{"writeCDCToDestination", "postgresql", func(c *http.Client) error {
			msg := kafka.Message{Topic: "cdc.inventory.orders", Partition: 0, Offset: 7}
			sm := &SinkMessage{PipelineID: "p-host-fallback", IsCDC: true, CDCOp: "c", Table: "inventory.orders",
				After: map[string]interface{}{"id": 1}, PK: map[string]interface{}{"id": 1}, KeyFields: []string{"id"}}
			_, _, err := writeCDCToDestination(context.Background(), c, pg, nil, msg, sm, "upsert")
			return err
		}},
		{"writeToDestination", "postgresql", func(c *http.Client) error {
			sm := &SinkMessage{PipelineID: "p-host-fallback", Table: "public.orders", RowCount: 1, KeyFields: []string{"id"}}
			_, _, err := writeToDestination(context.Background(), c, pg, nil, sm,
				[]map[string]interface{}{{"id": 1}}, "", "", nil)
			return err
		}},
		{"writeBlobToDestination", "gcs", func(c *http.Client) error {
			sm := &SinkMessage{IsBlob: true, ObjectKey: "docs/a.pdf", ClaimCheckURL: "s3://staging/k"}
			_, err := writeBlobToDestination(context.Background(), c, gcs, sm)
			return err
		}},
	}
}

const rowFault = `duplicate key value violates unique constraint "pg_type_typname_nsp_index"`

func failingAnswer(msg string) string {
	b, _ := json.Marshal(map[string]interface{}{"success": false, "error": msg})
	return string(b)
}

// The Helm shape: the versioned host answers with the real error and the
// compose-era aliases do not exist. The real error must come back, classified as
// what it is, from exactly one request.
func TestDestHostFallback_AnsweredErrorIsNotMaskedByAnAbsentAlias(t *testing.T) {
	for _, ep := range destEntryPoints() {
		t.Run(ep.name, func(t *testing.T) {
			hosts := destinationHostCandidates(ep.destType, "1.0.0")
			if len(hosts) < 2 {
				t.Fatalf("want several candidates to make the fallback observable, got %v", hosts)
			}
			script := &hostScript{answers: map[string]string{hosts[0]: failingAnswer(rowFault)}}
			err := ep.call(&http.Client{Transport: script})
			if err == nil {
				t.Fatal("a write the destination refused returned no error")
			}
			if !strings.Contains(err.Error(), "pg_type_typname_nsp_index") || strings.Contains(err.Error(), "no such host") {
				t.Errorf("err = %q — want the answering host's error, not a fallback alias's DNS failure", err)
			}
			if got := classifyDestFault(err); got != faultData {
				t.Errorf("classifyDestFault = %v, want faultData — a masked row fault is held as an outage", got)
			}
			if got := script.writeHosts(); len(got) != 1 {
				t.Errorf("write reached %v, want exactly one answering host", got)
			}
		})
	}
}

// The compose shape: every candidate name answers, because they are aliases of one
// container. A write the destination refused must not be run again on an alias.
func TestDestHostFallback_RefusedWriteIsNotReplayedOnAnAlias(t *testing.T) {
	for _, ep := range destEntryPoints() {
		t.Run(ep.name, func(t *testing.T) {
			answers := map[string]string{}
			for _, h := range destinationHostCandidates(ep.destType, "1.0.0") {
				answers[h] = failingAnswer(rowFault)
			}
			script := &hostScript{answers: answers}
			if err := ep.call(&http.Client{Transport: script}); err == nil {
				t.Fatal("a write the destination refused returned no error")
			}
			if got := script.writeHosts(); len(got) != 1 {
				t.Errorf("one refused write was sent %d times, to %v — want once", len(got), got)
			}
		})
	}
}

// The fallback the candidates exist for still works: a host that cannot be reached
// hands over to the next, and the next one's answer — success or failure — is the
// result.
func TestDestHostFallback_UnreachableHostStillFallsThrough(t *testing.T) {
	for _, ep := range destEntryPoints() {
		t.Run(ep.name, func(t *testing.T) {
			hosts := destinationHostCandidates(ep.destType, "1.0.0")
			const authFault = "password authentication failed for user \"sink\""
			script := &hostScript{answers: map[string]string{hosts[1]: failingAnswer(authFault)}}
			err := ep.call(&http.Client{Transport: script})
			if err == nil || !strings.Contains(err.Error(), "password authentication failed") {
				t.Fatalf("err = %v — want the second host's answer after the first was unreachable", err)
			}
			if got := classifyDestFault(err); got != faultAuth {
				t.Errorf("classifyDestFault = %v, want faultAuth", got)
			}
			if got := script.writeHosts(); len(got) != 1 || got[0] != hosts[1] {
				t.Errorf("write reached %v, want only %s", got, hosts[1])
			}
		})
	}

	// And a success on the fallback host is a success.
	hosts := destinationHostCandidates("postgresql", "1.0.0")
	script := &hostScript{answers: map[string]string{hosts[1]: `{"success":true,"rows_upserted":1}`}}
	cfg := &WorkerConfig{DestinationConnector: "postgresql", DestinationVersion: "1.0.0", DestinationConfig: map[string]interface{}{}}
	if _, err := callDestinationTool(context.Background(), &http.Client{Transport: script}, cfg, "postgresql", "upsert_data", map[string]interface{}{}); err != nil {
		t.Errorf("fallback host answered success, got err %v", err)
	}
}

// A host that answers with something other than JSON — a proxy's error page, a
// garbled body — still answered. Its reply is final, and the HTTP status survives
// into the error so a 502/503/504 page is classified as the outage it is.
func TestDestHostFallback_NonJSONAnswerIsFinalAndKeepsItsStatus(t *testing.T) {
	for _, tc := range []struct {
		status int
		body   string
		want   destFault
	}{
		{http.StatusBadGateway, "<html><body>502 Bad Gateway</body></html>", faultInfra},
		{http.StatusServiceUnavailable, "upstream connect error", faultInfra},
		{http.StatusOK, "<html>not json</html>", faultUnclassified},
	} {
		for _, ep := range destEntryPoints() {
			t.Run(fmt.Sprintf("%s/HTTP_%d", ep.name, tc.status), func(t *testing.T) {
				script := &hostScript{reply: everyHost(tc.status, tc.body)}
				err := ep.call(&http.Client{Transport: script})
				if err == nil {
					t.Fatal("a non-JSON answer returned no error")
				}
				if want := fmt.Sprintf("HTTP %d", tc.status); !strings.Contains(err.Error(), want) {
					t.Errorf("err = %q — want the answering host's %s kept in the error", err, want)
				}
				if got := classifyDestFault(err); got != tc.want {
					t.Errorf("classifyDestFault = %v, want %v", got, tc.want)
				}
				if got := script.writeHosts(); len(got) != 1 {
					t.Errorf("write reached %v, want exactly one answering host", got)
				}
			})
		}
	}
}

// The CDC and batch writers retry a refused upsert as import_data when the
// connector lacks the tool. That second call goes to the host that answered the
// first — and ITS answer, JSON or not, is final too.
func TestDestHostFallback_ImportDataRetryStaysOnTheAnsweringHost(t *testing.T) {
	for _, second := range []struct {
		name   string
		status int
		body   string
		want   string
	}{
		{"import_data refuses the row", 200, failingAnswer(rowFault), "pg_type_typname_nsp_index"},
		{"import_data answers an error page", http.StatusBadGateway, "<html>502</html>", "HTTP 502"},
	} {
		for _, ep := range destEntryPoints() {
			if ep.name != "writeCDCToDestination" && ep.name != "writeToDestination" {
				continue // only these two retry as import_data
			}
			t.Run(ep.name+"/"+second.name, func(t *testing.T) {
				script := &hostScript{reply: func(_, tool string) (int, string, bool) {
					if strings.HasSuffix(tool, "_import_data") {
						return second.status, second.body, true
					}
					return 200, failingAnswer("tool not found: postgresql_upsert_data"), true
				}}
				err := ep.call(&http.Client{Transport: script})
				if err == nil || !strings.Contains(err.Error(), second.want) {
					t.Fatalf("err = %v — want the import_data answer (%s)", err, second.want)
				}
				hosts, tools := script.writeHosts(), script.writeTools()
				if len(hosts) != 2 || hosts[0] != hosts[1] || !strings.HasSuffix(tools[0], "_upsert_data") || !strings.HasSuffix(tools[1], "_import_data") {
					t.Errorf("writes = %v on %v — want upsert_data then import_data, both on the first host, and nothing on an alias", tools, hosts)
				}
			})
		}
	}
}

// The batch writer refuses a success that reports no write count. The write may
// well have run, so it is not a cue to try an alias — on compose that is the same
// container, and the rows would be written twice.
func TestDestHostFallback_SuccessWithoutACountIsNotReplayed(t *testing.T) {
	for _, ep := range destEntryPoints() {
		if ep.name != "writeToDestination" {
			continue
		}
		script := &hostScript{reply: everyHost(200, `{"success":true}`)}
		err := ep.call(&http.Client{Transport: script})
		if err == nil || !strings.Contains(err.Error(), "missing write-count field") {
			t.Fatalf("err = %v — want the missing write-count refusal", err)
		}
		if got := script.writeHosts(); len(got) != 1 {
			t.Errorf("a batch whose count was missing was sent %d times, to %v — want once", len(got), got)
		}
	}
}

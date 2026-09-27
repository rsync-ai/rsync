package cdcsnapshot

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// ConnectClient reads connector state from the Kafka Connect REST API.
type ConnectClient struct {
	BaseURL string
	HTTP    *http.Client
}

func NewConnectClient(baseURL string) *ConnectClient {
	return &ConnectClient{
		BaseURL: strings.TrimRight(strings.TrimSpace(baseURL), "/"),
		HTTP:    &http.Client{Timeout: 10 * time.Second},
	}
}

// errNotFound is a 404 from Connect: the connector does not exist.
var errNotFound = fmt.Errorf("kafka connect: connector not found")

func (c *ConnectClient) getJSON(ctx context.Context, path string, out interface{}) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.BaseURL+path, nil)
	if err != nil {
		return err
	}
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return errNotFound
	}
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
		return fmt.Errorf("kafka connect GET %s: status %d: %s", path, resp.StatusCode, strings.TrimSpace(string(body)))
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

// Config returns the connector's config (GET /connectors/<name>/config).
func (c *ConnectClient) Config(ctx context.Context, name string) (map[string]interface{}, error) {
	var cfg map[string]interface{}
	if err := c.getJSON(ctx, "/connectors/"+url.PathEscape(name)+"/config", &cfg); err != nil {
		return nil, err
	}
	return cfg, nil
}

// State returns the connector's and its tasks' states and what each running
// task captures. The task configs are what the tasks were STARTED with — the
// connector config can already hold the new table list while the old task
// still runs, which is exactly the window a signal must not be sent in.
func (c *ConnectClient) State(ctx context.Context, name string) (ConnectorState, error) {
	var status struct {
		Connector struct {
			State string `json:"state"`
		} `json:"connector"`
		Tasks []struct {
			ID    int    `json:"id"`
			State string `json:"state"`
		} `json:"tasks"`
	}
	base := "/connectors/" + url.PathEscape(name)
	if err := c.getJSON(ctx, base+"/status", &status); err != nil {
		if err == errNotFound {
			return ConnectorState{Found: false}, nil
		}
		return ConnectorState{}, err
	}
	cs := ConnectorState{Found: true, State: status.Connector.State}
	for _, t := range status.Tasks {
		cs.TaskStates = append(cs.TaskStates, t.State)
	}
	var tasks []struct {
		Config map[string]interface{} `json:"config"`
	}
	if err := c.getJSON(ctx, base+"/tasks", &tasks); err != nil {
		if err == errNotFound {
			return ConnectorState{Found: false}, nil
		}
		return ConnectorState{}, err
	}
	for _, t := range tasks {
		cs.TaskIncludes = append(cs.TaskIncludes, ParseIncludeList(t.Config))
	}
	return cs, nil
}

// BuildExecuteSnapshotSignal encodes the VALUE of a Debezium execute-snapshot
// signal for the Kafka signal channel. The shape is Debezium's, not ours: the
// snapshot type lives under "data", and Debezium silently ignores a signal it
// cannot parse — so getting this wrong means "no backfill" with no error
// anywhere. The message KEY is the connector's topic.prefix (SignalKey).
func BuildExecuteSnapshotSignal(mode string, collections []string) ([]byte, error) {
	if len(collections) == 0 {
		return nil, fmt.Errorf("execute-snapshot signal: no data collections")
	}
	snapshotType := "INCREMENTAL"
	if strings.EqualFold(strings.TrimSpace(mode), "blocking") {
		snapshotType = "BLOCKING"
	}
	return json.Marshal(map[string]interface{}{
		"type": "execute-snapshot",
		"data": map[string]interface{}{
			"type":             snapshotType,
			"data-collections": collections,
		},
	})
}

func cfgString(cfg map[string]interface{}, key string) string {
	v, ok := cfg[key]
	if !ok || v == nil {
		return ""
	}
	return strings.TrimSpace(fmt.Sprint(v))
}

// SignalKey is the Kafka key Debezium accepts a signal under: the connector's
// topic.prefix, which is the connector name when the property is unset.
func SignalKey(cfg map[string]interface{}, connectorName string) string {
	if k := cfgString(cfg, "topic.prefix"); k != "" {
		return k
	}
	return connectorName
}

// SignalTopic is the connector's Kafka signal topic, "" when it has none.
func SignalTopic(cfg map[string]interface{}) string {
	return cfgString(cfg, "signal.kafka.topic")
}

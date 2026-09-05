package kafkaclient

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

// TestMatchesCrossLanguageContract pins this implementation to the same table
// the Python copies are pinned to (llm-service/src/utils/kafka_topics.py and the
// debezium connector's _qualify_topic).
//
// The three exist separately because they ship in different images. Nothing in
// the build links them, and a divergence is silent at runtime: the planner names
// a topic one way, the orchestrator creates it another, and the pipeline goes
// quiet with no error anywhere. This file is the only thing that makes such a
// drift fail a build.
func TestMatchesCrossLanguageContract(t *testing.T) {
	path := filepath.Join("..", "..", "contracts", "kafka-topic-naming.json")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("reading the shared contract: %v", err)
	}

	var contract struct {
		Cases []struct {
			Prefix *string `json:"prefix"`
			Input  string  `json:"input"`
			Want   string  `json:"want"`
		} `json:"cases"`
	}
	if err := json.Unmarshal(raw, &contract); err != nil {
		t.Fatalf("parsing the shared contract: %v", err)
	}
	if len(contract.Cases) == 0 {
		t.Fatal("the shared contract has no cases; it would pass vacuously")
	}

	for _, c := range contract.Cases {
		// A null prefix means "the variable is unset", which is the case that
		// decides what a deployment gets when nobody configures anything.
		if c.Prefix == nil {
			os.Unsetenv(EnvTopicPrefix)
		} else {
			t.Setenv(EnvTopicPrefix, *c.Prefix)
		}
		if got := Topic(c.Input); got != c.Want {
			p := "<unset>"
			if c.Prefix != nil {
				p = *c.Prefix
			}
			t.Errorf("prefix=%q Topic(%q) = %q, want %q", p, c.Input, got, c.Want)
		}
	}
}

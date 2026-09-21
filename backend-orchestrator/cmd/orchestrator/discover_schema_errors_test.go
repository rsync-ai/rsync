package main

import (
	"errors"
	"fmt"
	"net/http"
	"testing"

	"github.com/rsync-ai/backend-orchestrator/internal/agents/executor"
)

func TestDiscoverSchemaErrorResponse(t *testing.T) {
	t.Run("connector-reported failure is 422 with the connector's message", func(t *testing.T) {
		err := &executor.DiscoveryFailedError{Connector: "mongodb", Message: "The DNS query name does not exist"}
		status, body := discoverSchemaErrorResponse(err)
		if status != http.StatusUnprocessableEntity {
			t.Fatalf("status = %d, want 422", status)
		}
		if body["details"] != "The DNS query name does not exist" {
			t.Fatalf("details = %v", body["details"])
		}
	})

	t.Run("a wrapped connector failure is still recognised", func(t *testing.T) {
		err := fmt.Errorf("discover: %w", &executor.DiscoveryFailedError{Message: "auth failed"})
		if status, _ := discoverSchemaErrorResponse(err); status != http.StatusUnprocessableEntity {
			t.Fatalf("status = %d, want 422", status)
		}
	})

	t.Run("transport failure stays 503", func(t *testing.T) {
		status, body := discoverSchemaErrorResponse(errors.New("schema discovery failed: context deadline exceeded"))
		if status != http.StatusServiceUnavailable {
			t.Fatalf("status = %d, want 503", status)
		}
		if body["details"] != "schema discovery failed: context deadline exceeded" {
			t.Fatalf("details = %v", body["details"])
		}
	})
}

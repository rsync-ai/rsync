package correlation

import (
	"context"
	"testing"
	"time"
)

// TestStore_NilReceiver_NoPanic is a regression net for the scheduled-run failure
// where correlationStore was nil (Redis init failed at adapter startup because of a
// REDIS_PASSWORD/--requirepass split-brain) and every V2 activity panicked with a
// nil-pointer dereference at store.go s.redis.Set (ExecutorActivityV2 -> WriteRequest).
//
// The durable fix makes the adapter fail loud at startup (cmd/adapter/main.go), so a
// nil store should never reach these methods in production. This test pins the
// defense-in-depth guard: even if it does, the methods must return a clean error
// instead of panicking.
func TestStore_NilReceiver_NoPanic(t *testing.T) {
	cases := []struct {
		name  string
		store *Store
	}{
		{"nil store pointer", nil},
		{"store with nil redis client", &Store{}},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.store.WriteRequest(context.Background(), Request{CorrelationID: "abc"}); err == nil {
				t.Fatalf("WriteRequest(%s): expected a non-nil error, got nil", tc.name)
			}
			if _, err := tc.store.WaitForResponse(context.Background(), "abc", 10*time.Millisecond); err == nil {
				t.Fatalf("WaitForResponse(%s): expected a non-nil error, got nil", tc.name)
			}
		})
	}
}

package executor

import (
	"os"
	"runtime"
	"strconv"
	"strings"
)

// Per-table worker-pool sizing for the batch data plane (executeBatchDataTransfer,
// also used by the hybrid-CDC historical backfill). The pool fans table-level work
// — schema discovery, sink dispatch, chunked export — across goroutines. The right
// size scales with the host/container's available CPU and memory rather than a fixed
// constant, so a bigger box moves more tables in parallel automatically and a small
// / memory-capped box does not oversubscribe and OOM.
const (
	// Lower/upper bounds on the auto-derived pool. The floor keeps small (1-2 vCPU)
	// boxes from serializing multi-table syncs; the ceiling caps fan-out so we don't
	// swamp the source DB, Kafka, or the destination MCP with connections.
	//
	// The floor is currently unreachable and kept only as a guard: numCPU is clamped
	// to >= 1 before scaling, so the CPU term is always >= 1*tableConcurrencyCPUScale
	// = 2 = this value. It would start binding again if the scale dropped to 1. The
	// thing that actually serializes a sync is the memory guard below, not this.
	tableConcurrencyMin = 2
	tableConcurrencyMax = 32

	// Per-table work is I/O-bound (source read + MCP round-trips + sink wait), so a
	// pool larger than the CPU count keeps cores busy while workers block on I/O.
	tableConcurrencyCPUScale = 2

	// Assumed memory headroom per concurrent table load. Each worker stages chunks
	// in memory before the MinIO claim-check offload; budget against this so the
	// pool never exceeds what the (cgroup) memory limit can buffer.
	perTableMemBudgetBytes = 384 * 1024 * 1024 // ~384 MiB
)

// computeTableConcurrency is the pure, testable core of the sizing decision.
//
// Given the detected CPU count, the memory limit in bytes (0 = unknown/unlimited),
// and an explicit operator override (0 = none), it returns the worker-pool size
// (before the per-run table-count clamp applied by the caller). The override always
// wins so ops can pin a value; otherwise the size is CPU-scaled, clamped to
// [min,max], and further capped by the memory budget.
func computeTableConcurrency(numCPU int, memLimitBytes int64, override int) int {
	size, _ := decideTableConcurrency(numCPU, memLimitBytes, override)
	return size
}

// Binding-constraint labels for the sizing decision. These are log strings, not
// control flow — see decideTableConcurrency for why they exist at all.
const (
	concurrencyBoundByOverride = "EXECUTOR_TABLE_CONCURRENCY override"
	concurrencyBoundByCPU      = "CPU"
	concurrencyBoundByFloor    = "floor"
	concurrencyBoundByCeiling  = "ceiling"
	concurrencyBoundByMemory   = "container memory limit"
)

// decideTableConcurrency returns the pool size AND which constraint produced it.
//
// The reason is not decoration. The memory guard can dominate the CPU term
// completely and silently: a 512 MiB orchestrator yields 512/384 = 1 worker at
// ANY CPU count, so an operator who gives the box more vCPU sees the sync stay
// strictly serial and has nothing in the logs to explain it. Reporting the
// binding constraint turns "concurrency=1" from an unexplained number into a
// diagnosis that names the knob to turn (raise mem_limit, or pin the override).
//
// Callers that only need the number use computeTableConcurrency.
func decideTableConcurrency(numCPU int, memLimitBytes int64, override int) (int, string) {
	if override > 0 {
		return override, concurrencyBoundByOverride
	}
	if numCPU < 1 {
		numCPU = 1
	}
	reason := concurrencyBoundByCPU
	c := numCPU * tableConcurrencyCPUScale
	if c < tableConcurrencyMin {
		c = tableConcurrencyMin
		reason = concurrencyBoundByFloor
	}
	if c > tableConcurrencyMax {
		c = tableConcurrencyMax
		reason = concurrencyBoundByCeiling
	}
	// Memory guard: never run more concurrent table loads than the box can buffer.
	if memLimitBytes > 0 {
		memCap := int(memLimitBytes / perTableMemBudgetBytes)
		if memCap < 1 {
			memCap = 1
		}
		if memCap < c {
			c = memCap
			reason = concurrencyBoundByMemory
		}
	}
	if c < 1 {
		c = 1
	}
	return c, reason
}

// tableConcurrencyOverride reads the explicit EXECUTOR_TABLE_CONCURRENCY ops knob.
// Returns 0 when unset or invalid, which selects resource-derived sizing.
func tableConcurrencyOverride() int {
	if raw := strings.TrimSpace(os.Getenv("EXECUTOR_TABLE_CONCURRENCY")); raw != "" {
		if n, err := strconv.Atoi(raw); err == nil && n > 0 {
			return n
		}
	}
	return 0
}

// resolveTableConcurrency returns the resource-aware per-table worker-pool size.
//
// EXECUTOR_TABLE_CONCURRENCY overrides everything; otherwise the size is derived
// from GOMAXPROCS — cgroup-CPU-aware on Go 1.25+, so it reflects the container's
// CPU allocation, not the bare host — and capped by the detected memory limit.
func resolveTableConcurrency() int {
	size, _ := resolveTableConcurrencyWithReason()
	return size
}

// resolveTableConcurrencyWithReason is resolveTableConcurrency plus a human-readable
// account of the inputs and the binding constraint, for the dispatch log line.
func resolveTableConcurrencyWithReason() (int, string) {
	cpu := runtime.GOMAXPROCS(0)
	mem := detectMemoryLimitBytes()
	size, bound := decideTableConcurrency(cpu, mem, tableConcurrencyOverride())
	return size, describeConcurrency(cpu, mem, bound)
}

// describeConcurrency renders the operator-facing account of a sizing decision.
// Split out from resolveTableConcurrencyWithReason so the exact string a customer
// will paste into a support thread is asserted by a test rather than assumed.
func describeConcurrency(cpu int, memLimitBytes int64, bound string) string {
	memDesc := "unlimited"
	if memLimitBytes > 0 {
		memDesc = strconv.FormatInt(memLimitBytes/(1024*1024), 10) + "MiB"
	}
	return "bound by " + bound + " (GOMAXPROCS=" + strconv.Itoa(cpu) +
		", mem=" + memDesc + ", budget=" + strconv.FormatInt(perTableMemBudgetBytes/(1024*1024), 10) + "MiB/worker)"
}

// detectMemoryLimitBytes best-effort reads the container/cgroup memory limit in
// bytes. Returns 0 when the limit is unknown or effectively unlimited, so callers
// skip the memory cap. Linux cgroup v2 (memory.max) is tried first, then v1
// (memory.limit_in_bytes). Non-Linux hosts / unreadable paths return 0.
func detectMemoryLimitBytes() int64 {
	// cgroup v2
	if b, err := os.ReadFile("/sys/fs/cgroup/memory.max"); err == nil {
		if v := parseCgroupMemLimit(strings.TrimSpace(string(b))); v > 0 {
			return v
		}
	}
	// cgroup v1
	if b, err := os.ReadFile("/sys/fs/cgroup/memory/memory.limit_in_bytes"); err == nil {
		if v := parseCgroupMemLimit(strings.TrimSpace(string(b))); v > 0 {
			return v
		}
	}
	return 0
}

// parseCgroupMemLimit parses a cgroup memory-limit file value into bytes. The v2
// sentinel "max" and the v1 "unlimited" (a huge page-aligned value) both map to 0
// (unknown/unlimited), as do empty or non-numeric values.
func parseCgroupMemLimit(s string) int64 {
	if s == "" || s == "max" {
		return 0
	}
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil || n <= 0 {
		return 0
	}
	// cgroup v1 "unlimited" is a huge page-aligned sentinel (~9.2e18). Treat any
	// limit >= 1 PiB as effectively unlimited so we don't mistake it for a real cap.
	if n >= 1<<50 {
		return 0
	}
	return n
}

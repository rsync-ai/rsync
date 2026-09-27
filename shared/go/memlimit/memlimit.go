// Package memlimit gives a Go service a soft memory limit taken from its container's
// cgroup, so the garbage collector works harder as the process nears the cap instead of
// letting the heap grow until the kernel OOM-kills the container.
//
// Go (1.25) derives GOMAXPROCS from the cgroup CPU limit but does NOT derive GOMEMLIMIT
// from the memory limit: with GOGC=100 the heap may double between collections, so a
// service idling at 40 MiB inside a 512 MiB container can still be killed by one burst
// the GC would have absorbed had it known where the wall was. Apply closes that gap.
//
// An explicit GOMEMLIMIT always wins — the runtime has already honoured it by the time
// Apply runs, and Apply leaves it alone. No cgroup limit (a bare host, macOS, "max")
// means no soft limit, exactly as before.
package memlimit

import (
	"os"
	"runtime/debug"
	"strconv"
	"strings"
)

// DefaultRatio is the share of the cgroup limit handed to the Go runtime. The remaining
// 10% covers what the soft limit cannot see or steer: goroutine stacks beyond the heap
// accounting slack, cgo/OS allocations, and page cache charged to the same cgroup.
const DefaultRatio = 0.9

// cgroup files, v2 first. Vars so tests can point them at fixtures.
var (
	cgroupV2File = "/sys/fs/cgroup/memory.max"
	cgroupV1File = "/sys/fs/cgroup/memory/memory.limit_in_bytes"
)

// Result says what Apply did, for the caller's own logger.
type Result struct {
	// Source is "env" (GOMEMLIMIT was set; left alone), "cgroup" (soft limit applied)
	// or "none" (no finite cgroup limit found; nothing changed).
	Source string
	// CgroupBytes is the container limit read, 0 when unknown/unlimited.
	CgroupBytes int64
	// LimitBytes is the soft limit now in force, 0 when Source is "none".
	LimitBytes int64
}

// Apply sets the runtime soft memory limit to DefaultRatio of the cgroup limit unless
// GOMEMLIMIT is set. Call it first thing in main.
func Apply() Result {
	return apply(os.Getenv, os.ReadFile, debug.SetMemoryLimit)
}

func apply(getenv func(string) string, readFile func(string) ([]byte, error), setLimit func(int64) int64) Result {
	if strings.TrimSpace(getenv("GOMEMLIMIT")) != "" {
		// setLimit(-1) reads the current limit without changing it.
		return Result{Source: "env", LimitBytes: setLimit(-1)}
	}
	cg := cgroupLimitBytes(readFile)
	if cg <= 0 {
		return Result{Source: "none"}
	}
	limit := int64(float64(cg) * DefaultRatio)
	setLimit(limit)
	return Result{Source: "cgroup", CgroupBytes: cg, LimitBytes: limit}
}

// cgroupLimitBytes reads the container memory limit, cgroup v2 then v1. 0 = unknown or
// unlimited.
func cgroupLimitBytes(readFile func(string) ([]byte, error)) int64 {
	for _, f := range []string{cgroupV2File, cgroupV1File} {
		if b, err := readFile(f); err == nil {
			if v := parseLimit(strings.TrimSpace(string(b))); v > 0 {
				return v
			}
		}
	}
	return 0
}

// parseLimit maps a cgroup memory-limit value to bytes. The v2 sentinel "max" and the
// v1 "unlimited" value (a huge page-aligned number, ~9.2e18) both map to 0, as do empty
// or malformed values. Same rule as backend-orchestrator's parseCgroupMemLimit.
func parseLimit(s string) int64 {
	if s == "" || s == "max" {
		return 0
	}
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil || n <= 0 || n >= 1<<50 {
		return 0
	}
	return n
}

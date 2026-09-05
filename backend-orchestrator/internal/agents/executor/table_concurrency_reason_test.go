package executor

import "testing"

// The sizing function is resource-derived, which means the number it returns is a
// property of the DEPLOYMENT, not of this package. These cases pin what each
// shipped compose file actually buys, so a change to a mem_limit that silently
// serializes batch syncs shows up here as a failing test rather than as a slow
// customer.
func TestConcurrencyForShippedDeployments(t *testing.T) {
	const mib = int64(1024 * 1024)

	cases := []struct {
		name       string
		cpu        int
		mem        int64
		wantSize   int
		wantReason string
	}{
		// docker-compose.prod.yml pins orchestrator at 512M. 512/384 = 1, so the
		// memory guard alone decides the pool -- and it decides 1 no matter how
		// many cores the box has. Adding vCPU to a serial batch sync on prod does
		// nothing; this is the case that motivated reporting the reason at all.
		{"prod overlay 512M, 4 vCPU", 4, 512 * mib, 1, concurrencyBoundByMemory},
		{"prod overlay 512M, 8 vCPU", 8, 512 * mib, 1, concurrencyBoundByMemory},
		{"prod overlay 512M, 32 vCPU", 32, 512 * mib, 1, concurrencyBoundByMemory},

		// docker-compose.quickstart.yml gives orchestrator 2g -> 2048/384 = 5,
		// which still binds before CPU on a 4 vCPU box (4*2 = 8).
		{"quickstart 2g, 4 vCPU", 4, 2048 * mib, 5, concurrencyBoundByMemory},
		// ...but not on a 2 vCPU box, where CPU is the tighter of the two.
		{"quickstart 2g, 2 vCPU", 2, 2048 * mib, 4, concurrencyBoundByCPU},

		// docker-compose.yml (dev) sets no limit at all, so CPU is the only term.
		{"dev compose unlimited, 4 vCPU", 4, 0, 8, concurrencyBoundByCPU},

		// Bounds still attribute correctly. Note the asymmetry: the CEILING binds,
		// the FLOOR cannot. numCPU is clamped to >= 1 before scaling, so the CPU
		// term is always >= 1*2 = 2 = tableConcurrencyMin, and the `c < min` branch
		// is dead by construction. A 1 vCPU box is therefore attributed to CPU, not
		// to the floor -- which is the honest answer, since CPU is what produced 2.
		{"1 vCPU: CPU term already equals the floor", 1, 0, 2, concurrencyBoundByCPU},
		{"0 vCPU clamps to 1, still CPU-bound", 0, 0, 2, concurrencyBoundByCPU},
		{"64 vCPU hits the ceiling", 64, 0, 32, concurrencyBoundByCeiling},

		// An explicit override is reported as such even when it contradicts the
		// resources -- otherwise a pinned-too-high value looks resource-derived.
		{"override beats a tight memory limit", 4, 512 * mib, 16, concurrencyBoundByOverride},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			override := 0
			if tc.wantReason == concurrencyBoundByOverride {
				override = tc.wantSize
			}
			got, reason := decideTableConcurrency(tc.cpu, tc.mem, override)
			if got != tc.wantSize {
				t.Fatalf("size = %d, want %d", got, tc.wantSize)
			}
			if reason != tc.wantReason {
				t.Fatalf("reason = %q, want %q", reason, tc.wantReason)
			}
		})
	}
}

// The wrapper must stay a pure projection of the decision, or the logged reason
// and the pool the code actually uses could drift apart -- which would be worse
// than not logging a reason at all.
func TestComputeTableConcurrencyAgreesWithDecide(t *testing.T) {
	const mib = int64(1024 * 1024)
	for _, cpu := range []int{0, 1, 2, 4, 8, 64} {
		for _, mem := range []int64{0, 256 * mib, 512 * mib, 2048 * mib, 16384 * mib} {
			for _, override := range []int{0, 3} {
				want, _ := decideTableConcurrency(cpu, mem, override)
				if got := computeTableConcurrency(cpu, mem, override); got != want {
					t.Fatalf("cpu=%d mem=%d override=%d: compute=%d decide=%d",
						cpu, mem, override, got, want)
				}
			}
		}
	}
}

// The log line is the whole deliverable here, so assert the literal string an
// operator will read rather than trusting that it formats sensibly.
func TestDescribeConcurrencyRendersTheOperatorFacingString(t *testing.T) {
	const mib = int64(1024 * 1024)

	got := describeConcurrency(8, 512*mib, concurrencyBoundByMemory)
	want := "bound by container memory limit (GOMAXPROCS=8, mem=512MiB, budget=384MiB/worker)"
	if got != want {
		t.Fatalf("prod shape:\n got %q\nwant %q", got, want)
	}

	got = describeConcurrency(4, 0, concurrencyBoundByCPU)
	want = "bound by CPU (GOMAXPROCS=4, mem=unlimited, budget=384MiB/worker)"
	if got != want {
		t.Fatalf("unlimited shape:\n got %q\nwant %q", got, want)
	}
}

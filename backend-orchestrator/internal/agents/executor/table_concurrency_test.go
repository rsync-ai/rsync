package executor

import "testing"

func TestComputeTableConcurrency(t *testing.T) {
	const gib = int64(1024 * 1024 * 1024)

	cases := []struct {
		name     string
		numCPU   int
		memBytes int64
		override int
		want     int
	}{
		// Override always wins, ignoring CPU/memory.
		{"override wins", 8, 16 * gib, 6, 6},
		{"override wins even above max", 8, 16 * gib, 100, 100},

		// CPU scaling (x2), no memory limit.
		{"1 cpu -> floor", 1, 0, 0, 2},   // 1*2=2 == min
		{"2 cpu", 2, 0, 0, 4},            // 2*2=4
		{"4 cpu", 4, 0, 0, 8},            // 4*2=8
		{"8 cpu", 8, 0, 0, 16},           // 8*2=16
		{"big box clamps to max", 64, 0, 0, 32}, // 64*2=128 -> capped at 32

		// Floor applies for pathological CPU counts.
		{"zero cpu -> floor", 0, 0, 0, 2},
		{"negative cpu -> floor", -3, 0, 0, 2},

		// Memory cap tightens the CPU-derived value (budget ~384 MiB per worker).
		{"tiny mem caps to 1", 8, 256 * 1024 * 1024, 0, 1}, // 256MiB/384MiB -> <1 -> 1
		{"1 GiB caps below cpu", 8, gib, 0, 2},             // 1024/384 = 2 < 16
		{"8 GiB does not cap 4 cpu", 4, 8 * gib, 0, 8},     // 8GiB/384MiB=21 >= 8
		{"unknown mem -> cpu only", 16, 0, 0, 32},          // 16*2=32
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := computeTableConcurrency(tc.numCPU, tc.memBytes, tc.override)
			if got != tc.want {
				t.Fatalf("computeTableConcurrency(cpu=%d, mem=%d, override=%d) = %d, want %d",
					tc.numCPU, tc.memBytes, tc.override, got, tc.want)
			}
			if got < 1 {
				t.Fatalf("concurrency must be >= 1, got %d", got)
			}
		})
	}
}

func TestParseCgroupMemLimit(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want int64
	}{
		{"empty", "", 0},
		{"v2 max", "max", 0},
		{"valid 512MiB", "536870912", 536870912},
		{"valid 4GiB", "4294967296", 4294967296},
		{"v1 unlimited sentinel", "9223372036854771712", 0}, // ~9.2e18 >= 1 PiB
		{"exactly 1 PiB is unlimited", "1125899906842624", 0},
		{"just under 1 PiB is a real cap", "1125899906842623", 1125899906842623},
		{"negative", "-1", 0},
		{"garbage", "not-a-number", 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := parseCgroupMemLimit(tc.in); got != tc.want {
				t.Fatalf("parseCgroupMemLimit(%q) = %d, want %d", tc.in, got, tc.want)
			}
		})
	}
}

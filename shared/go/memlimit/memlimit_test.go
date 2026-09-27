package memlimit

import (
	"errors"
	"os"
	"path/filepath"
	"runtime/debug"
	"testing"
)

func fakeFS(files map[string]string) func(string) ([]byte, error) {
	return func(p string) ([]byte, error) {
		if v, ok := files[p]; ok {
			return []byte(v), nil
		}
		return nil, errors.New("not found")
	}
}

// share is the soft limit Apply should derive from a cgroup limit of n bytes.
func share(n int64) int64 { return int64(float64(n) * DefaultRatio) }

func env(vals map[string]string) func(string) string {
	return func(k string) string { return vals[k] }
}

func TestApply(t *testing.T) {
	const mib = 1024 * 1024
	cases := []struct {
		name       string
		env        map[string]string
		files      map[string]string
		wantSource string
		wantSet    int64 // 0 = setLimit must not be called with a new value
	}{
		{"v2 512MiB", nil, map[string]string{cgroupV2File: "536870912\n"}, "cgroup", share(512 * mib)},
		{"v2 max falls back to v1", nil, map[string]string{cgroupV2File: "max\n", cgroupV1File: "268435456"}, "cgroup", share(256 * mib)},
		{"v1 unlimited sentinel", nil, map[string]string{cgroupV1File: "9223372036854771712"}, "none", 0},
		{"no cgroup files", nil, nil, "none", 0},
		{"garbage", nil, map[string]string{cgroupV2File: "lots"}, "none", 0},
		{"GOMEMLIMIT wins over cgroup", map[string]string{"GOMEMLIMIT": "300MiB"}, map[string]string{cgroupV2File: "536870912"}, "env", 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var set int64
			got := apply(env(tc.env), fakeFS(tc.files), func(n int64) int64 {
				if n >= 0 {
					set = n
				}
				return 123
			})
			if got.Source != tc.wantSource {
				t.Fatalf("source = %q, want %q", got.Source, tc.wantSource)
			}
			if set != tc.wantSet {
				t.Fatalf("setLimit(%d), want %d", set, tc.wantSet)
			}
			if tc.wantSet > 0 && got.LimitBytes != tc.wantSet {
				t.Fatalf("LimitBytes = %d, want %d", got.LimitBytes, tc.wantSet)
			}
		})
	}
}

// TestApplyRealRuntime drives the exported path against a fixture cgroup file and reads
// the limit back from the Go runtime itself, so a wiring slip (e.g. never calling
// debug.SetMemoryLimit) fails here rather than passing on the fakes above.
func TestApplyRealRuntime(t *testing.T) {
	if os.Getenv("GOMEMLIMIT") != "" {
		t.Skip("GOMEMLIMIT set in the test environment")
	}
	dir := t.TempDir()
	f := filepath.Join(dir, "memory.max")
	if err := os.WriteFile(f, []byte("1073741824\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	oldV2, oldV1 := cgroupV2File, cgroupV1File
	cgroupV2File, cgroupV1File = f, filepath.Join(dir, "absent")
	prev := debug.SetMemoryLimit(-1)
	t.Cleanup(func() {
		cgroupV2File, cgroupV1File = oldV2, oldV1
		debug.SetMemoryLimit(prev)
	})

	res := Apply()
	want := share(1 << 30)
	if res.Source != "cgroup" || res.LimitBytes != want {
		t.Fatalf("Apply() = %+v, want cgroup/%d", res, want)
	}
	if got := debug.SetMemoryLimit(-1); got != want {
		t.Fatalf("runtime soft limit = %d, want %d", got, want)
	}
}

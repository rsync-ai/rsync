package dockerx

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

// A deploy must not keep serving connector code the volume no longer holds.
//
// 0.1.7-rc1 on a VM: after an upgrade the connector volume held the new release's
// code, but Deploy reused the running mcp-<id> container, and when there was none it
// reused the mcp-<id>:<version> image because an image of that name existed. The
// version string names the connector release, not the bytes, so both answered "up to
// date" for code that was a release old. The bug class is "an existing image or
// container stands in for a build context that has since changed"; each of the two
// reuse points is checked here, plus the pre-fingerprint container an upgrade finds.

func staleOpts(hash string) DeployOptions {
	o := baseOpts()
	o.ContextHash = hash
	return o
}

func TestDeploy_RunningContainerFromOlderCode_IsRebuilt(t *testing.T) {
	fb := &fakeBackend{
		snap:        &ContainerSnapshot{ID: "oldcontainer1234", Status: "running", Running: true, Labels: map[string]string{ContextHashLabel: "old"}},
		imageExists: true, imageLabels: map[string]string{ContextHashLabel: "old"},
		createID: "newcontainer5678",
	}
	res, err := NewDeployer(fb, "/nonexistent").Deploy(context.Background(), validReq(), testDcfg, staleOpts("new"))
	if err != nil {
		t.Fatalf("Deploy: %v", err)
	}
	if len(fb.removed) == 0 || !fb.buildCalled || !fb.createCalled || !res.Built {
		t.Fatalf("a container built from older code must be replaced by a fresh build; removed=%v build=%v create=%v",
			fb.removed, fb.buildCalled, fb.createCalled)
	}
	if got := fb.createdCfg.Labels[ContextHashLabel]; got != "new" {
		t.Errorf("new container label %s = %q, want the context it was built from", ContextHashLabel, got)
	}
	if !contains(fb.buildLabels, ContextHashLabel+"=new") {
		t.Errorf("image built without its context label: %v", fb.buildLabels)
	}
}

func TestDeploy_ContainerFromBeforeTheFingerprint_IsRebuilt(t *testing.T) {
	// The upgrade case: containers and images a previous release made carry no label.
	fb := &fakeBackend{
		snap:        &ContainerSnapshot{ID: "v016container123", Status: "running", Running: true},
		imageExists: true,
		createID:    "newcontainer5678",
	}
	if _, err := NewDeployer(fb, "/nonexistent").Deploy(context.Background(), validReq(), testDcfg, staleOpts("new")); err != nil {
		t.Fatalf("Deploy: %v", err)
	}
	if !fb.buildCalled || !fb.createCalled {
		t.Errorf("an unlabelled container/image must be rebuilt once; build=%v create=%v", fb.buildCalled, fb.createCalled)
	}
}

func TestDeploy_ImageFromOlderCode_IsRebuiltEvenWithNoContainer(t *testing.T) {
	fb := &fakeBackend{imageExists: true, imageLabels: map[string]string{ContextHashLabel: "old"}, createID: "newcontainer5678"}
	if _, err := NewDeployer(fb, "/nonexistent").Deploy(context.Background(), validReq(), testDcfg, staleOpts("new")); err != nil {
		t.Fatalf("Deploy: %v", err)
	}
	if !fb.buildCalled {
		t.Error("an image built from older code must not be reused")
	}
}

func TestDeploy_StaleContainerOverACurrentImage_RecreatesWithoutABuild(t *testing.T) {
	fb := &fakeBackend{
		snap:        &ContainerSnapshot{ID: "oldcontainer1234", Status: "running", Running: true, Labels: map[string]string{ContextHashLabel: "old"}},
		imageExists: true, imageLabels: map[string]string{ContextHashLabel: "new"},
		createID: "newcontainer5678",
	}
	if _, err := NewDeployer(fb, "/nonexistent").Deploy(context.Background(), validReq(), testDcfg, staleOpts("new")); err != nil {
		t.Fatalf("Deploy: %v", err)
	}
	if fb.buildCalled {
		t.Error("the image already matches the context; only the container is stale")
	}
	if len(fb.removed) == 0 || !fb.createCalled {
		t.Errorf("the stale container must be replaced; removed=%v create=%v", fb.removed, fb.createCalled)
	}
}

func TestDeploy_CurrentContainer_IsReused(t *testing.T) {
	// Control: a matching fingerprint keeps the old fast path.
	fb := &fakeBackend{
		snap:        &ContainerSnapshot{ID: "runningid1234567", Status: "running", Running: true, Labels: map[string]string{ContextHashLabel: "same"}},
		imageExists: true, imageLabels: map[string]string{ContextHashLabel: "same"},
	}
	res, err := NewDeployer(fb, "/nonexistent").Deploy(context.Background(), validReq(), testDcfg, staleOpts("same"))
	if err != nil {
		t.Fatalf("Deploy: %v", err)
	}
	if fb.buildCalled || fb.createCalled || len(fb.removed) != 0 || res.Built {
		t.Errorf("a current container must be reused untouched; build=%v create=%v removed=%v", fb.buildCalled, fb.createCalled, fb.removed)
	}
}

func TestDeploy_NoFingerprint_ReusesAsBefore(t *testing.T) {
	// When the server could not fingerprint the context, reuse beats a rebuild loop.
	fb := &fakeBackend{snap: &ContainerSnapshot{ID: "runningid1234567", Status: "running", Running: true, Labels: map[string]string{ContextHashLabel: "old"}}}
	if _, err := NewDeployer(fb, "/nonexistent").Deploy(context.Background(), validReq(), testDcfg, staleOpts("")); err != nil {
		t.Fatalf("Deploy: %v", err)
	}
	if fb.buildCalled || fb.createCalled {
		t.Error("without a fingerprint Deploy must fall back to reuse")
	}
}

// ---- ContextHash ----

// connectorTree lays out <tools>/public/{canonical_types.py,warehouse_adapters.py,
// rsync_protocol/} and one connector version dir whose Dockerfile copies two of them.
func connectorTree(t *testing.T) (public, ctx string) {
	t.Helper()
	public = filepath.Join(t.TempDir(), "public")
	ctx = filepath.Join(public, "database", "mongodb", "versions", "v1.0.0")
	files := map[string]string{
		filepath.Join(public, "canonical_types.py"):               "# types\n",
		filepath.Join(public, "warehouse_adapters.py"):            "# not copied by this connector\n",
		filepath.Join(public, "rsync_protocol", "checkpoints.py"): "# cp\n",
		filepath.Join(ctx, "connector.py"):                        "# v1\n",
		filepath.Join(ctx, "Dockerfile"): "FROM python:3.13-slim\nCOPY . /app/\n" +
			"COPY --from=shared canonical_types.py /app/canonical_types.py\n" +
			"COPY --chown=1000:1000 --from=shared \\\n    rsync_protocol /app/rsync_protocol\n",
	}
	for p, body := range files {
		mustWrite(t, p, body)
	}
	return public, ctx
}

func mustWrite(t *testing.T, path, body string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
}

func mustHash(t *testing.T, ctx string) string {
	t.Helper()
	h, err := ContextHash(ctx)
	if err != nil {
		t.Fatalf("ContextHash: %v", err)
	}
	if h == "" {
		t.Fatal("ContextHash returned an empty fingerprint")
	}
	return h
}

func TestContextHash_FollowsWhatTheImageIsBuiltFrom(t *testing.T) {
	cases := []struct {
		name    string
		edit    func(public, ctx string)
		changes bool
	}{
		{"a connector file changes", func(_, ctx string) { mustWrite(t, filepath.Join(ctx, "connector.py"), "# v2\n") }, true},
		{"a connector file is added", func(_, ctx string) { mustWrite(t, filepath.Join(ctx, "helper.py"), "# new\n") }, true},
		{"a shared file the Dockerfile copies changes", func(public, _ string) {
			mustWrite(t, filepath.Join(public, "canonical_types.py"), "# types v2\n")
		}, true},
		{"a file inside a copied shared dir changes (continued COPY line)", func(public, _ string) {
			mustWrite(t, filepath.Join(public, "rsync_protocol", "checkpoints.py"), "# cp v2\n")
		}, true},
		{"a shared file the Dockerfile does not copy changes", func(public, _ string) {
			mustWrite(t, filepath.Join(public, "warehouse_adapters.py"), "# edited\n")
		}, false},
		{"python writes bytecode next to the code", func(_, ctx string) {
			mustWrite(t, filepath.Join(ctx, "__pycache__", "connector.cpython-313.pyc"), "\x00bytecode")
		}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			public, ctx := connectorTree(t)
			before := mustHash(t, ctx)
			if again := mustHash(t, ctx); again != before {
				t.Fatalf("fingerprint is not stable: %s then %s", before, again)
			}
			tc.edit(public, ctx)
			if after := mustHash(t, ctx); (after != before) != tc.changes {
				t.Errorf("fingerprint changed=%v, want %v", after != before, tc.changes)
			}
		})
	}
}

func TestContextHash_ASharedPathCannotWalkOutOfTheSharedContext(t *testing.T) {
	// The Dockerfile of a generated connector is not trusted: a `..` source must be
	// read inside public/ (as BuildKit resolves it), never above it.
	public, ctx := connectorTree(t)
	outside := filepath.Join(filepath.Dir(public), "secret.txt")
	mustWrite(t, outside, "one\n")
	mustWrite(t, filepath.Join(ctx, "Dockerfile"), "FROM scratch\nCOPY --from=shared ../secret.txt /x\n")
	before := mustHash(t, ctx)
	mustWrite(t, outside, "two\n")
	if after := mustHash(t, ctx); after != before {
		t.Error("a file outside the shared context changed the fingerprint")
	}
}

func contains(xs []string, want string) bool {
	for _, x := range xs {
		if x == want {
			return true
		}
	}
	return false
}

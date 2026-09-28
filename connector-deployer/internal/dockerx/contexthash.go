package dockerx

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
)

// ContextHashLabel carries ContextHash on every image Deploy builds and every
// container it creates. The image tag cannot do this job: mcp-<id>:<version> names
// the connector release, and the bytes behind one version change on an upgrade or a
// re-generation, so a tag match said "current" for code a release old (0.1.7-rc1).
const ContextHashLabel = "mcp.build.context-hash"

// ContextHash fingerprints what an image built from contextDir is made of: every
// file under contextDir (the connector version directory) and every path its
// Dockerfile copies out of the `shared` named context (public/). Shared files the
// Dockerfile does not copy are left out, so editing one connector's helper does not
// rebuild every other connector. Python bytecode is left out too: it is written
// next to the code by whoever imports it and never changes what the image runs.
func ContextHash(contextDir string) (string, error) {
	h := sha256.New()
	if err := hashTree(h, "context", contextDir, "."); err != nil {
		return "", err
	}
	sources, err := sharedSources(filepath.Join(contextDir, "Dockerfile"))
	if err != nil {
		return "", err
	}
	if len(sources) > 0 {
		sharedDir, err := resolveSharedContext(contextDir)
		if err != nil {
			return "", err
		}
		for _, src := range sources {
			if err := hashShared(h, sharedDir, src); err != nil {
				return "", err
			}
		}
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// sharedSources returns the source operands of every `COPY --from=shared` in the
// Dockerfile, with backslash continuations joined.
func sharedSources(dockerfile string) ([]string, error) {
	data, err := os.ReadFile(dockerfile)
	if err != nil {
		return nil, err
	}
	text := strings.ReplaceAll(strings.ReplaceAll(string(data), "\r\n", "\n"), "\\\n", " ")
	var out []string
	for _, line := range strings.Split(text, "\n") {
		fields := strings.Fields(line)
		if len(fields) == 0 || !strings.EqualFold(fields[0], "COPY") {
			continue
		}
		fromShared := false
		var operands []string
		for _, f := range fields[1:] {
			if strings.HasPrefix(f, "--") {
				fromShared = fromShared || strings.EqualFold(f, "--from=shared")
				continue
			}
			operands = append(operands, f)
		}
		if fromShared && len(operands) >= 2 {
			out = append(out, operands[:len(operands)-1]...) // the last operand is the destination
		}
	}
	return out, nil
}

// hashShared adds one COPY source to h. The source is resolved the way BuildKit
// resolves it -- inside the shared context, `..` and a leading `/` clamped to its
// root -- because a generated connector's Dockerfile is not trusted input.
func hashShared(h hash.Hash, sharedDir, src string) error {
	rel := strings.TrimPrefix(path.Clean("/"+filepath.ToSlash(src)), "/")
	if rel == "" {
		rel = "."
	}
	if !strings.ContainsAny(rel, "*?[") {
		return hashTree(h, "shared", sharedDir, rel)
	}
	matches, err := filepath.Glob(filepath.Join(sharedDir, filepath.FromSlash(rel)))
	if err != nil {
		return err
	}
	sort.Strings(matches)
	fmt.Fprintf(h, "shared-glob:%s %d\n", rel, len(matches))
	for _, m := range matches {
		r, err := filepath.Rel(sharedDir, m)
		if err != nil {
			return err
		}
		if err := hashTree(h, "shared", sharedDir, r); err != nil {
			return err
		}
	}
	return nil
}

// hashTree adds every file at or under root/rel to h as "<tag>:<path> <size>\n"
// followed by its contents, in lexical order. A missing start path is recorded as
// missing rather than failing: the build reports that better than a hash would.
// Symlinks are recorded by target and never followed.
func hashTree(h hash.Hash, tag, root, rel string) error {
	start := filepath.Join(root, filepath.FromSlash(rel))
	if _, err := os.Lstat(start); errors.Is(err, fs.ErrNotExist) {
		fmt.Fprintf(h, "%s:%s missing\n", tag, rel)
		return nil
	}
	return filepath.WalkDir(start, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if d.Name() == "__pycache__" {
				return filepath.SkipDir
			}
			return nil
		}
		if strings.HasSuffix(d.Name(), ".pyc") {
			return nil
		}
		r, err := filepath.Rel(root, p)
		if err != nil {
			return err
		}
		name := tag + ":" + filepath.ToSlash(r)
		if d.Type()&fs.ModeSymlink != 0 {
			target, err := os.Readlink(p)
			if err != nil {
				return err
			}
			fmt.Fprintf(h, "%s -> %s\n", name, target)
			return nil
		}
		data, err := os.ReadFile(p)
		if err != nil {
			return err
		}
		fmt.Fprintf(h, "%s %d\n", name, len(data))
		h.Write(data)
		return nil
	})
}

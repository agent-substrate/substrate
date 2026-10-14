// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Command cpufeatures generates internal/hardware/zz_generated.cpufeatures.go
// from the gVisor cpuid source files committed under hack/third_party/gvisor.
//
// That directory is the only pin: VERSION names the gvisor.dev/gvisor module
// version the files were taken from, and SHA256SUMS lets every run check they
// are still the bytes that were fetched. Pass -fetch <version>
// (hack/update/cpufeatures.sh does so when GVISOR_VERSION is set) to refresh
// the directory from the Go module proxy before generating.
//
// Within SchemaVersionV1, a feature's slice index is its bit position in
// persisted snapshot cpu_features masks, so the generator is strictly
// append-only: existing entries in zz_generated.cpufeatures.go keep their
// indices, every non-retired entry must still exist in upstream gVisor, and
// any newly added upstream features are appended at the end. If upstream
// renames or removes a feature, add a trailing "// retired" comment to its
// entry in zz_generated.cpufeatures.go so it keeps its bit index without
// requiring upstream to still define it.
package main

import (
	"archive/zip"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

const gvisorModulePath = "gvisor.dev/gvisor"

// Files the thirdParty directory carries besides the gVisor sources.
const (
	versionFile   = "VERSION"
	checksumsFile = "SHA256SUMS"

	// maxFeatures is the capacity of internal/hardware's 4-word (256-bit)
	// featureMask within SchemaVersionV1.
	maxFeatures = 4 * 64
)

// archs are the architectures with a table, in the order the generated file
// lists them. Each one's names come from featuresFile(arch) and are emitted
// as tableVar(arch).
var archs = []string{"amd64", "arm64"}

func featuresFile(arch string) string { return "pkg/cpuid/features_" + arch + ".go" }
func tableVar(arch string) string     { return arch + "CPUFeatures" }

// thirdPartyFiles are the files fetched from gVisor and listed in SHA256SUMS.
func thirdPartyFiles() []string {
	files := []string{"LICENSE"}
	for _, arch := range archs {
		files = append(files, featuresFile(arch))
	}
	return files
}

// committedEntry is one name of a generated table. A retired entry keeps its
// bit although gVisor no longer prints the name.
type committedEntry struct {
	name    string
	retired bool
}

func main() {
	var (
		fetch     = flag.String("fetch", "", "gvisor.dev/gvisor module version to download into -gvisor-dir before generating; empty generates from the committed copies")
		gvisorDir = flag.String("gvisor-dir", "hack/third_party/gvisor", "directory holding the gVisor copies, VERSION and SHA256SUMS; relative to the working directory, so hack/update/cpufeatures.sh passes repo-rooted paths")
		outFile   = flag.String("out", "internal/hardware/zz_generated.cpufeatures.go", "generated Go file; relative to the working directory")
	)
	flag.Parse()

	if err := run(*fetch, *gvisorDir, *outFile); err != nil {
		fmt.Fprintf(os.Stderr, "cpufeatures: %v\n", err)
		os.Exit(1)
	}
}

func run(fetchVersion, gvisorDir, outFile string) error {
	if fetchVersion != "" {
		if err := populateThirdPartyGVisor(fetchVersion, gvisorDir); err != nil {
			return fmt.Errorf("fetching gVisor %s into %s: %w", fetchVersion, gvisorDir, err)
		}
	}
	if err := verifyChecksums(gvisorDir); err != nil {
		return err
	}
	pinned, err := readPinnedVersion(gvisorDir)
	if err != nil {
		return err
	}
	existing, err := parseExistingTables(outFile)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("reading existing %s: %w", outFile, err)
	}

	tables := make([][]committedEntry, len(archs))
	for i, arch := range archs {
		srcPath := filepath.Join(gvisorDir, filepath.FromSlash(featuresFile(arch)))
		src, err := os.ReadFile(srcPath)
		if err != nil {
			return fmt.Errorf("reading %s: %w", srcPath, err)
		}
		upstream, err := parseGVisorAllFeatures(featuresFile(arch), src)
		if err != nil {
			return fmt.Errorf("parsing %s: %w", srcPath, err)
		}
		tables[i], err = mergeAppendOnly(arch, existing[tableVar(arch)], upstream)
		if err != nil {
			return err
		}
	}

	rendered, err := renderGeneratedFile(gvisorModulePath+"@"+pinned, tables)
	if err != nil {
		return err
	}
	return os.WriteFile(outFile, rendered, 0o644)
}

// populateThirdPartyGVisor downloads gvisor.dev/gvisor at version from the Go
// module proxy, extracts thirdPartyFiles into gvisorDir, and writes VERSION,
// SHA256SUMS, and a minimal go.mod so root `./...` package walks skip the
// partial cpuid package.
func populateThirdPartyGVisor(version, gvisorDir string) error {
	zipBytes, err := fetchModuleZip(version)
	if err != nil {
		return err
	}
	zr, err := zip.NewReader(bytes.NewReader(zipBytes), int64(len(zipBytes)))
	if err != nil {
		return fmt.Errorf("opening module zip: %w", err)
	}
	for _, rel := range thirdPartyFiles() {
		data, err := readZipFile(zr, gvisorModulePath+"@"+version+"/"+rel)
		if err != nil {
			return err
		}
		dst := filepath.Join(gvisorDir, filepath.FromSlash(rel))
		if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
			return err
		}
		if err := os.WriteFile(dst, data, 0o644); err != nil {
			return err
		}
	}
	if err := os.WriteFile(filepath.Join(gvisorDir, versionFile), []byte(gvisorModulePath+"@"+version+"\n"), 0o644); err != nil {
		return err
	}
	if err := writeChecksums(gvisorDir); err != nil {
		return err
	}
	goModContent := "// Module boundary so root ./... walks skip partial gVisor source files.\nmodule gvisor.dev/gvisor\n"
	return os.WriteFile(filepath.Join(gvisorDir, "go.mod"), []byte(goModContent), 0o644)
}

// fetchModuleZip downloads the module zip from the first HTTP proxy in
// GOPROXY (proxy.golang.org when unset). "off" and "direct" are not proxies
// this tool can use, so a GOPROXY naming only those is an error rather than a
// silent fallback.
func fetchModuleZip(version string) ([]byte, error) {
	proxyBase := "https://proxy.golang.org"
	if env := os.Getenv("GOPROXY"); env != "" {
		proxyBase = ""
		for part := range strings.SplitSeq(env, ",") {
			part = strings.TrimSpace(part)
			if strings.HasPrefix(part, "https://") || strings.HasPrefix(part, "http://") {
				proxyBase = strings.TrimRight(part, "/")
				break
			}
		}
		if proxyBase == "" {
			return nil, fmt.Errorf("GOPROXY=%q names no HTTP proxy to fetch %s from", env, gvisorModulePath)
		}
	}
	url := fmt.Sprintf("%s/%s/@v/%s.zip", proxyBase, gvisorModulePath, version)
	client := &http.Client{Timeout: 5 * time.Minute}
	resp, err := client.Get(url)
	if err != nil {
		return nil, fmt.Errorf("fetching %s: %w", url, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("fetching %s: HTTP %d", url, resp.StatusCode)
	}
	return io.ReadAll(resp.Body)
}

func readZipFile(zr *zip.Reader, name string) ([]byte, error) {
	for _, f := range zr.File {
		if f.Name != name {
			continue
		}
		rc, err := f.Open()
		if err != nil {
			return nil, err
		}
		defer rc.Close()
		return io.ReadAll(rc)
	}
	return nil, fmt.Errorf("%s not found in module zip", name)
}

// readPinnedVersion returns the module version named by gvisorDir's VERSION
// file, which holds "gvisor.dev/gvisor@<version>".
func readPinnedVersion(gvisorDir string) (string, error) {
	path := filepath.Join(gvisorDir, versionFile)
	data, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	module, version, ok := strings.Cut(strings.TrimSpace(string(data)), "@")
	if !ok || module != gvisorModulePath || version == "" {
		return "", fmt.Errorf("%s: want %q, got %q", path, gvisorModulePath+"@<version>", strings.TrimSpace(string(data)))
	}
	return version, nil
}

// writeChecksums records the sha256 of every file in thirdPartyFiles, in the
// format sha256sum reads.
func writeChecksums(gvisorDir string) error {
	var buf bytes.Buffer
	for _, rel := range thirdPartyFiles() {
		sum, err := fileSHA256(filepath.Join(gvisorDir, filepath.FromSlash(rel)))
		if err != nil {
			return err
		}
		fmt.Fprintf(&buf, "%s  %s\n", sum, rel)
	}
	return os.WriteFile(filepath.Join(gvisorDir, checksumsFile), buf.Bytes(), 0o644)
}

// verifyChecksums checks that every file in thirdPartyFiles matches
// gvisorDir's SHA256SUMS, so the generator only ever reads the bytes a fetch
// wrote. The file is required: it is what makes a hand edit of a copy show up.
func verifyChecksums(gvisorDir string) error {
	path := filepath.Join(gvisorDir, checksumsFile)
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("reading %s (run with -fetch to populate %s): %w", path, gvisorDir, err)
	}
	want := make(map[string]string)
	for _, line := range strings.Split(strings.TrimSpace(string(data)), "\n") {
		sum, rel, ok := strings.Cut(line, "  ")
		if !ok {
			return fmt.Errorf("%s: malformed line %q", path, line)
		}
		want[rel] = sum
	}
	for _, rel := range thirdPartyFiles() {
		sum, ok := want[rel]
		if !ok {
			return fmt.Errorf("%s does not list %s", path, rel)
		}
		got, err := fileSHA256(filepath.Join(gvisorDir, filepath.FromSlash(rel)))
		if err != nil {
			return err
		}
		if got != sum {
			return fmt.Errorf("%s/%s does not match %s; refresh it with -fetch rather than editing it", gvisorDir, rel, checksumsFile)
		}
	}
	return nil
}

func fileSHA256(path string) (string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:]), nil
}

// parseGVisorAllFeatures extracts the displayName string from every entry in
// `var allFeatures = map[Feature]allFeatureInfo{ ... }` in declaration order.
func parseGVisorAllFeatures(filename string, src []byte) ([]string, error) {
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, filename, src, 0)
	if err != nil {
		return nil, err
	}
	var names []string
	for _, decl := range file.Decls {
		gen, ok := decl.(*ast.GenDecl)
		if !ok || gen.Tok != token.VAR {
			continue
		}
		for _, spec := range gen.Specs {
			vs, ok := spec.(*ast.ValueSpec)
			if !ok || len(vs.Names) != 1 || vs.Names[0].Name != "allFeatures" || len(vs.Values) != 1 {
				continue
			}
			comp, ok := vs.Values[0].(*ast.CompositeLit)
			if !ok {
				return nil, fmt.Errorf("allFeatures is not a composite literal")
			}
			for _, elt := range comp.Elts {
				kv, ok := elt.(*ast.KeyValueExpr)
				if !ok {
					return nil, fmt.Errorf("unexpected element %T in allFeatures", elt)
				}
				valComp, ok := kv.Value.(*ast.CompositeLit)
				if !ok || len(valComp.Elts) == 0 {
					return nil, fmt.Errorf("unexpected value %T in allFeatures", kv.Value)
				}
				lit, ok := valComp.Elts[0].(*ast.BasicLit)
				if !ok || lit.Kind != token.STRING {
					return nil, fmt.Errorf("unexpected displayName literal %T in allFeatures", valComp.Elts[0])
				}
				name, err := strconv.Unquote(lit.Value)
				if err != nil {
					return nil, err
				}
				names = append(names, name)
			}
		}
	}
	if len(names) == 0 {
		return nil, errors.New("var allFeatures not found or empty")
	}
	return names, nil
}

// parseExistingTables reads the string slice literals of a previously
// generated file, keyed by variable name, noting which entries carry a
// "// retired" comment on their line.
func parseExistingTables(path string) (map[string][]committedEntry, error) {
	src, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, path, src, parser.ParseComments)
	if err != nil {
		return nil, err
	}

	retiredLines := make(map[int]bool)
	for _, cg := range file.Comments {
		for _, c := range cg.List {
			if strings.TrimSpace(strings.TrimPrefix(c.Text, "//")) == "retired" {
				retiredLines[fset.Position(c.Pos()).Line] = true
			}
		}
	}

	out := make(map[string][]committedEntry)
	for _, decl := range file.Decls {
		gen, ok := decl.(*ast.GenDecl)
		if !ok || gen.Tok != token.VAR {
			continue
		}
		for _, spec := range gen.Specs {
			vs, ok := spec.(*ast.ValueSpec)
			if !ok || len(vs.Names) != 1 || len(vs.Values) != 1 {
				continue
			}
			comp, ok := vs.Values[0].(*ast.CompositeLit)
			if !ok {
				continue
			}
			var items []committedEntry
			for _, elt := range comp.Elts {
				lit, ok := elt.(*ast.BasicLit)
				if !ok || lit.Kind != token.STRING {
					continue
				}
				s, err := strconv.Unquote(lit.Value)
				if err != nil {
					return nil, err
				}
				items = append(items, committedEntry{
					name:    s,
					retired: retiredLines[fset.Position(lit.Pos()).Line],
				})
			}
			out[vs.Names[0].Name] = items
		}
	}
	return out, nil
}

// mergeAppendOnly verifies that upstream contains every non-retired feature in
// committed, preserves committed[0..len(committed)-1] in its exact order, and
// appends any new upstream features at the end.
func mergeAppendOnly(arch string, committed []committedEntry, upstream []string) ([]committedEntry, error) {
	upstreamSet := make(map[string]bool, len(upstream))
	for _, name := range upstream {
		if upstreamSet[name] {
			return nil, fmt.Errorf("upstream %s allFeatures lists %q more than once", arch, name)
		}
		upstreamSet[name] = true
	}

	entries := make([]committedEntry, 0, len(committed)+len(upstream))
	seen := make(map[string]bool, len(committed)+len(upstream))
	for i, c := range committed {
		if c.name == "" {
			return nil, fmt.Errorf("committed %s table has empty feature name at bit %d", arch, i)
		}
		if !c.retired && !upstreamSet[c.name] {
			return nil, fmt.Errorf("%s feature %q (bit %d) in committed table is missing from upstream gVisor; mark its line with `// retired` in zz_generated.cpufeatures.go if renamed or removed upstream", arch, c.name, i)
		}
		if seen[c.name] {
			return nil, fmt.Errorf("committed %s table lists %q more than once", arch, c.name)
		}
		seen[c.name] = true
		entries = append(entries, c)
	}
	for _, name := range upstream {
		if !seen[name] {
			seen[name] = true
			entries = append(entries, committedEntry{name: name})
		}
	}
	if len(entries) > maxFeatures {
		return nil, fmt.Errorf("%s table has %d features, exceeding SchemaVersionV1's %d-bit featureMask", arch, len(entries), maxFeatures)
	}
	return entries, nil
}

// renderGeneratedFile renders one table per arch, in archs order.
func renderGeneratedFile(sourceDesc string, tables [][]committedEntry) ([]byte, error) {
	var buf bytes.Buffer
	buf.WriteString(`// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Code generated by hack/update/cpufeatures.sh; DO NOT EDIT.
// Source: ` + sourceDesc + ` (pkg/cpuid/features_amd64.go, pkg/cpuid/features_arm64.go)

package hardware

// A feature name's index in these tables is its bit in an encoded cpu_features
// mask. Within SchemaVersionV1 the tables are append-only: existing indices
// are preserved and new upstream gVisor features are appended at the end. If
// upstream renames or removes a feature, mark its line with "// retired".
var (
`)
	for i, arch := range archs {
		if i > 0 {
			buf.WriteString("\n")
		}
		fmt.Fprintf(&buf, "\t%s = []string{\n", tableVar(arch))
		for _, e := range tables[i] {
			if e.retired {
				fmt.Fprintf(&buf, "\t\t%q, // retired\n", e.name)
			} else {
				fmt.Fprintf(&buf, "\t\t%q,\n", e.name)
			}
		}
		buf.WriteString("\t}\n")
	}
	buf.WriteString(")\n")
	return format.Source(buf.Bytes())
}

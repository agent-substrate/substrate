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

package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestGenerate(t *testing.T) {
	repo := t.TempDir()
	out := filepath.Join(t.TempDir(), "content")

	mustWrite := func(rel, content string) {
		t.Helper()
		p := filepath.Join(repo, rel)
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}

	mustWrite("docs/api-style-guide.md", "---\ndocsite: false\n---\n\n# API Style Guide\n")
	mustWrite("docs/dev/internal.md", "# Internal Dev Guide\n")
	mustWrite("docs/concepts/architecture.md", "---\nweight: 1\nstatus: planned\n---\n\n# Architecture\n\nHow the components fit together.\n")
	mustWrite("demos/counter/README.md", "# `counter` Demo\n\nStateful counter walkthrough.\n")
	mustWrite("demos/hidden/README.md", "---\ndocsite: false\n---\n\n# Hidden Demo\n")

	if err := generate(repo, out); err != nil {
		t.Fatalf("generate() unexpected error: %v", err)
	}

	if _, err := os.Stat(filepath.Join(out, "docs", "api-style-guide.md")); !os.IsNotExist(err) {
		t.Errorf("expected docsite:false file to be omitted, got err=%v", err)
	}
	if _, err := os.Stat(filepath.Join(out, "docs", "dev")); !os.IsNotExist(err) {
		t.Errorf("expected docs/dev to be omitted, got err=%v", err)
	}
	if _, err := os.Stat(filepath.Join(out, "docs", "tutorials", "hidden.md")); !os.IsNotExist(err) {
		t.Errorf("expected docsite:false demo to be omitted, got err=%v", err)
	}

	archBytes, err := os.ReadFile(filepath.Join(out, "docs", "concepts", "architecture.md"))
	if err != nil {
		t.Fatalf("reading generated architecture.md: %v", err)
	}
	arch := string(archBytes)
	for _, want := range []string{
		"title: Architecture",
		"linkTitle: Architecture",
		"description: How the components fit together.",
		"repo_source: docs/concepts/architecture.md",
		"status: planned",
		"{{% include-file %}}",
	} {
		if !strings.Contains(arch, want) {
			t.Errorf("generated architecture.md missing %q:\n%s", want, arch)
		}
	}

	counterBytes, err := os.ReadFile(filepath.Join(out, "docs", "tutorials", "counter.md"))
	if err != nil {
		t.Fatalf("reading generated tutorials/counter.md: %v", err)
	}
	counter := string(counterBytes)
	for _, want := range []string{
		"title: counter Demo",
		"description: Stateful counter walkthrough.",
		"repo_source: demos/counter/README.md",
	} {
		if !strings.Contains(counter, want) {
			t.Errorf("generated tutorials/counter.md missing %q:\n%s", want, counter)
		}
	}
}

func TestGenerateRejectsUnclassifiedRootDoc(t *testing.T) {
	repo := t.TempDir()
	out := filepath.Join(t.TempDir(), "content")

	p := filepath.Join(repo, "docs", "stray.md")
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, []byte("# Stray Doc\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	err := generate(repo, out)
	if err == nil || !strings.Contains(err.Error(), "docs/stray.md") {
		t.Fatalf("generate() error = %v, want error mentioning docs/stray.md", err)
	}
}

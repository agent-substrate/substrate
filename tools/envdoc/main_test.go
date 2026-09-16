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

const fixture = `package owner
import settings "github.com/agent-substrate/substrate/internal/env"
const name = "SECRET_SETTING"
var setting = settings.Var[string]{
 Name: name, Default: "declared",
 Description: "Description <tag> with *markup*",
}
`

func TestGenerateAndCheck(t *testing.T) {
	root := t.TempDir()
	for _, dir := range []string{"cmd", "internal", "pkg", "docs"} {
		if err := os.Mkdir(filepath.Join(root, dir), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	write := func(path, content string) {
		t.Helper()
		if err := os.WriteFile(filepath.Join(root, path), []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	write("cmd/settings.go", fixture)
	write("internal/identity.go", strings.ReplaceAll(fixture, `Name: name`, `Name: "NODE_NAME"`))
	write("cmd/ignored_test.go", "not valid Go; tests are outside the registry")
	write("pkg/unrelated.go", "not valid Go; has no env import")
	t.Setenv("SECRET_SETTING", "live-secret-must-not-appear")
	if err := run(root, false); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, "docs/environment-variables.md")
	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	output := string(content)
	for _, want := range []string{"SECRET_SETTING", "declared", "&lt;tag&gt;", `\*markup\*`, "## Operator configuration", "## System-provided values", "NODE_NAME", "../cmd/settings.go#L"} {
		if !strings.Contains(output, want) {
			t.Errorf("output missing %q", want)
		}
	}
	if strings.Contains(output, "live-secret") {
		t.Fatal("output contains values other than declaration metadata")
	}
	if strings.Index(output, "SECRET_SETTING") > strings.Index(output, "## System-provided values") {
		t.Fatal("operator setting appears in system section")
	}
	t.Setenv("SECRET_SETTING", "a-different-live-secret")
	if err := run(root, true); err != nil {
		t.Fatal(err)
	}
	write("docs/environment-variables.md", "stale")
	if err := run(root, true); err == nil || !strings.Contains(err.Error(), "stale") {
		t.Fatalf("check error = %v", err)
	}
	current, err := os.ReadFile(path)
	if err != nil || string(current) != "stale" {
		t.Fatal("check mode modified the file")
	}
	write("pkg/other.go", strings.ReplaceAll(fixture, `Default: "declared"`, `Default: "other-default"`))
	entries, err := collect(root)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 3 || entries[0].fields["Default"] != "declared" || entries[1].fields["Default"] != "other-default" {
		t.Fatalf("consumer defaults = %#v", entries)
	}

}

func TestRejectDynamicOrMissingMetadata(t *testing.T) {
	for _, tc := range []struct{ name, source, want string }{
		{"dynamic default", strings.ReplaceAll(fixture, `Default: "declared"`, `Default: os.Getenv("SECRET")`), "must be a string/boolean literal"},
		{"missing description", strings.ReplaceAll(fixture, `Description: "Description <tag> with *markup*",`, ""), "missing Description"},
		{"constant cycle", strings.ReplaceAll(fixture, `const name = "SECRET_SETTING"`, `const name = name`), "must be a string/boolean literal"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := declarations("fixture.go", []byte(tc.source))
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error = %v, want %q", err, tc.want)
			}
		})
	}
}

func TestBoolDefaultAndImportIdentity(t *testing.T) {
	source := strings.ReplaceAll(strings.ReplaceAll(fixture, "Var[string]", "Var[bool]"), `Default: "declared"`, `Default: false`)
	entries, err := declarations("fixture.go", []byte(source))
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].typ != "bool" || entries[0].fields["Default"] != "false" {
		t.Fatalf("entries = %#v", entries)
	}
	entries, err = declarations("fixture.go", []byte(strings.ReplaceAll(fixture, envPackage, "example.com/unrelated")))
	if err != nil || len(entries) != 0 {
		t.Fatalf("unrelated declarations = %#v, error = %v", entries, err)
	}
}

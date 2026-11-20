// Copyright 2026 The Agent Substrate Authors
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
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

func TestTablesUpToDate(t *testing.T) {
	committedDir := "../../internal/selfseccomp"
	tmp := t.TempDir()
	cmd := exec.Command("go", "run", ".", "-out", tmp)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("go run . -out %s: %v\n%s", tmp, err, out)
	}
	for _, name := range []string{"amd64_syscallnum.go", "arm64_syscallnum.go"} {
		want, err := os.ReadFile(filepath.Join(tmp, name))
		if err != nil {
			t.Fatal(err)
		}
		got, err := os.ReadFile(filepath.Join(committedDir, name))
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, want) {
			t.Errorf("%s is out of date: run go generate ./hack/selfseccomp", name)
		}
	}
}

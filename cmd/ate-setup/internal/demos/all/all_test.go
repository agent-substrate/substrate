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

package all_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/config"
	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/demos"
	_ "github.com/agent-substrate/substrate/cmd/ate-setup/internal/demos/all"
)

// nonDemoPackages are the directories under internal/demos that are support
// packages rather than demos.
var nonDemoPackages = map[string]bool{
	"all":      true,
	"demotest": true,
}

// TestEveryDemoPackageIsLinked walks the demo directories and checks that each
// one produced a registration. A demo package that no one imports compiles
// fine and then quietly has no subcommand, which this catches.
func TestEveryDemoPackageIsLinked(t *testing.T) {
	root, err := config.RepoRoot()
	if err != nil {
		t.Fatalf("RepoRoot: %v", err)
	}
	entries, err := os.ReadDir(filepath.Join(root, "cmd/ate-setup/internal/demos"))
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}

	packages := 0
	for _, entry := range entries {
		if !entry.IsDir() || nonDemoPackages[entry.Name()] {
			continue
		}
		packages++
	}

	if registered := len(demos.All()); registered != packages {
		t.Errorf("%d demo packages on disk but %d registered demos %v; is one missing from package all?",
			packages, registered, names())
	}
}

func names() []string {
	var out []string
	for _, d := range demos.All() {
		out = append(out, d.Name())
	}
	return out
}

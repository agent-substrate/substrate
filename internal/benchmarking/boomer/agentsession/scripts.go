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

package agentsession

import (
	"embed"
	"fmt"
	"io/fs"
	"sort"
	"strings"
)

// DefaultScript is the variant a run uses when --agentsession-script is
// unset: the 20-step coding session.
const DefaultScript = "coding-session"

// scriptFS holds the built-in script variants. A file scripts/<name>.yaml is
// selectable as --agentsession-script=<name>; TestEmbeddedScriptsAreValid
// keeps every one of them loadable.
//
//go:embed scripts/*.yaml
var scriptFS embed.FS

// Names lists the built-in script variants.
func Names() []string {
	entries, err := fs.ReadDir(scriptFS, "scripts")
	if err != nil {
		return nil
	}
	var names []string
	for _, e := range entries {
		if n, ok := strings.CutSuffix(e.Name(), ".yaml"); ok {
			names = append(names, n)
		}
	}
	sort.Strings(names)
	return names
}

// Load returns the built-in script variant called name. The file's name
// field must agree with the file name, so a knob value always matches what
// the stats rows and logs report.
func Load(name string) (*Script, error) {
	data, err := scriptFS.ReadFile("scripts/" + name + ".yaml")
	if err != nil {
		return nil, fmt.Errorf("no built-in agent-session script %q (have %s)", name, strings.Join(Names(), ", "))
	}
	s, err := Decode(data)
	if err != nil {
		return nil, fmt.Errorf("built-in script %q: %w", name, err)
	}
	if s.Name != name {
		return nil, fmt.Errorf("built-in script file %q names itself %q", name, s.Name)
	}
	return s, nil
}

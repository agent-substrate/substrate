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

// Package render expands the demos' *.yaml.tmpl files.
//
// The install scripts drove these through sed with two distinct behaviors:
// substitute a ${PLACEHOLDER} with a value, or delete the whole line when the
// placeholder is not in play. Both are reproduced here so the rendered
// manifests stay byte-identical to what the shell pipeline produced.
package render

import (
	"fmt"
	"os"
	"strings"
)

// Template renders a *.yaml.tmpl file.
//
// values maps a placeholder name (without the ${} wrapper) to its replacement.
// Any placeholder named in drop causes every line mentioning it to be removed,
// matching sed's `/\${NAME}/d`. A placeholder that appears in neither map is
// left untouched, so an unexpected template variable surfaces as invalid YAML
// rather than silently disappearing.
func Template(path string, values map[string]string, drop []string) ([]byte, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("while reading %s: %w", path, err)
	}
	return Expand(string(raw), values, drop), nil
}

// Expand applies the substitution and line-deletion rules to a template body.
func Expand(body string, values map[string]string, drop []string) []byte {
	dropTokens := make([]string, 0, len(drop))
	for _, name := range drop {
		dropTokens = append(dropTokens, token(name))
	}

	replacements := make([]string, 0, len(values)*2)
	for name, value := range values {
		replacements = append(replacements, token(name), value)
	}
	replacer := strings.NewReplacer(replacements...)

	// Preserve the trailing newline: sed emits a line-oriented stream, and
	// splitting on "\n" would otherwise turn a final newline into an empty
	// trailing document.
	trailingNewline := strings.HasSuffix(body, "\n")
	lines := strings.Split(strings.TrimSuffix(body, "\n"), "\n")

	kept := make([]string, 0, len(lines))
	for _, line := range lines {
		if containsAny(line, dropTokens) {
			continue
		}
		kept = append(kept, replacer.Replace(line))
	}

	out := strings.Join(kept, "\n")
	if trailingNewline {
		out += "\n"
	}
	return []byte(out)
}

// token wraps a placeholder name in the ${...} form used by the templates.
func token(name string) string {
	return "${" + name + "}"
}

func containsAny(line string, tokens []string) bool {
	for _, t := range tokens {
		if strings.Contains(line, t) {
			return true
		}
	}
	return false
}

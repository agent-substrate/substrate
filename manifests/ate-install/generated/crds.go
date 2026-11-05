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

// Package generated embeds the CRD manifests next to it, for tools that need
// to read them (e.g. apitool).
package generated

import "embed"

//go:embed ate.dev_*.yaml
var crdFiles embed.FS

// CRDs returns each CRD manifest's raw text.
func CRDs() ([]string, error) {
	entries, err := crdFiles.ReadDir(".")
	if err != nil {
		return nil, err
	}
	crds := make([]string, 0, len(entries))
	for _, e := range entries {
		text, err := crdFiles.ReadFile(e.Name())
		if err != nil {
			return nil, err
		}
		crds = append(crds, string(text))
	}
	return crds, nil
}

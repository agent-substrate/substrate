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

// Package examples holds a hand-written example of each API resource, one
// protojson file per resource named after its message, e.g. Actor.json. The
// doc site shows each beside the resource's fields. Tests check that every
// resource has an example and that each example parses as its message, so a
// renamed or removed field fails them.
package examples

import (
	"embed"
	"strings"
)

//go:embed *.json
var files embed.FS

// ByResource returns each example's text keyed by the resource's message
// name. For example, Actor.json's text under "Actor".
func ByResource() (map[string]string, error) {
	entries, err := files.ReadDir(".")
	if err != nil {
		return nil, err
	}
	examples := make(map[string]string, len(entries))
	for _, e := range entries {
		text, err := files.ReadFile(e.Name())
		if err != nil {
			return nil, err
		}
		examples[strings.TrimSuffix(e.Name(), ".json")] = string(text)
	}
	return examples, nil
}

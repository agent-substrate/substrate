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

// Package bootid reads the node's boot ID.
package bootid

import (
	"fmt"
	"os"
	"strings"
)

// Path is where Linux exposes the boot ID: a random UUID generated once per
// boot, so it changes on every reboot, clean or not, and on nothing else. It
// is not namespaced, so a pod reads the node's.
const Path = "/proc/sys/kernel/random/boot_id"

// Read returns the boot ID read from path. It is never empty, so a record that
// has none never matches it.
func Read(path string) (string, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("while reading the boot ID: %w", err)
	}
	id := strings.TrimSpace(string(b))
	if id == "" {
		return "", fmt.Errorf("boot ID file %s is empty", path)
	}
	return id, nil
}

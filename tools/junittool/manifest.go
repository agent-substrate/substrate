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
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// errDuplicateEntry is returned when a path is already recorded. Registration
// is the only point where both runs are still visible; after the overwrite the
// first has left no trace.
var errDuplicateEntry = errors.New(
	"already registered by an earlier run; give each run its own path, " +
		"or the second overwrites the first and only the second is verified")

// readManifest returns the recorded paths, deduplicated, in first-seen order.
// Duplicates are collapsed, not rejected: a run repeated locally records its
// path again. Rejecting one is register's job, where a repeat and a collision
// are still distinguishable.
func readManifest(path string) ([]string, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading manifest: %w", err)
	}
	return dedupe(strings.Split(string(raw), "\n")), nil
}

// dedupe trims, drops blanks, and keeps the first occurrence of each entry.
func dedupe(lines []string) []string {
	seen := map[string]bool{}
	var out []string
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" || seen[line] {
			continue
		}
		seen[line] = true
		out = append(out, line)
	}
	return out
}

// register appends entry to the manifest at path, creating it if absent. With
// rejectDuplicate, a repeat is an error rather than a no-op. Callers register
// before running, so a run killed part-way still has a record, and verify
// reports its file as missing.
func register(path, entry string, rejectDuplicate bool) error {
	entry = strings.TrimSpace(entry)
	if entry == "" {
		return errors.New("refusing to register an empty entry")
	}
	if strings.ContainsAny(entry, "\n\r") {
		return fmt.Errorf("entry %q contains a newline: one path per line", entry)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("creating manifest directory: %w", err)
	}

	existing, err := os.ReadFile(path)
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("reading manifest: %w", err)
	}
	for _, e := range dedupe(strings.Split(string(existing), "\n")) {
		if e != entry {
			continue
		}
		if rejectDuplicate {
			return fmt.Errorf("%s: %w", entry, errDuplicateEntry)
		}
		return nil
	}

	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return fmt.Errorf("opening manifest: %w", err)
	}
	defer f.Close()
	if _, err := fmt.Fprintln(f, entry); err != nil {
		return fmt.Errorf("appending to manifest: %w", err)
	}
	return nil
}

// defaultManifest puts the manifest beside the JUnit file, so runs sharing an
// artifact directory share a manifest.
func defaultManifest(entry string) string {
	return filepath.Join(filepath.Dir(entry), "expected-junit.txt")
}

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

// Package startupsweep provides a registry for cleaning up files and
// directories orphaned by a previous crash. Callers register (dir, glob,
// removeFn) tuples at init time; a single Sweep call removes all matches.
package startupsweep

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
)

type entry struct {
	label    string
	dir      string
	pattern  string
	removeFn func(string) error
}

// Sweeper accumulates glob-based cleanup tasks and runs them at startup.
type Sweeper struct {
	entries []entry
}

// New returns an empty Sweeper.
func New() *Sweeper { return &Sweeper{} }

// Add calls fn with the Sweeper so the caller can register its entries.
func (s *Sweeper) Add(fn func(*Sweeper)) { fn(s) }

// Register adds a cleanup entry: all paths matching filepath.Join(dir, pattern)
// will be passed to removeFn during Sweep. label is used in the summary log
// line emitted when at least one match is removed. Use os.Remove for plain
// files and os.RemoveAll (or a writable variant) for directories.
// Register is deliberately a no-op on a nil Sweeper.
func (s *Sweeper) Register(label, dir, pattern string, removeFn func(string) error) {
	if s == nil {
		return
	}
	s.entries = append(s.entries, entry{label, dir, pattern, removeFn})
}

// Sweep removes all registered orphaned paths. Each match is logged before
// removal. Failures are logged as warnings and do not abort the sweep;
// missing files are silently skipped.
func (s *Sweeper) Sweep(ctx context.Context) {
	for _, e := range s.entries {
		matches, err := filepath.Glob(filepath.Join(e.dir, e.pattern))
		if err != nil {
			// filepath.Glob only errors on malformed patterns — treat as a bug.
			slog.WarnContext(ctx, "Startup sweep: invalid glob pattern",
				slog.String("dir", e.dir), slog.String("pattern", e.pattern), slog.Any("err", err))
			continue
		}
		swept := 0
		for _, m := range matches {
			slog.InfoContext(ctx, "Startup sweep removing orphan", slog.String("path", m))
			if err := e.removeFn(m); err != nil && !errors.Is(err, os.ErrNotExist) {
				slog.WarnContext(ctx, "Startup sweep failed to remove orphan",
					slog.String("path", m), slog.Any("err", err))
			} else {
				swept++
			}
		}
		if swept > 0 {
			slog.InfoContext(ctx, "Startup sweep removed orphans",
				slog.String("label", e.label), slog.Int("count", swept))
		}
	}
}

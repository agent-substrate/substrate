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

package filecache

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// SweepStats reports what a SweepDebris pass removed.
type SweepStats struct {
	// TmpRemoved counts removed tmp/ children (unfinished fetches).
	TmpRemoved int
	// RetiredRemoved counts removed .rm-* dirs (interrupted evictions).
	RetiredRemoved int
}

// SweepDebris removes crash debris: everything under tmp/ (fetches a crash
// cut short) and every .rm-* dir at the store root (evictions that renamed
// but never removed). It must run once at startup, before the store serves
// requests, and not again: a later sweep would delete the working
// directories of in-flight fetches. No periodic schedule is needed either —
// debris only appears at crash or eviction time, and eviction passes retry
// leftover .rm-* removals themselves.
//
// Removal failures are joined and reported after the sweep visits
// everything, so one bad path does not shadow the rest; published entries
// are never touched.
func (s *Store) SweepDebris(ctx context.Context) (SweepStats, error) {
	var stats SweepStats
	var errs []error

	tmpChildren, err := os.ReadDir(s.tmpDir())
	if err != nil {
		errs = append(errs, fmt.Errorf("while listing tmp dir: %w", err))
	}
	for _, child := range tmpChildren {
		if err := ctx.Err(); err != nil {
			return stats, err
		}
		if err := os.RemoveAll(filepath.Join(s.tmpDir(), child.Name())); err != nil {
			errs = append(errs, fmt.Errorf("while removing tmp debris %q: %w", child.Name(), err))
			continue
		}
		stats.TmpRemoved++
	}

	rootChildren, err := os.ReadDir(s.root)
	if err != nil {
		errs = append(errs, fmt.Errorf("while listing store root: %w", err))
	}
	for _, child := range rootChildren {
		if err := ctx.Err(); err != nil {
			return stats, err
		}
		if !strings.HasPrefix(child.Name(), rmPrefix) {
			continue
		}
		if err := os.RemoveAll(filepath.Join(s.root, child.Name())); err != nil {
			errs = append(errs, fmt.Errorf("while removing retired entry %q: %w", child.Name(), err))
			continue
		}
		stats.RetiredRemoved++
	}

	return stats, errors.Join(errs...)
}

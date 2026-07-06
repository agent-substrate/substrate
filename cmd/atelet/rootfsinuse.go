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

	"github.com/agent-substrate/substrate/internal/ateompath"
)

// overlayLowerInUse reports the rootfs-cache lowerDirs currently referenced by
// a live actor on this node, by scanning every per-container overlay-lower
// marker under the actors tree. atelet writes a marker before ateom mounts the
// overlay and removes it only after teardown (unmount), so a present marker is
// a conservative, restart-safe signal that the lowerdir is mounted: it is
// derived purely from on-disk state and survives atelet restarts (unlike an
// in-memory refcount, which would be lost while the mount lives on in ateom's
// mount namespace).
//
// It is wired into the rootfs cache via rootfscache.WithInUseFunc so eviction
// never os.RemoveAll's a lowerdir backing a running actor's overlay mount
// (which the kernel forbids modifying while mounted).
func overlayLowerInUse() (map[string]bool, error) {
	pattern := filepath.Join(ateompath.ActorsDir(), "*", "bundles", "*", "overlay-lower")
	matches, err := filepath.Glob(pattern)
	if err != nil {
		return nil, fmt.Errorf("globbing overlay-lower markers: %w", err)
	}

	inUse := make(map[string]bool, len(matches))
	for _, m := range matches {
		data, err := os.ReadFile(m)
		if err != nil {
			// A marker removed between glob and read simply means that actor
			// finished tearing down; treat it as no longer in use.
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			return nil, fmt.Errorf("reading overlay-lower marker %s: %w", m, err)
		}
		if lower := strings.TrimSpace(string(data)); lower != "" {
			inUse[lower] = true
		}
	}
	return inUse, nil
}

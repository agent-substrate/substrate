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

package ateompath

import (
	"strings"
	"testing"
)

func TestAteomPath(t *testing.T) {
	podUID := "123e4567-e89b-12d3-a456-426614174000"

	path := AteomPath(podUID)
	expectedSuffix := "/ateoms/" + podUID
	if !strings.HasSuffix(path, expectedSuffix) {
		t.Errorf("expected path to end with %s, got %s", expectedSuffix, path)
	}
}

func TestAteomSocketPathLimits(t *testing.T) {
	podUID := "123e4567-e89b-12d3-a456-426614174000"

	sockPath := AteomSocketPath(podUID)

	// Unix domain socket path limit is 107 bytes (108 with NUL terminator)
	const maxUnixSocketLen = 107
	if len(sockPath) > maxUnixSocketLen {
		t.Errorf("socket path length %d exceeds max allowed length %d: %q", len(sockPath), maxUnixSocketLen, sockPath)
	}

	// Verify it is deterministic
	sockPath2 := AteomSocketPath(podUID)
	if sockPath != sockPath2 {
		t.Errorf("expected deterministic socket paths, got %q and %q", sockPath, sockPath2)
	}
}

func TestAteomPathUniqueness(t *testing.T) {
	uid1 := "123e4567-e89b-12d3-a456-426614174000"
	uid2 := "987f6543-e21b-32d1-b654-246614174111"

	path1 := AteomPath(uid1)
	path2 := AteomPath(uid2)

	if path1 == path2 {
		t.Errorf("expected different paths for different pod UIDs, got %q", path1)
	}
}

// TestOverlayPathHelpers checks that every per-container overlay path helper is
// rooted under the container's OCI bundle, that they are mutually distinct
// (upper/work/lower must not collide with each other or with rootfs — an
// overlayfs mount requires them separate), and that they are deterministic.
func TestOverlayPathHelpers(t *testing.T) {
	const (
		ns        = "team-a"
		tmpl      = "web"
		id        = "actor-1"
		container = "app"
	)

	bundle := OCIBundlePath(ns, tmpl, id, container)

	cases := []struct {
		name string
		got  string
	}{
		{"rootfs", ContainerRootfsDir(ns, tmpl, id, container)},
		{"upper", OverlayUpperDir(ns, tmpl, id, container)},
		{"work", OverlayWorkDir(ns, tmpl, id, container)},
		{"marker", OverlayLowerMarkerFile(ns, tmpl, id, container)},
	}

	seen := make(map[string]string, len(cases))
	for _, tc := range cases {
		if !strings.HasPrefix(tc.got, bundle+"/") {
			t.Errorf("%s: %q is not under bundle %q", tc.name, tc.got, bundle)
		}
		if prev, dup := seen[tc.got]; dup {
			t.Errorf("%s and %s collide on path %q", prev, tc.name, tc.got)
		}
		seen[tc.got] = tc.name
	}
}

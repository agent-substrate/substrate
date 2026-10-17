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

package nodepath

import (
	"strings"
	"testing"
)

func TestWorkerPath(t *testing.T) {
	podUID := "123e4567-e89b-12d3-a456-426614174000"

	path := WorkerPath(podUID)
	expectedSuffix := "/workers/" + podUID
	if !strings.HasSuffix(path, expectedSuffix) {
		t.Errorf("expected path to end with %s, got %s", expectedSuffix, path)
	}
}

func TestWorkerSocketPathLimits(t *testing.T) {
	podUID := "123e4567-e89b-12d3-a456-426614174000"

	sockPath := WorkerSocketPath(podUID)

	// Unix domain socket path limit is 107 bytes (108 with NUL terminator)
	const maxUnixSocketLen = 107
	if len(sockPath) > maxUnixSocketLen {
		t.Errorf("socket path length %d exceeds max allowed length %d: %q", len(sockPath), maxUnixSocketLen, sockPath)
	}

	// Verify it is deterministic
	sockPath2 := WorkerSocketPath(podUID)
	if sockPath != sockPath2 {
		t.Errorf("expected deterministic socket paths, got %q and %q", sockPath, sockPath2)
	}
}

func TestAteletOTLPSocketPath(t *testing.T) {
	sockPath := AteletOTLPSocketPath()

	// Unix domain socket path limit is 107 bytes (108 with NUL terminator)
	const maxUnixSocketLen = 107
	if len(sockPath) > maxUnixSocketLen {
		t.Errorf("socket path length %d exceeds max allowed length %d: %q", len(sockPath), maxUnixSocketLen, sockPath)
	}

	// It must sit under BasePath: that is the host directory already mounted at
	// the same path into atelet and into every worker pod, which is the whole
	// reason the relay needs no new volume.
	if !strings.HasPrefix(sockPath, BasePath+"/") {
		t.Errorf("AteletOTLPSocketPath() = %q, want it under %q so worker and atelet see the same file", sockPath, BasePath)
	}

	// Node-scoped, so it must not collide with any per-pod worker socket.
	if other := WorkerSocketPath("123e4567-e89b-12d3-a456-426614174000"); sockPath == other {
		t.Errorf("AteletOTLPSocketPath() collides with WorkerSocketPath: %q", sockPath)
	}
}

func TestWorkerPathUniqueness(t *testing.T) {
	uid1 := "123e4567-e89b-12d3-a456-426614174000"
	uid2 := "987f6543-e21b-32d1-b654-246614174111"

	path1 := WorkerPath(uid1)
	path2 := WorkerPath(uid2)

	if path1 == path2 {
		t.Errorf("expected different paths for different pod UIDs, got %q", path1)
	}
}

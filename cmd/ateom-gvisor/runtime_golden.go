//go:build linux

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
	"fmt"
	"github.com/agent-substrate/substrate/internal/proto/ateompb"
	"os"
	"path/filepath"
)

const runtimeGoldenFile = "runtime-golden"
const runtimeGoldenFormat = "gvisor-pause-only-v1\n"

func readRuntimeGolden(dir string) (bool, error) {
	b, err := os.ReadFile(filepath.Join(dir, runtimeGoldenFile))
	if os.IsNotExist(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if string(b) != runtimeGoldenFormat {
		return false, fmt.Errorf("invalid runtime golden marker")
	}
	return true, nil
}

// isRuntimeGolden reads the capture phase under the session publication lock.
func (s *AteomService) isRuntimeGolden(actorUID string) bool {
	s.actorsMu.RLock()
	defer s.actorsMu.RUnlock()
	h := s.actors[actorUID]
	return h != nil && h.session != nil && h.session.runtimeGolden
}

// A pause-only checkpoint has no application state. Its applications start
// fresh after the sandbox restore and use the destination's mounted volumes.
func restoreApplicationMemory(fidelity ateompb.SnapshotFidelity, runtimeGolden bool) (bool, error) {
	if runtimeGolden && fidelity != ateompb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY {
		return false, fmt.Errorf("runtime golden restore requires MEMORY fidelity")
	}
	return fidelity == ateompb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY && !runtimeGolden, nil
}

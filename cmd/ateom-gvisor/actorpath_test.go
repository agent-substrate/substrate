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
	"os"
	"path/filepath"
	"testing"

	"github.com/agent-substrate/substrate/internal/proto/ateompb"
)

func TestResetRunscStateAndPidFileDirs(t *testing.T) {
	actorDirs := &ateompb.ActorDirs{RootDir: t.TempDir()}
	if err := os.MkdirAll(filepath.Join(runscStateDir(actorDirs), "stale"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := resetRunscStateAndPidFileDirs(actorDirs); err != nil {
		t.Fatalf("resetRunscStateAndPidFileDirs() = %v", err)
	}
	for _, dir := range []string{runscStateDir(actorDirs), pidFileDir(actorDirs)} {
		if entries, err := os.ReadDir(dir); err != nil || len(entries) != 0 {
			t.Errorf("ReadDir(%q) = %v, %v; want an empty directory", dir, entries, err)
		}
	}
}

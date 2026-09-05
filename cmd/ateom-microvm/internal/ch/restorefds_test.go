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

package ch

import (
	"context"
	"encoding/json"
	"net/http"
	"path/filepath"
	"testing"
)

// TestRestoreWithNetFDsPinsCopyMode asserts the vm.restore body names Copy
// explicitly. CH defaults an unset memory_restore_mode to Copy today, but that
// default is a serde attribute we do not control, and a flip to OnDemand would
// take every actor down (the prefault storm starves the guest past readyz). So
// the value has to be on the wire, not inherited.
func TestRestoreWithNetFDsPinsCopyMode(t *testing.T) {
	client, fake := startFakeCH(t)
	srcDir := filepath.Join(t.TempDir(), "restore-state")

	// No nets: RestoreWithNetFDs then sends no SCM_RIGHTS, so the plain HTTP
	// server in startFakeCH can parse the request.
	if err := client.RestoreWithNetFDs(context.Background(), srcDir, nil); err != nil {
		t.Fatalf("RestoreWithNetFDs: %v", err)
	}

	var got *recordedReq
	for _, r := range fake.recorded() {
		if r.path == "/api/v1/vm.restore" {
			got = &r
			break
		}
	}
	if got == nil {
		t.Fatalf("no vm.restore request recorded, got %+v", fake.recorded())
	}
	if got.method != http.MethodPut {
		t.Errorf("vm.restore method = %s, want %s", got.method, http.MethodPut)
	}

	var cfg struct {
		SourceURL         string `json:"source_url"`
		MemoryRestoreMode string `json:"memory_restore_mode"`
	}
	if err := json.Unmarshal([]byte(got.body), &cfg); err != nil {
		t.Fatalf("vm.restore body not JSON: %v (%q)", err, got.body)
	}
	if cfg.SourceURL != SnapshotURL(srcDir) {
		t.Errorf("source_url = %q, want %q", cfg.SourceURL, SnapshotURL(srcDir))
	}
	if cfg.MemoryRestoreMode != memRestoreCopy {
		t.Errorf("memory_restore_mode = %q, want %q (must be sent, not left to CH's default)",
			cfg.MemoryRestoreMode, memRestoreCopy)
	}
}

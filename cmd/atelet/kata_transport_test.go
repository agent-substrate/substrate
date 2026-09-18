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
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/agent-substrate/substrate/internal/proto/ateompb"
)

func TestValidateDownloadedKataTransport(t *testing.T) {
	root := t.TempDir()
	state := []byte("qemu-state")
	statePath := filepath.Join(root, "vm.state")
	if err := os.WriteFile(statePath, state, 0o600); err != nil {
		t.Fatal(err)
	}
	digest, err := fileSHA256(statePath)
	if err != nil {
		t.Fatal(err)
	}
	manifest := kataTransportManifest{
		SchemaVersion: kataTransportSchemaVersion,
		OperationID:   "8d830f4d-0813-44ef-8495-3d95af80e27c", Profile: kataCRProfileQEMU,
		RuntimeFingerprint: strings.Repeat("a", 64), SourceNode: "node-a",
		Files: []kataTransportFile{{Path: "vm.state", Size: int64(len(state)), SHA256: digest}},
	}
	b, err := json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, kataFileIndexName), b, 0o600); err != nil {
		t.Fatal(err)
	}
	rec := &sandboxAssetsRecord{SandboxClass: "kata", SnapshotFiles: []string{"vm.state", kataFileIndexName}}
	if err := validateDownloadedKataTransport(root, rec, "node-a"); err != nil {
		t.Fatal(err)
	}
	if err := validateDownloadedKataTransport(root, rec, "node-b"); err == nil {
		t.Fatal("cross-node Kata restore accepted")
	}
	if err := os.WriteFile(statePath, []byte("corrupt"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := validateDownloadedKataTransport(root, rec, "node-a"); err == nil {
		t.Fatal("corrupt downloaded closure accepted")
	}
}

func TestValidateDownloadedKataTransportLegacyCompatibility(t *testing.T) {
	rec := &sandboxAssetsRecord{SandboxClass: "kata", SnapshotFiles: []string{"vm.state"}}
	if err := validateDownloadedKataTransport(t.TempDir(), rec, "node-a"); err != nil {
		t.Fatalf("legacy snapshot rejected: %v", err)
	}
	rec.SnapshotFiles = append(rec.SnapshotFiles, kataFileIndexName)
	if err := validateDownloadedKataTransport(t.TempDir(), rec, ""); err == nil {
		t.Fatalf("v1 snapshot accepted without node identity: %+v", rec)
	}
}

func TestWriteKataTransport(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "vm.state"), []byte("state"), 0o600); err != nil {
		t.Fatal(err)
	}
	operationID := "8d830f4d-0813-44ef-8495-3d95af80e27c"
	compatibility := &ateompb.RuntimeCompatibility{Profile: kataCRProfileQEMU, RuntimeFingerprint: strings.Repeat("b", 64)}
	if err := writeKataTransport(root, operationID, "node-a", compatibility, []string{"vm.state"}); err != nil {
		t.Fatal(err)
	}
	rec := &sandboxAssetsRecord{SandboxClass: "kata", SnapshotFiles: []string{"vm.state", kataFileIndexName}}
	manifest, err := readValidatedKataTransport(root, rec, "node-a")
	if err != nil {
		t.Fatal(err)
	}
	if manifest.OperationID != operationID || manifest.RuntimeFingerprint != compatibility.RuntimeFingerprint || len(manifest.Files) != 1 {
		t.Fatalf("unexpected transport manifest: %+v", manifest)
	}
}

func TestKataTransportRejectsNonHexFingerprint(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "vm.state"), []byte("state"), 0o600); err != nil {
		t.Fatal(err)
	}
	compatibility := &ateompb.RuntimeCompatibility{Profile: kataCRProfileQEMU, RuntimeFingerprint: strings.Repeat("z", 64)}
	if err := writeKataTransport(root, "8d830f4d-0813-44ef-8495-3d95af80e27c", "node-a", compatibility, []string{"vm.state"}); err == nil {
		t.Fatal("writer accepted a non-hex runtime fingerprint")
	}
}

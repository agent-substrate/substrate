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
	"context"
	"os"
	"testing"

	"github.com/agent-substrate/substrate/internal/ateompath"
	"github.com/agent-substrate/substrate/internal/proto/ateletpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestValidateActorRequest(t *testing.T) {
	const okNS, okTmpl, okID, okUID = "ate-demo", "counter", "counter-1", "422938ba-8860-4983-a25d-d6bcb0a69d4e"
	okSpec := &ateletpb.WorkloadSpec{Containers: []*ateletpb.Container{{Name: "worker"}}}

	tests := []struct {
		name              string
		ns, tmpl, id, uid string
		spec              *ateletpb.WorkloadSpec
		wantErr           bool
	}{
		{"all valid", okNS, okTmpl, okID, okUID, okSpec, false},
		{"bad namespace", "../x", okTmpl, okID, okUID, okSpec, true},
		{"bad actor id", okNS, okTmpl, "../x", okUID, okSpec, true},
		{"bad uid", okNS, okTmpl, okID, "../x", okSpec, true},
		{"bad container", okNS, okTmpl, okID, okUID, &ateletpb.WorkloadSpec{Containers: []*ateletpb.Container{{Name: "../x"}}}, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if err := validateActorRequest(tc.ns, tc.tmpl, tc.id, tc.uid, tc.spec); (err != nil) != tc.wantErr {
				t.Errorf("validateActorRequest err = %v, wantErr %v", err, tc.wantErr)
			}
		})
	}
}

// validRunRequest, validCheckpointRequest, and validRestoreRequest build
// requests whose every field passes validation; the per-request tests below
// break one field per case.
func validRunRequest() *ateletpb.RunRequest {
	return &ateletpb.RunRequest{
		ActorTemplateNamespace: "ate-demo",
		ActorTemplateName:      "counter",
		ActorId:                "counter-1",
		TargetAteomUid:         "422938ba-8860-4983-a25d-d6bcb0a69d4e",
		Spec:                   &ateletpb.WorkloadSpec{Containers: []*ateletpb.Container{{Name: "worker"}}},
	}
}

func validCheckpointRequest() *ateletpb.CheckpointRequest {
	return &ateletpb.CheckpointRequest{
		ActorTemplateNamespace: "ate-demo",
		ActorTemplateName:      "counter",
		ActorId:                "counter-1",
		TargetAteomUid:         "422938ba-8860-4983-a25d-d6bcb0a69d4e",
		Spec:                   &ateletpb.WorkloadSpec{Containers: []*ateletpb.Container{{Name: "worker"}}},
		SnapshotUriPrefix:      "gs://bucket/actors/1/snapshots/2/",
	}
}

func validRestoreRequest() *ateletpb.RestoreRequest {
	return &ateletpb.RestoreRequest{
		ActorTemplateNamespace: "ate-demo",
		ActorTemplateName:      "counter",
		ActorId:                "counter-1",
		TargetAteomUid:         "422938ba-8860-4983-a25d-d6bcb0a69d4e",
		Spec:                   &ateletpb.WorkloadSpec{Containers: []*ateletpb.Container{{Name: "worker"}}},
		SnapshotUriPrefix:      "gs://bucket/actors/1/snapshots/2/",
	}
}

func TestValidateRunRequest(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(*ateletpb.RunRequest)
		wantErr bool
	}{
		{"valid", func(*ateletpb.RunRequest) {}, false},
		{"invalid ateom uid", func(r *ateletpb.RunRequest) { r.TargetAteomUid = "../escape" }, true},
		{"invalid actor id", func(r *ateletpb.RunRequest) { r.ActorId = "../escape" }, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req := validRunRequest()
			tc.mutate(req)
			if err := validateRunRequest(req); (err != nil) != tc.wantErr {
				t.Errorf("validateRunRequest err = %v, wantErr %v", err, tc.wantErr)
			}
		})
	}
}

// Checkpoint and Restore must reject a bad snapshot URI prefix even when
// every common field is valid.
func TestValidateCheckpointRequest(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(*ateletpb.CheckpointRequest)
		wantErr bool
	}{
		{"valid", func(*ateletpb.CheckpointRequest) {}, false},
		{"empty snapshot uri", func(r *ateletpb.CheckpointRequest) { r.SnapshotUriPrefix = "" }, true},
		{"bucketless snapshot uri", func(r *ateletpb.CheckpointRequest) { r.SnapshotUriPrefix = "relative/path" }, true},
		{"invalid ateom uid", func(r *ateletpb.CheckpointRequest) { r.TargetAteomUid = "../escape" }, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req := validCheckpointRequest()
			tc.mutate(req)
			if err := validateCheckpointRequest(req); (err != nil) != tc.wantErr {
				t.Errorf("validateCheckpointRequest err = %v, wantErr %v", err, tc.wantErr)
			}
		})
	}
}

func TestValidateRestoreRequest(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(*ateletpb.RestoreRequest)
		wantErr bool
	}{
		{"valid", func(*ateletpb.RestoreRequest) {}, false},
		{"empty snapshot uri", func(r *ateletpb.RestoreRequest) { r.SnapshotUriPrefix = "" }, true},
		{"bucketless snapshot uri", func(r *ateletpb.RestoreRequest) { r.SnapshotUriPrefix = "relative/path" }, true},
		{"invalid ateom uid", func(r *ateletpb.RestoreRequest) { r.TargetAteomUid = "../escape" }, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req := validRestoreRequest()
			tc.mutate(req)
			if err := validateRestoreRequest(req); (err != nil) != tc.wantErr {
				t.Errorf("validateRestoreRequest err = %v, wantErr %v", err, tc.wantErr)
			}
		})
	}
}

// TestFetchRunscRejectsBadHash confirms fetchRunsc validates the runsc hash
// before the cache-hit os.Stat/early-return, not merely "at some point". To
// prove the ordering, it plants a real file at the exact path an invalid hash
// resolves to: a correctly-ordered fetchRunsc validates first and returns an
// error, while a regression that stats first would find this file and return it
// with a nil error, failing the test. StaticFilesDir is redirected to a temp
// dir so the planted path is writable and isolated. Both arch fields are set so
// the test is independent of the host GOARCH.
func TestFetchRunscRejectsBadHash(t *testing.T) {
	orig := ateompath.StaticFilesDir
	ateompath.StaticFilesDir = t.TempDir()
	t.Cleanup(func() { ateompath.StaticFilesDir = orig })

	// Invalid (8 chars, not 64) but separator-free, so it resolves to a normal
	// filename inside the temp StaticFilesDir.
	const badHash = "deadbeef"
	if err := os.WriteFile(ateompath.RunSCBinaryPath(badHash), []byte("planted"), 0o755); err != nil {
		t.Fatalf("planting cache file: %v", err)
	}

	s := &AteomHerder{}
	bad := &ateletpb.RunscPlatformConfig{Sha256Hash: badHash}
	cfg := &ateletpb.RunscConfig{Amd64: bad, Arm64: bad}

	if _, err := s.fetchRunsc(context.Background(), cfg); err == nil {
		t.Error("fetchRunsc returned a cache hit for an invalid hash; validation must run before the os.Stat early return")
	}
}

// TestRPCBoundariesReject confirms each of the three RPCs validates path inputs
// before touching its (here nil) dependencies. A traversal value must be
// rejected as InvalidArgument rather than panicking or surfacing as
// Internal. Guards against a future removal or reordering of the validation
// call at any boundary.
func TestRPCBoundariesReject(t *testing.T) {
	s := &AteomHerder{}
	ctx := context.Background()
	badUID := "../escape" // valid actor ref, invalid ateom UID
	const okNS, okTmpl, okID = "ate-demo", "counter", "counter-1"
	okSpec := &ateletpb.WorkloadSpec{Containers: []*ateletpb.Container{{Name: "worker"}}}

	wantInvalidArgument := func(t *testing.T, rpc string, err error) {
		t.Helper()
		if err == nil {
			t.Errorf("%s accepted an invalid target ateom UID", rpc)
			return
		}
		if code := status.Code(err); code != codes.InvalidArgument {
			t.Errorf("%s returned code %v, want InvalidArgument", rpc, code)
		}
	}

	t.Run("Run", func(t *testing.T) {
		_, err := s.Run(ctx, &ateletpb.RunRequest{
			ActorTemplateNamespace: okNS, ActorTemplateName: okTmpl, ActorId: okID,
			TargetAteomUid: badUID, Spec: okSpec,
		})
		wantInvalidArgument(t, "Run", err)
	})
	t.Run("Checkpoint", func(t *testing.T) {
		_, err := s.Checkpoint(ctx, &ateletpb.CheckpointRequest{
			ActorTemplateNamespace: okNS, ActorTemplateName: okTmpl, ActorId: okID,
			TargetAteomUid: badUID, Spec: okSpec,
		})
		wantInvalidArgument(t, "Checkpoint", err)
	})
	t.Run("Restore", func(t *testing.T) {
		_, err := s.Restore(ctx, &ateletpb.RestoreRequest{
			ActorTemplateNamespace: okNS, ActorTemplateName: okTmpl, ActorId: okID,
			TargetAteomUid: badUID, Spec: okSpec,
		})
		wantInvalidArgument(t, "Restore", err)
	})
}

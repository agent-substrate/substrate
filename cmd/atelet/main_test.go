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
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/agent-substrate/substrate/internal/ateompath"
	"github.com/agent-substrate/substrate/internal/proto/ateletpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestWriteFileAtomic(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "actor-id")

	// One shared write over an existing value, as happens on every resume;
	// each subtest checks one postcondition.
	if err := os.WriteFile(target, []byte("golden-id"), 0o600); err != nil {
		t.Fatalf("seeding target: %v", err)
	}
	if err := writeFileAtomic(target, []byte("counter-1"), 0o644); err != nil {
		t.Fatalf("writeFileAtomic: %v", err)
	}

	t.Run("replaces content", func(t *testing.T) {
		got, err := os.ReadFile(target)
		if err != nil {
			t.Fatalf("reading target: %v", err)
		}
		if string(got) != "counter-1" {
			t.Errorf("content = %q, want %q", got, "counter-1")
		}
	})

	t.Run("sets permissions", func(t *testing.T) {
		info, err := os.Stat(target)
		if err != nil {
			t.Fatalf("stat target: %v", err)
		}
		if perm := info.Mode().Perm(); perm != 0o644 {
			t.Errorf("perm = %o, want 644", perm)
		}
	})

	t.Run("leaves no temp files", func(t *testing.T) {
		// The directory is visible inside the actor.
		entries, err := os.ReadDir(dir)
		if err != nil {
			t.Fatalf("reading dir: %v", err)
		}
		if len(entries) != 1 {
			names := make([]string, 0, len(entries))
			for _, e := range entries {
				names = append(names, e.Name())
			}
			t.Errorf("leftover files in identity dir: %v", names)
		}
	})
}

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
		Type:                   ateletpb.CheckpointType_CHECKPOINT_TYPE_EXTERNAL,
		Config: &ateletpb.CheckpointRequest_ExternalConfig{
			ExternalConfig: &ateletpb.ExternalCheckpointConfiguration{
				SnapshotUriPrefix: "gs://bucket/actors/1/snapshots/2/",
			},
		},
	}
}

func validRestoreRequest() *ateletpb.RestoreRequest {
	return &ateletpb.RestoreRequest{
		ActorTemplateNamespace: "ate-demo",
		ActorTemplateName:      "counter",
		ActorId:                "counter-1",
		TargetAteomUid:         "422938ba-8860-4983-a25d-d6bcb0a69d4e",
		Spec:                   &ateletpb.WorkloadSpec{Containers: []*ateletpb.Container{{Name: "worker"}}},
		Type:                   ateletpb.CheckpointType_CHECKPOINT_TYPE_EXTERNAL,
		Config: &ateletpb.RestoreRequest_ExternalConfig{
			ExternalConfig: &ateletpb.ExternalCheckpointConfiguration{
				SnapshotUriPrefix: "gs://bucket/actors/1/snapshots/2/",
			},
		},
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
	makeReq := func(opts ...func(*ateletpb.CheckpointRequest)) *ateletpb.CheckpointRequest {
		r := validCheckpointRequest()
		for _, opt := range opts {
			opt(r)
		}
		return r
	}

	tests := []struct {
		name    string
		req     *ateletpb.CheckpointRequest
		wantErr bool
	}{
		{"valid", makeReq(), false},
		{"empty snapshot uri", makeReq(func(r *ateletpb.CheckpointRequest) { r.GetExternalConfig().SnapshotUriPrefix = "" }), true},
		{"bucketless snapshot uri", makeReq(func(r *ateletpb.CheckpointRequest) { r.GetExternalConfig().SnapshotUriPrefix = "relative/path" }), true},
		{"invalid ateom uid", makeReq(func(r *ateletpb.CheckpointRequest) { r.TargetAteomUid = "../escape" }), true},
		{"invalid local snapshot prefix", makeReq(func(r *ateletpb.CheckpointRequest) {
			r.Type = ateletpb.CheckpointType_CHECKPOINT_TYPE_LOCAL
			r.Config = &ateletpb.CheckpointRequest_LocalConfig{LocalConfig: &ateletpb.LocalCheckpointConfiguration{SnapshotPrefix: ""}}
		}), true},
		{"unspecified snapshot type", makeReq(func(r *ateletpb.CheckpointRequest) { r.Type = ateletpb.CheckpointType_CHECKPOINT_TYPE_UNSPECIFIED }), true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if err := validateCheckpointRequest(tc.req); (err != nil) != tc.wantErr {
				t.Errorf("validateCheckpointRequest err = %v, wantErr %v", err, tc.wantErr)
			}
		})
	}
}

func TestValidateRestoreRequest(t *testing.T) {
	makeReq := func(opts ...func(*ateletpb.RestoreRequest)) *ateletpb.RestoreRequest {
		r := validRestoreRequest()
		for _, opt := range opts {
			opt(r)
		}
		return r
	}

	tests := []struct {
		name    string
		req     *ateletpb.RestoreRequest
		wantErr bool
	}{
		{"valid", makeReq(), false},
		{"empty snapshot uri", makeReq(func(r *ateletpb.RestoreRequest) { r.GetExternalConfig().SnapshotUriPrefix = "" }), true},
		{"bucketless snapshot uri", makeReq(func(r *ateletpb.RestoreRequest) { r.GetExternalConfig().SnapshotUriPrefix = "relative/path" }), true},
		{"invalid ateom uid", makeReq(func(r *ateletpb.RestoreRequest) { r.TargetAteomUid = "../escape" }), true},
		{"invalid local snapshot prefix", makeReq(func(r *ateletpb.RestoreRequest) {
			r.Type = ateletpb.CheckpointType_CHECKPOINT_TYPE_LOCAL
			r.Config = &ateletpb.RestoreRequest_LocalConfig{LocalConfig: &ateletpb.LocalCheckpointConfiguration{SnapshotPrefix: ""}}
		}), true},
		{"unspecified snapshot type", makeReq(func(r *ateletpb.RestoreRequest) { r.Type = ateletpb.CheckpointType_CHECKPOINT_TYPE_UNSPECIFIED }), true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if err := validateRestoreRequest(tc.req); (err != nil) != tc.wantErr {
				t.Errorf("validateRestoreRequest err = %v, wantErr %v", err, tc.wantErr)
			}
		})
	}
}

// TestFetchAssetRejectsBadHash confirms fetchAsset validates the asset hash
// before the cache-hit os.Stat/early-return, not merely "at some point". To
// prove the ordering, it plants a real file at the exact path an invalid hash
// resolves to: a correctly-ordered fetchAsset validates first and returns an
// error, while a regression that stats first would find this file and return it
// with a nil error, failing the test. StaticFilesDir is redirected to a temp
// dir so the planted path is writable and isolated.
func TestFetchAssetRejectsBadHash(t *testing.T) {
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
	if _, err := s.fetchAsset(context.Background(), assetEntry{SHA256: badHash}); err == nil {
		t.Error("fetchAsset returned a cache hit for an invalid hash; validation must run before the os.Stat early return")
	}
}

// fakeObjectStorage serves fixed bytes for GetObject so fetchAsset can be tested.
type fakeObjectStorage struct {
	data []byte
	err  error
}

func (f fakeObjectStorage) GetObject(_ context.Context, _, _ string) (io.ReadCloser, error) {
	if f.err != nil {
		return nil, f.err
	}
	return io.NopCloser(bytes.NewReader(f.data)), nil
}

func (fakeObjectStorage) PutObject(_ context.Context, _, _ string, _ io.Reader) error { return nil }

// TestFetchAssetStreaming covers the streamed download: good asset cached,
// over-cap rejected, hash mismatch rejected (failures leave no cache file).
func TestFetchAssetStreaming(t *testing.T) {
	origDir, origCap := ateompath.StaticFilesDir, maxAssetBytes
	t.Cleanup(func() { ateompath.StaticFilesDir, maxAssetBytes = origDir, origCap })

	content := []byte("micro-vm kernel bytes")
	goodHash := fmt.Sprintf("%x", sha256.Sum256(content))
	const url = "gs://test-bucket/asset"

	t.Run("good asset is cached", func(t *testing.T) {
		ateompath.StaticFilesDir = t.TempDir()
		s := &AteomHerder{anonGCSClient: fakeObjectStorage{data: content}}
		path, err := s.fetchAsset(context.Background(), assetEntry{URL: url, SHA256: goodHash})
		if err != nil {
			t.Fatalf("fetchAsset: %v", err)
		}
		got, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("reading cached asset: %v", err)
		}
		if !bytes.Equal(got, content) {
			t.Errorf("cached bytes = %q, want %q", got, content)
		}
	})

	t.Run("over-cap asset rejected, cache not written", func(t *testing.T) {
		ateompath.StaticFilesDir = t.TempDir()
		maxAssetBytes = 4 // content is longer than this
		s := &AteomHerder{anonGCSClient: fakeObjectStorage{data: content}}
		if _, err := s.fetchAsset(context.Background(), assetEntry{URL: url, SHA256: goodHash}); err == nil {
			t.Fatal("fetchAsset accepted an over-cap asset")
		}
		if _, err := os.Stat(ateompath.RunSCBinaryPath(goodHash)); !errors.Is(err, os.ErrNotExist) {
			t.Errorf("over-cap download left a file at the cache path (stat err = %v)", err)
		}
	})

	t.Run("hash mismatch rejected, cache not written", func(t *testing.T) {
		ateompath.StaticFilesDir = t.TempDir()
		maxAssetBytes = origCap
		wrongHash := strings.Repeat("a", 64) // valid 64-hex format, wrong value
		s := &AteomHerder{anonGCSClient: fakeObjectStorage{data: content}}
		if _, err := s.fetchAsset(context.Background(), assetEntry{URL: url, SHA256: wrongHash}); err == nil {
			t.Fatal("fetchAsset accepted a hash mismatch")
		}
		if _, err := os.Stat(ateompath.RunSCBinaryPath(wrongHash)); !errors.Is(err, os.ErrNotExist) {
			t.Errorf("mismatched download left a file at the cache path (stat err = %v)", err)
		}
	})
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

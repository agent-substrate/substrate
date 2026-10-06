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
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/agent-substrate/substrate/pkg/proto/hostruntimepb"
	objectstoresnapshotv1 "github.com/agent-substrate/substrate/pkg/proto/objectstoresnapshotpb/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type fakeSnapshots struct {
	calls        [][]string
	objects      map[string][]byte
	failManifest bool
}

func (f *fakeSnapshots) UploadSnapshot(_ context.Context, r *objectstoresnapshotv1.UploadSnapshotRequest, _ ...grpc.CallOption) (*objectstoresnapshotv1.UploadSnapshotResponse, error) {
	f.calls = append(f.calls, slices.Clone(r.Files))
	if f.failManifest && slices.Equal(r.Files, []string{manifestName}) {
		return nil, errors.New("commit failed")
	}
	if f.objects == nil {
		f.objects = map[string][]byte{}
	}
	for _, name := range r.Files {
		b, err := os.ReadFile(filepath.Join(r.LocalPath, name))
		if err != nil {
			return nil, err
		}
		f.objects[name] = b
	}
	return &objectstoresnapshotv1.UploadSnapshotResponse{}, nil
}
func (f *fakeSnapshots) FetchSnapshot(_ context.Context, r *objectstoresnapshotv1.FetchSnapshotRequest, _ ...grpc.CallOption) (*objectstoresnapshotv1.FetchSnapshotResponse, error) {
	for _, name := range r.Files {
		b, ok := f.objects[name]
		if !ok {
			return nil, errors.New("missing")
		}
		if err := os.WriteFile(filepath.Join(r.WritePath, name), b, 0o600); err != nil {
			return nil, err
		}
	}
	return &objectstoresnapshotv1.FetchSnapshotResponse{}, nil
}

func stoppedBundle(t *testing.T, s *server, f *fakeRunner, uid string) string {
	t.Helper()
	bundle := s.bundle(uid)
	if err := os.Mkdir(bundle, 0o700); err != nil {
		t.Fatal(err)
	}
	for _, name := range snapshotFiles {
		if err := os.WriteFile(filepath.Join(bundle, name), []byte(name+" contents"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	f.states[bundle] = vmStatus{ActorID: uid, Phase: "stopped"}
	return bundle
}

func TestPauseIdempotencyConflictAndLocalResume(t *testing.T) {
	f := newFakeRunner()
	s := testServer(t, f)
	stoppedBundle(t, s, f, "a")
	for range 2 {
		if _, err := s.Pause(context.Background(), &hostruntimepb.PauseRequest{ActorUid: "a", LocalSnapshotName: "local-1"}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := s.Pause(context.Background(), &hostruntimepb.PauseRequest{ActorUid: "a", LocalSnapshotName: "other"}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("conflict = %v", err)
	}
	r := request("a")
	r.LocalSnapshotName = new("local-1")
	if _, err := s.Activate(context.Background(), r); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Pause(context.Background(), &hostruntimepb.PauseRequest{ActorUid: "a", LocalSnapshotName: "local-2"}); err != nil {
		t.Fatalf("second lifecycle pause: %v", err)
	}
	if got, err := readLocalReceipt(s.bundle("a")); err != nil || got != "local-2" {
		t.Fatalf("second pause receipt = %q, %v", got, err)
	}
}

func TestCheckpointOrdersCommitAndCleansOnlyAfterSuccess(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(map[bool]string{false: "success", true: "failure"}[fail], func(t *testing.T) {
			f := newFakeRunner()
			s := testServer(t, f)
			bundle := stoppedBundle(t, s, f, "a")
			store := &fakeSnapshots{failManifest: fail}
			s.snapshots = store
			_, err := s.Checkpoint(context.Background(), &hostruntimepb.CheckpointRequest{ActorUid: "a", ExternalSnapshotUri: "gs://bucket/a"})
			if fail && status.Code(err) != codes.Unavailable {
				t.Fatalf("error = %v", err)
			}
			if !fail && err != nil {
				t.Fatal(err)
			}
			if len(store.calls) != 2 || !slices.Equal(store.calls[0], snapshotFiles) || !slices.Equal(store.calls[1], []string{manifestName}) {
				t.Fatalf("calls = %v", store.calls)
			}
			_, statErr := os.Stat(bundle)
			if fail && statErr != nil || !fail && !errors.Is(statErr, os.ErrNotExist) {
				t.Fatalf("bundle stat = %v", statErr)
			}
		})
	}
}

func TestExternalRestoreRejectsChecksumAndURIConflict(t *testing.T) {
	f := newFakeRunner()
	source := testServer(t, f)
	stoppedBundle(t, source, f, "a")
	store := &fakeSnapshots{}
	source.snapshots = store
	if _, err := source.Checkpoint(context.Background(), &hostruntimepb.CheckpointRequest{ActorUid: "a", ExternalSnapshotUri: "gs://bucket/a"}); err != nil {
		t.Fatal(err)
	}
	if _, err := source.Checkpoint(context.Background(), &hostruntimepb.CheckpointRequest{ActorUid: "a", ExternalSnapshotUri: "gs://bucket/other"}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("conflict = %v", err)
	}
	originalManifest := slices.Clone(store.objects[manifestName])
	var manifest snapshotManifest
	if err := json.Unmarshal(originalManifest, &manifest); err != nil {
		t.Fatal(err)
	}
	manifest.ContentScope = "full"
	manifestBytes, err := json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	store.objects[manifestName] = manifestBytes
	target := testServer(t, newFakeRunner())
	target.snapshots = store
	r := request("b")
	r.ExternalSnapshotUri = new("gs://bucket/a")
	if _, err := target.Activate(context.Background(), r); status.Code(err) != codes.Internal {
		t.Fatalf("content-scope error = %v", err)
	}
	store.objects[manifestName] = originalManifest
	store.objects["disk.img"][0] ^= 1
	if _, err := target.Activate(context.Background(), r); status.Code(err) != codes.Internal {
		t.Fatalf("checksum error = %v", err)
	}
	if exists, _ := safeDirectory(target.bundle("b")); exists {
		t.Fatal("failed restore left an Actor bundle")
	}
}

func TestRestoredActorCanCheckpointToNewURI(t *testing.T) {
	sourceRunner := newFakeRunner()
	source := testServer(t, sourceRunner)
	stoppedBundle(t, source, sourceRunner, "a")
	store := &fakeSnapshots{}
	source.snapshots = store
	if _, err := source.Checkpoint(context.Background(), &hostruntimepb.CheckpointRequest{ActorUid: "a", ExternalSnapshotUri: "gs://bucket/first"}); err != nil {
		t.Fatal(err)
	}

	target := testServer(t, newFakeRunner())
	target.snapshots = store
	req := request("a")
	req.ExternalSnapshotUri = new("gs://bucket/first")
	if _, err := target.Activate(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	if _, err := target.Checkpoint(context.Background(), &hostruntimepb.CheckpointRequest{ActorUid: "a", ExternalSnapshotUri: "gs://bucket/second"}); err != nil {
		t.Fatalf("second lifecycle checkpoint: %v", err)
	}
	if got, err := target.readExternalReceipt("a"); err != nil || got != "gs://bucket/second" {
		t.Fatalf("completion receipt = %q, %v", got, err)
	}
}

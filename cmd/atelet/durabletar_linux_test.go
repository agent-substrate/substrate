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
	"errors"
	"io"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/agent-substrate/substrate/internal/ateompath"
	ateletpb "github.com/agent-substrate/substrate/internal/proto/ateletpb"
	ateompb "github.com/agent-substrate/substrate/internal/proto/ateompb"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/internal/tarutil"
	atev1alpha1 "github.com/agent-substrate/substrate/pkg/api/v1alpha1"
	"github.com/klauspost/compress/zstd"
)

// durableDirFixture builds a small durable-dir tree and returns its path.
func durableDirFixture(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, "vol-a", "nested"), 0o755); err != nil {
		t.Fatal(err)
	}
	// Big enough to outrun the pipe's internal handoff, so the walk and the
	// consumer really do run concurrently rather than completing in one write.
	if err := os.WriteFile(filepath.Join(dir, "vol-a", "big"), bytes.Repeat([]byte("substrate"), 1<<16), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "vol-a", "nested", "small"), []byte("hello"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("big", filepath.Join(dir, "vol-a", "link")); err != nil {
		t.Fatal(err)
	}
	return dir
}

// TestStreamDurableDirTarMatchesStagedArchive is the property the whole change
// rests on: a snapshot taken by streaming must restore the same tree as one
// ateom staged on disk, so the two archives have to be byte-identical.
func TestStreamDurableDirTarMatchesStagedArchive(t *testing.T) {
	dir := durableDirFixture(t)

	staged := filepath.Join(t.TempDir(), "durable-dir.tar")
	if err := tarutil.Create(t.Context(), staged, dir); err != nil {
		t.Fatalf("staging archive: %v", err)
	}
	want, err := os.ReadFile(staged)
	if err != nil {
		t.Fatal(err)
	}

	var got bytes.Buffer
	size, err := streamDurableDirTar(t.Context(), dir, func(r io.Reader) error {
		_, err := io.Copy(&got, r)
		return err
	})
	if err != nil {
		t.Fatalf("streamDurableDirTar: %v", err)
	}
	if !bytes.Equal(got.Bytes(), want) {
		t.Errorf("streamed archive differs from staged archive: got %d bytes, want %d", got.Len(), len(want))
	}
	if size != int64(len(want)) {
		t.Errorf("reported size = %d, want %d", size, len(want))
	}
}

// TestStreamDurableDirTarUploadFailure checks the upload's error is what the
// caller sees, and that the walk does not deadlock once nobody is reading.
func TestStreamDurableDirTarUploadFailure(t *testing.T) {
	dir := durableDirFixture(t)
	sentinel := errors.New("object storage is down")

	// Reads one byte and quits, leaving the walk blocked on a pipe with no
	// reader: only the CloseWithError on the read end lets it finish.
	size, err := streamDurableDirTar(t.Context(), dir, func(r io.Reader) error {
		if _, readErr := io.ReadFull(r, make([]byte, 1)); readErr != nil {
			t.Errorf("reading first byte: %v", readErr)
		}
		return sentinel
	})
	if !errors.Is(err, sentinel) {
		t.Errorf("err = %v, want %v", err, sentinel)
	}
	if size != 0 {
		t.Errorf("size = %d, want 0 on failure", size)
	}
}

// TestStreamDurableDirTarArchiveFailure checks a failed walk aborts the upload
// rather than committing whatever bytes it managed to produce.
func TestStreamDurableDirTarArchiveFailure(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "no-such-durable-dir")

	var uploadErr error
	uploaded := false
	if _, err := streamDurableDirTar(t.Context(), missing, func(r io.Reader) error {
		_, uploadErr = io.Copy(io.Discard, r)
		uploaded = uploadErr == nil
		return uploadErr
	}); err == nil {
		t.Fatal("streamDurableDirTar succeeded on a missing source directory")
	}
	if uploaded {
		t.Error("upload saw a clean EOF; a truncated archive would have been committed")
	}
	if !errors.Is(uploadErr, os.ErrNotExist) {
		t.Errorf("upload read error = %v, want it to carry %v", uploadErr, os.ErrNotExist)
	}
}

func TestStreamDurableTarEnabled(t *testing.T) {
	for _, tc := range []struct {
		value string
		want  bool
	}{
		{"1", true}, {"true", true}, {"TRUE", true}, {" on ", true}, {"yes", true},
		{"", false}, {"0", false}, {"false", false}, {"off", false},
		// Anything unrecognized stays on the staged path: the knob exists to
		// try the new one, so a typo must not silently opt in.
		{"onn", false}, {"enabled", false},
	} {
		t.Run(tc.value, func(t *testing.T) {
			t.Setenv(streamDurableTarEnv, tc.value)
			if got := streamDurableTarEnabled(); got != tc.want {
				t.Errorf("streamDurableTarEnabled(%q) = %v, want %v", tc.value, got, tc.want)
			}
		})
	}
}

func TestCanStreamDurableDirTar(t *testing.T) {
	externalReq := &ateletpb.CheckpointRequest{Type: ateletpb.CheckpointType_CHECKPOINT_TYPE_EXTERNAL}
	localReq := &ateletpb.CheckpointRequest{Type: ateletpb.CheckpointType_CHECKPOINT_TYPE_LOCAL}
	durableSpec := &ateompb.WorkloadSpec{Containers: []*ateompb.Container{{
		Name:                   "app",
		DurableDirVolumeMounts: []*ateompb.DurableDirVolumeMount{{VolumeName: "data", MountPath: "/data"}},
	}}}
	plainSpec := &ateompb.WorkloadSpec{Containers: []*ateompb.Container{{Name: "app"}}}
	microVM := &sandboxAssetsRecord{SandboxClass: string(atev1alpha1.SandboxClassMicroVM)}

	for _, tc := range []struct {
		name string
		env  string
		req  *ateletpb.CheckpointRequest
		spec *ateompb.WorkloadSpec
		rec  *sandboxAssetsRecord
		want bool
	}{
		{"eligible", "1", externalReq, durableSpec, microVM, true},
		{"knob off", "", externalReq, durableSpec, microVM, false},
		{"local checkpoint", "1", localReq, durableSpec, microVM, false},
		{"gvisor", "1", externalReq, durableSpec, &sandboxAssetsRecord{SandboxClass: string(atev1alpha1.SandboxClassGvisor)}, false},
		// The predicate ateom applies: a declared-but-unmounted durable volume
		// makes ateom write no archive, so we must not invent one.
		{"no durable mounts", "1", externalReq, plainSpec, microVM, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv(streamDurableTarEnv, tc.env)
			if got := canStreamDurableDirTar(tc.req, tc.spec, tc.rec); got != tc.want {
				t.Errorf("canStreamDurableDirTar() = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestUploadSnapshotStreamsDurableDirTar covers the seam: the durable-dir tar
// is the one snapshot file with no copy in srcDir, and it still has to land as
// the same object, beside the files that do come off disk.
func TestUploadSnapshotStreamsDurableDirTar(t *testing.T) {
	uri, err := resources.ParseSnapshotURI(pausedSnapshotURI)
	if err != nil {
		t.Fatalf("ParseSnapshotURI: %v", err)
	}

	// Holds every file EXCEPT the durable-dir tar, exactly as ateom leaves it
	// when it is told to skip that one.
	srcDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(srcDir, "config.json"), []byte("cfg"), 0o600); err != nil {
		t.Fatal(err)
	}
	durableDir := durableDirFixture(t)

	store := &recordingObjectStorage{}
	s := &AteomHerder{gcsClient: store}
	rec := &sandboxAssetsRecord{
		SandboxClass:  string(atev1alpha1.SandboxClassMicroVM),
		SnapshotFiles: []string{"config.json", ateompath.DurableDirTarFile},
	}
	if err := s.uploadSnapshot(t.Context(), uri, srcDir, rec, "team-a", "tmpl", durableDir); err != nil {
		t.Fatalf("uploadSnapshot: %v", err)
	}

	want := []string{
		pausedSnapshotPath + "/config.json.zstd",
		pausedSnapshotPath + "/durable-dir.tar.zstd",
		pausedSnapshotPath + "/manifest.json",
	}
	if got := store.keys(); !slices.Equal(got, want) {
		t.Fatalf("uploaded objects = %v, want %v", got, want)
	}

	// Decompressing and extracting is what proves the streamed object is a
	// usable archive rather than merely present.
	dec, err := zstd.NewReader(bytes.NewReader(store.objects[pausedSnapshotPath+"/durable-dir.tar.zstd"]))
	if err != nil {
		t.Fatalf("opening zstd reader: %v", err)
	}
	defer dec.Close()
	plain, err := io.ReadAll(dec)
	if err != nil {
		t.Fatalf("decompressing durable-dir tar: %v", err)
	}
	tarPath := filepath.Join(t.TempDir(), "durable-dir.tar")
	if err := os.WriteFile(tarPath, plain, 0o600); err != nil {
		t.Fatal(err)
	}
	restored := t.TempDir()
	if err := tarutil.Extract(tarPath, restored); err != nil {
		t.Fatalf("extracting streamed archive: %v", err)
	}
	if got, err := os.ReadFile(filepath.Join(restored, "vol-a", "nested", "small")); err != nil || string(got) != "hello" {
		t.Errorf("restored vol-a/nested/small = %q, %v; want %q", got, err, "hello")
	}
}

// Compile-time check that the counter is a plain io.Writer, so tarutil writes
// through it unchanged.
var _ io.Writer = (*countingWriter)(nil)

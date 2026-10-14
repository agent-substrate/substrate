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
	"fmt"
	"os"
	"testing"
	"time"

	"go.ate.dev/substrate/cmd/atelet/internal/ateletpath"
	"go.ate.dev/substrate/cmd/atelet/internal/snapshottracker"
	"go.ate.dev/substrate/internal/apierror"
	"go.ate.dev/substrate/internal/ateinterceptors"
	"go.ate.dev/substrate/internal/checkpointabort"
	"go.ate.dev/substrate/internal/proto/ateletpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/utils/clock"
)

// TestCheckpointPassesAbortThrough pins atelet's part of the aborted-checkpoint
// contract: an abort from the worker reaches the control plane as an abort,
// with the actor's on-node state untouched, and any other worker error does
// not. The untouched state includes the earlier local snapshot and the tracker
// entry naming it, which the control plane keeps listing after an abort, with
// its detach still unscheduled.
func TestCheckpointPassesAbortThrough(t *testing.T) {
	tests := []struct {
		name      string
		workerErr error
		wantAbort bool
	}{
		{
			name:      "abort",
			workerErr: checkpointabort.Errorf("while freezing guest container %q: injected", "app"),
			wantAbort: true,
		},
		{
			name:      "failed precondition",
			workerErr: apierror.FailedPrecondition("no durable-dir or CSI volumes found for a VOLUMES snapshot"),
		},
		{
			name:      "plain error",
			workerErr: fmt.Errorf("while pausing guest: injected"),
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			useTempNodeDirs(t)
			ctx := t.Context()

			const (
				actorUID     = "01234567-89ab-cdef-0123-456789abcdef"
				workerPodUID = "5f1c0d2e-3a4b-4c5d-8e9f-0a1b2c3d4e5f"
				snapshotName = "pause-snap-1"
				prevSnapshot = "pause-snap-0"
			)
			worker := &fakeWorker{}
			serveFakeWorker(t, worker)

			host := imageVolumeTestRegistry(t)
			pinnedImage := pushTestImage(t, host+"/actor:v1", singleFileLayer(t, "bin/app", "app"))
			release := newGVisorTestRelease(t)
			s := &Atelet{
				workerDialer:      newWorkerDialer(1),
				imageCache:        newImageVolumeStore(t),
				anonGCSClient:     fakeObjectStorage{data: release.tarball},
				systemInfoVolumes: newSystemInfoVolumeRefresher(nil, nil),
				sandboxAssetCache: newTestAssetCache(t),
			}
			s.snapshots = snapshottracker.New(t.Context(), &trackerStore{s: s}, clock.RealClock{})
			spec := &ateletpb.WorkloadSpec{
				Containers: []*ateletpb.Container{{Name: "app", Image: pinnedImage, Command: []string{"/bin/app"}}},
			}
			sandboxRec := testSandboxRecord(release)
			advertiseRuntime(t, s, sandboxRec)
			if _, err := s.Run(ctx, &ateletpb.RunRequest{
				Atespace:              "ate-demo",
				ActorName:             "counter",
				ActorUid:              actorUID,
				ActorTemplateAtespace: "default",
				ActorTemplateName:     "counter",
				WorkerPodUid:          workerPodUID,
				SandboxRuntime:        runtimeFor(t, sandboxRec),
				Spec:                  spec,
			}); err != nil {
				t.Fatalf("Run: %v", err)
			}

			// An earlier suspend left a local snapshot, and the actor resumed
			// from it on this node, which unscheduled its detach.
			if err := os.MkdirAll(ateletpath.LocalSnapshotDir(actorUID, prevSnapshot), 0o755); err != nil {
				t.Fatal(err)
			}
			s.snapshots.CommitLocal(actorUID, snapshottracker.SnapshotRef{
				Snapshot:   &ateletpb.Snapshot{SnapshotUuid: prevSnapshot, Generation: 1, Fidelity: ateletpb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY},
				CommitTime: time.Now(),
			})
			endStart, err := s.snapshots.BeginStart(ctx, actorUID)
			if err != nil {
				t.Fatalf("BeginStart: %v", err)
			}
			endStart()

			worker.checkpointErr = tc.workerErr
			_, err = s.Checkpoint(ctx, &ateletpb.CheckpointRequest{
				Atespace:              "ate-demo",
				ActorName:             "counter",
				ActorUid:              actorUID,
				ActorTemplateAtespace: "default",
				ActorTemplateName:     "counter",
				WorkerPodUid:          workerPodUID,
				Spec:                  spec,
				Snapshot:              lifecycleSnapshot(actorUID, snapshotName, 2, ateletpb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY),
				UploadConfig:          &ateletpb.UploadConfig{Mode: ateletpb.UploadMode_UPLOAD_MODE_DEFERRED},
			})
			if err == nil {
				t.Fatal("Checkpoint() = nil, want the worker's error")
			}
			if _, ok := worker.fidelities["CheckpointWorkload"]; !ok {
				t.Fatalf("Checkpoint() = %v before calling the worker's CheckpointWorkload", err)
			}
			// What ate-api-server receives: atelet serves with this interceptor.
			_, sent := ateinterceptors.InternalServerUnaryInterceptor(ctx, nil, &grpc.UnaryServerInfo{FullMethod: "/atelet/Checkpoint"},
				func(context.Context, any) (any, error) { return nil, err })
			if got := checkpointabort.Is(sent); got != tc.wantAbort {
				t.Errorf("checkpointabort.Is(%v) = %v, want %v", sent, got, tc.wantAbort)
			}
			if !tc.wantAbort && status.Code(sent) == codes.Aborted {
				t.Errorf("atelet sent %v, want any code but %v", sent, codes.Aborted)
			}

			// The actor is still hosted: its record stays, and no snapshot was kept.
			if _, err := readSandboxRecord(actorUID); err != nil {
				t.Errorf("sandbox record after a failed checkpoint: %v", err)
			}
			if _, err := os.Stat(ateletpath.LocalSnapshotDir(actorUID, snapshotName)); !os.IsNotExist(err) {
				t.Errorf("local snapshot dir after a failed checkpoint: stat err = %v, want not exist", err)
			}
			if _, err := os.Stat(ateletpath.LocalSnapshotDir(actorUID, prevSnapshot)); err != nil {
				t.Errorf("earlier local snapshot dir after a failed checkpoint: %v", err)
			}
			if got := s.snapshots.State(actorUID).GetLatestLocal().GetSnapshot().GetSnapshotUuid(); got != prevSnapshot {
				t.Errorf("tracker latest local after a failed checkpoint = %q, want %q", got, prevSnapshot)
			}
			// Nor is a detach scheduled: the running actor may still use it.
			if _, err := s.snapshots.Detach(ctx, actorUID); apierror.Code(err) != codes.Aborted {
				t.Errorf("Detach after a failed checkpoint = %v, want Aborted", err)
			}
		})
	}
}

// TestRestorePassesFailedPreconditionThrough pins that a FailedPrecondition
// from the worker's RestoreWorkload, such as guest memory captured on another
// hypervisor backend, reaches the control plane with its code, and that any
// other worker error does not become one.
func TestRestorePassesFailedPreconditionThrough(t *testing.T) {
	tests := []struct {
		name      string
		workerErr error
		wantCode  codes.Code
	}{
		{
			name:      "failed precondition",
			workerErr: apierror.FailedPrecondition("snapshot was taken on hypervisor mshv and cannot be restored on a kvm worker"),
			wantCode:  codes.FailedPrecondition,
		},
		{
			name:      "plain error",
			workerErr: fmt.Errorf("while resuming restored guest: injected"),
			wantCode:  codes.Internal,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			useTempNodeDirs(t)
			ctx := t.Context()

			const (
				actorUID     = "01234567-89ab-cdef-0123-456789abcdef"
				workerPodUID = "5f1c0d2e-3a4b-4c5d-8e9f-0a1b2c3d4e5f"
				snapshotName = "pause-snap-1"
			)
			worker := &fakeWorker{snapshotFiles: map[string]string{"checkpoint.img": "guest-memory"}}
			serveFakeWorker(t, worker)

			host := imageVolumeTestRegistry(t)
			pinnedImage := pushTestImage(t, host+"/actor:v1", singleFileLayer(t, "bin/app", "app"))
			release := newGVisorTestRelease(t)
			s := &Atelet{
				workerDialer:      newWorkerDialer(1),
				imageCache:        newImageVolumeStore(t),
				anonGCSClient:     fakeObjectStorage{data: release.tarball},
				systemInfoVolumes: newSystemInfoVolumeRefresher(nil, nil),
				snapshots:         snapshottracker.New(t.Context(), nil, clock.RealClock{}),
				sandboxAssetCache: newTestAssetCache(t),
			}
			sandboxRec := testSandboxRecord(release)
			spec := &ateletpb.WorkloadSpec{
				Containers: []*ateletpb.Container{{Name: "app", Image: pinnedImage, Command: []string{"/bin/app"}}},
			}
			snapshot := lifecycleSnapshot(actorUID, snapshotName, 1, ateletpb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY)
			advertiseRuntime(t, s, sandboxRec)
			if _, err := s.Run(ctx, &ateletpb.RunRequest{
				Atespace:              "ate-demo",
				ActorName:             "counter",
				ActorUid:              actorUID,
				ActorTemplateAtespace: "default",
				ActorTemplateName:     "counter",
				WorkerPodUid:          workerPodUID,
				SandboxRuntime:        runtimeFor(t, sandboxRec),
				Spec:                  spec,
			}); err != nil {
				t.Fatalf("Run: %v", err)
			}
			if _, err := s.Checkpoint(ctx, &ateletpb.CheckpointRequest{
				Atespace:              "ate-demo",
				ActorName:             "counter",
				ActorUid:              actorUID,
				ActorTemplateAtespace: "default",
				ActorTemplateName:     "counter",
				WorkerPodUid:          workerPodUID,
				Spec:                  spec,
				Snapshot:              snapshot,
				UploadConfig:          &ateletpb.UploadConfig{Mode: ateletpb.UploadMode_UPLOAD_MODE_DEFERRED},
			}); err != nil {
				t.Fatalf("Checkpoint: %v", err)
			}

			worker.restoreErr = tc.workerErr
			_, err := s.Restore(ctx, &ateletpb.RestoreRequest{
				Atespace:              "ate-demo",
				ActorName:             "counter",
				ActorUid:              actorUID,
				ActorTemplateAtespace: "default",
				ActorTemplateName:     "counter",
				WorkerPodUid:          workerPodUID,
				SandboxRuntime:        runtimeFor(t, sandboxRec),
				Spec:                  spec,
				Snapshot:              snapshot,
			})
			if err == nil {
				t.Fatal("Restore() = nil, want the worker's error")
			}
			if _, ok := worker.fidelities["RestoreWorkload"]; !ok {
				t.Fatalf("Restore() = %v before calling the worker's RestoreWorkload", err)
			}
			// What ate-api-server receives: atelet serves with this interceptor.
			_, sent := ateinterceptors.InternalServerUnaryInterceptor(ctx, nil, &grpc.UnaryServerInfo{FullMethod: "/atelet/Restore"},
				func(context.Context, any) (any, error) { return nil, err })
			if got := status.Code(sent); got != tc.wantCode {
				t.Errorf("atelet sent %v, want code %v", sent, tc.wantCode)
			}
		})
	}
}

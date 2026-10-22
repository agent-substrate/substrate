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
	"context"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"go.ate.dev/substrate/internal/actorlock"
	"go.ate.dev/substrate/internal/actorlog"
	"go.ate.dev/substrate/internal/apierror"
	"go.ate.dev/substrate/internal/ateinterceptors"
	"go.ate.dev/substrate/internal/atunnel"
	"go.ate.dev/substrate/internal/atunnel/egress"
	"go.ate.dev/substrate/internal/atunnel/ingress"
	"go.ate.dev/substrate/internal/nodepath"
	"go.ate.dev/substrate/internal/ocispec"
	"go.ate.dev/substrate/internal/proto/ateworkerpb"
	"golang.org/x/sys/unix"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Exercise the RPC with a runsc executable: a successful state command can
// report a stopped container while the sandbox itself stays alive.
func TestCheckpointWorkloadRejectsExitedApplication(t *testing.T) {
	for _, code := range []string{"0", "1"} {
		t.Run("exit "+code, func(t *testing.T) {
			s, req, dir := checkpointFixture(t)
			writeCheckpointFixture(t, dir, "sidecar.state", `{"id":"sidecar","status":"stopped"}`)
			writeCheckpointFixture(t, dir, "sidecar.wait", `{"id":"sidecar","exitStatus":`+code+`}`)
			resp, err := s.CheckpointWorkload(t.Context(), req)
			if err == nil || !strings.Contains(err.Error(), `container "sidecar"`) || !strings.Contains(err.Error(), "exit code "+code) {
				t.Fatalf("CheckpointWorkload = %v, %v; want the exited container and its code", resp, err)
			}
			if got := apierror.Code(err); got != codes.FailedPrecondition {
				t.Fatalf("checkpoint error code = %v, want FailedPrecondition", got)
			}
			assertCheckpointCommands(t, dir, false, true)
		})
	}
}

func TestCheckpointWorkloadChecksAfterFreeze(t *testing.T) {
	s, req, dir := checkpointFixture(t)
	// The application exits as the sandbox is being frozen.
	writeCheckpointFixture(t, dir, "sidecar.after-pause", `{"id":"sidecar","status":"stopped"}`)
	writeCheckpointFixture(t, dir, "sidecar.wait", `{"id":"sidecar","exitStatus":1}`)
	if _, err := s.CheckpointWorkload(t.Context(), req); err == nil {
		t.Fatal("checkpoint accepted an application that exited before the freeze completed")
	}
	assertCheckpointCommands(t, dir, false, true)
}

func TestCheckpointWorkloadFailsClosed(t *testing.T) {
	for _, tc := range []struct{ name, file, value, want string }{
		{"invalid state", "app.state", "not json", "app"},
		{"wrong container", "app.state", `{"id":"other","status":"running"}`, "app"},
		{"missing status", "app.state", `{"id":"app"}`, "app"},
		{"not started", "app.state", `{"id":"app","status":"created"}`, "created"},
		{"state failure", "state.error", "state unavailable", "state unavailable"},
		{"checkpoint failure", "checkpoint.error", "save failed", "while checkpointing sandbox"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, req, dir := checkpointFixture(t)
			writeCheckpointFixture(t, dir, tc.file, tc.value)
			if _, err := s.CheckpointWorkload(t.Context(), req); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("CheckpointWorkload error = %v, want %q", err, tc.want)
			}
			assertCheckpointCommands(t, dir, tc.file == "checkpoint.error", true)
		})
	}
}

func TestCheckpointWorkloadRejectsUnpinnedSandbox(t *testing.T) {
	s, req, dir := checkpointFixture(t)
	if err := os.Remove(pidFilePath(req.ActorDirs, ocispec.SandboxID)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.CheckpointWorkload(t.Context(), req); err == nil || !strings.Contains(err.Error(), "pinning sandbox") {
		t.Fatalf("CheckpointWorkload error = %v, want missing sandbox pin", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "calls")); !os.IsNotExist(err) {
		t.Fatalf("runsc was called without a sandbox pin: %v", err)
	}
}

func TestCheckpointWorkloadAbortsAfterFailedPause(t *testing.T) {
	s, req, dir := checkpointFixture(t)
	sandbox, err := openSandboxProcess(req.ActorDirs)
	if err != nil {
		t.Fatal(err)
	}
	defer sandbox.Close()
	// A failed pause may already have frozen the process while its persisted
	// metadata still says running, so runsc resume cannot undo it.
	if err := unix.PidfdSendSignal(sandbox.pidfd, unix.SIGSTOP, nil, 0); err != nil {
		t.Fatal(err)
	}
	writeCheckpointFixture(t, dir, "pause.error", "saving paused metadata failed")
	if _, err := s.CheckpointWorkload(t.Context(), req); err == nil || !strings.Contains(err.Error(), "while pausing sandbox") {
		t.Fatalf("CheckpointWorkload error = %v, want pause failure", err)
	}
	if err := sandbox.waitGone(time.Second); err != nil {
		t.Fatalf("sandbox survived the failed pause: %v", err)
	}
	assertCheckpointCommands(t, dir, false, false)
}

func TestCheckpointWorkloadUnwindsCanceledFreeze(t *testing.T) {
	for _, stage := range []string{"pause", "state"} {
		t.Run(stage, func(t *testing.T) {
			s, req, dir := checkpointFixture(t)
			sandbox, err := openSandboxProcess(req.ActorDirs)
			if err != nil {
				t.Fatal(err)
			}
			defer sandbox.Close()
			if err := unix.Mkfifo(filepath.Join(dir, stage+".block"), 0o600); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(t.Context())
			watchDone := make(chan struct{})
			defer func() { cancel(); <-watchDone }()
			go func() {
				defer close(watchDone)
				for {
					if _, err := os.Stat(filepath.Join(dir, stage+".entered")); err == nil {
						if stage == "pause" {
							if err := unix.PidfdSendSignal(sandbox.pidfd, unix.SIGSTOP, nil, 0); err != nil {
								t.Errorf("freeze sandbox: %v", err)
							}
						}
						cancel()
						return
					}
					select {
					case <-ctx.Done():
						return
					case <-time.After(time.Millisecond):
					}
				}
			}()
			if _, err := s.CheckpointWorkload(ctx, req); err == nil {
				t.Fatal("checkpoint succeeded after cancellation")
			}
			if stage == "pause" {
				if err := sandbox.waitGone(time.Second); err != nil {
					t.Fatalf("sandbox survived the canceled pause: %v", err)
				}
			}
			assertCheckpointCommands(t, dir, false, stage == "state")
		})
	}
}

func TestCheckpointWorkloadLiveApplications(t *testing.T) {
	s, req, dir := checkpointFixture(t)
	resp, err := s.CheckpointWorkload(t.Context(), req)
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.GetSnapshotFiles()) != 1 || resp.GetSnapshotFiles()[0] != "checkpoint.img" {
		t.Fatalf("snapshot files = %v, want checkpoint.img", resp.GetSnapshotFiles())
	}
	assertCheckpointCommands(t, dir, true, true)
}

func TestCheckpointWorkloadUnknownExitStatus(t *testing.T) {
	for _, result := range []string{"not json", `{"id":"app"}`, `{"id":"other","exitStatus":0}`} {
		t.Run(result, func(t *testing.T) {
			s, req, dir := checkpointFixture(t)
			writeCheckpointFixture(t, dir, "app.state", `{"id":"app","status":"stopped"}`)
			writeCheckpointFixture(t, dir, "app.wait", result)
			if _, err := s.CheckpointWorkload(t.Context(), req); err == nil || !strings.Contains(err.Error(), "exit code unknown") {
				t.Fatalf("CheckpointWorkload error = %v, want unknown exit code", err)
			} else if got := apierror.Code(err); got != codes.FailedPrecondition {
				t.Fatalf("checkpoint error code = %v, want FailedPrecondition", got)
			}
			assertCheckpointCommands(t, dir, false, true)
		})
	}
}

func TestCheckpointWorkloadResumeFailure(t *testing.T) {
	for _, tc := range []struct {
		name, file, value, want string
		checkpoint, saved       bool
	}{
		{"after application exit", "app.state", `{"id":"app","status":"stopped"}`, "exited before checkpoint", false, false},
		{"after checkpoint failure", "checkpoint.error", "save failed", "while checkpointing sandbox", true, false},
		{"after successful save", "", "", "", true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, req, dir := checkpointFixture(t)
			sandbox, err := openSandboxProcess(req.ActorDirs)
			if err != nil {
				t.Fatal(err)
			}
			defer sandbox.Close()
			if tc.file != "" {
				writeCheckpointFixture(t, dir, tc.file, tc.value)
			}
			writeCheckpointFixture(t, dir, "resume.error", "resume failed")
			resp, err := ateinterceptors.InternalServerUnaryInterceptor(t.Context(), req,
				&grpc.UnaryServerInfo{FullMethod: ateworkerpb.Worker_CheckpointWorkload_FullMethodName},
				func(ctx context.Context, _ any) (any, error) { return s.CheckpointWorkload(ctx, req) })
			st := status.Convert(err)
			if resp != nil || st.Code() != codes.Internal || !strings.Contains(st.Message(), "while resuming sandbox after checkpoint") {
				t.Fatalf("CheckpointWorkload = %v, %v; want Internal with the resume failure", resp, err)
			}
			if !strings.Contains(st.Message(), tc.want) {
				t.Errorf("RPC error = %q, want checkpoint failure %q", st.Message(), tc.want)
			}
			if err := sandbox.waitGone(time.Second); err != nil {
				t.Fatalf("sandbox survived the resume failure: %v", err)
			}
			if tc.saved {
				if _, err := os.Stat(filepath.Join(req.ActorDirs.CheckpointDir, "checkpoint.img")); err != nil {
					t.Fatalf("checkpoint was not saved before the resume failure: %v", err)
				}
			}
			assertCheckpointCommands(t, dir, tc.checkpoint, true)
		})
	}
}

func TestCheckpointWorkloadVolumesDoesNotRequireLiveApplications(t *testing.T) {
	s, req, dir := checkpointFixture(t)
	req.Fidelity = ateworkerpb.SnapshotFidelity_SNAPSHOT_FIDELITY_VOLUMES
	req.Spec.Containers[0].DurableDirVolumeMounts = []*ateworkerpb.VolumeMount{{Name: "data", MountPath: "/data"}}
	writeCheckpointFixture(t, dir, "app.state", `{"id":"app","status":"stopped"}`)
	resp, err := s.CheckpointWorkload(t.Context(), req)
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.GetSnapshotFiles()) != 1 || resp.GetSnapshotFiles()[0] != "fs/fscheckpoint.pb" || len(resp.GetDataSnapshotFiles()) != 1 {
		t.Fatalf("snapshot files = %v, want durable data", resp.GetSnapshotFiles())
	}
	assertCheckpointCommands(t, dir, false, false)
}

func checkpointFixture(t *testing.T) (*AteWorkerService, *ateworkerpb.CheckpointWorkloadRequest, string) {
	t.Helper()
	origActorsDir := nodepath.ActorsDir
	nodepath.ActorsDir = t.TempDir()
	t.Cleanup(func() { nodepath.ActorsDir = origActorsDir })
	dir := nodepath.ActorSandboxAssetsDir("123e4567-e89b-12d3-a456-426614174000")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	script, err := os.ReadFile("testdata/checkpoint-runsc.sh")
	if err != nil {
		t.Fatal(err)
	}
	writeCheckpointFixture(t, dir, "runsc", string(script))
	if err := os.Chmod(filepath.Join(dir, "runsc"), 0o700); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"app", "sidecar", "_sandbox"} {
		writeCheckpointFixture(t, dir, name+".state", `{"id":"`+name+`","status":"running"}`)
		writeCheckpointFixture(t, dir, name+".wait", `{"id":"`+name+`","exitStatus":0}`)
	}
	dirs := &ateworkerpb.ActorDirs{
		RootDir: dir, OciBundleDir: filepath.Join(dir, "bundles"),
		CheckpointDir: filepath.Join(dir, "checkpoint"), RestoreDir: filepath.Join(dir, "restore"),
		DurableDirVolumeMountsDir: filepath.Join(dir, "durable"),
		SystemInfoVolumeRootsDir:  filepath.Join(dir, "system-info"), ExternalVolumeMountsDir: filepath.Join(dir, "volumes"),
		SandboxAssetsDir: dir,
	}
	if err := unix.Mkfifo(filepath.Join(dir, "sandbox.exit"), 0o600); err != nil {
		t.Fatal(err)
	}
	sandbox := exec.Command("cat", filepath.Join(dir, "sandbox.exit"))
	if err := sandbox.Start(); err != nil {
		t.Fatal(err)
	}
	reaped := make(chan struct{})
	go func() { _ = sandbox.Wait(); close(reaped) }()
	t.Cleanup(func() { _ = sandbox.Process.Kill(); <-reaped })
	mustMkdir(t, pidFileDir(dirs))
	mustWrite(t, pidFilePath(dirs, ocispec.SandboxID), strconv.Itoa(sandbox.Process.Pid))
	s := &AteWorkerService{
		locks: actorlock.New(), inFlight: actorlock.NewInFlight(),
		actorLogger: actorlog.NewActorLogger(io.Discard, false),
		tunnel:      &atunnel.Tunnel{Ingress: &ingress.Server{}, Egress: &egress.Proxy{}},
		flags:       mustFlagStore(t),
	}
	req := &ateworkerpb.CheckpointWorkloadRequest{
		Atespace: "ns1", ActorName: "actor-a", ActorUid: "123e4567-e89b-12d3-a456-426614174000",
		ActorDirs: dirs, RunscPath: filepath.Join(dir, "runsc"),
		Fidelity: ateworkerpb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY,
		Spec:     &ateworkerpb.WorkloadSpec{Containers: []*ateworkerpb.Container{{Name: "app"}, {Name: "sidecar"}}},
	}
	return s, req, dir
}

func writeCheckpointFixture(t *testing.T, dir, name, value string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, name), []byte(value), 0o600); err != nil {
		t.Fatal(err)
	}
}

func assertCheckpointCommands(t *testing.T, dir string, checkpoint, resume bool) {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(dir, "calls"))
	if err != nil {
		t.Fatal(err)
	}
	calls := string(b)
	// The leading newline pins the match to a command boundary: fscheckpoint
	// lines must not count as memory checkpoints.
	if got := strings.Contains(calls, "\ncheckpoint -image-path"); got != checkpoint {
		t.Errorf("checkpoint called = %v, want %v; commands:\n%s", got, checkpoint, calls)
	}
	if got := strings.Contains(calls, "resume _sandbox"); got != resume {
		t.Errorf("resume called = %v, want %v; commands:\n%s", got, resume, calls)
	}
}

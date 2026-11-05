//go:build linux

// Copyright 2026 The Agent Substrate Authors
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
	"testing"
	"testing/synctest"
	"time"

	"go.ate.dev/substrate/internal/actorlock"
	"go.ate.dev/substrate/internal/actorlog"
	"go.ate.dev/substrate/internal/ateattr"
	"go.ate.dev/substrate/internal/atunnel"
	"go.ate.dev/substrate/internal/atunnel/egress"
	"go.ate.dev/substrate/internal/atunnel/ingress"
	"go.ate.dev/substrate/internal/nodepath"
	"go.ate.dev/substrate/internal/ocispec"
	"go.ate.dev/substrate/internal/proto/ateworkerpb"
	"go.ate.dev/substrate/internal/resources"
	"go.ate.dev/substrate/internal/workerstats"
	"golang.org/x/sys/unix"
)

func TestRemoveActorCgroupsContinuesAndRetries(t *testing.T) {
	for _, blocked := range []string{"_sandbox", "_ckpt", "app"} {
		t.Run(blocked, func(t *testing.T) {
			s := &AteWorkerService{cgroupRoot: t.TempDir()}
			const uid = "actor-a"
			var dirs []string
			for _, name := range []string{"_sandbox", "_ckpt", "app"} {
				dir := filepath.Join(s.cgroupRoot, ocispec.GVisorCgroupLeaf(uid, name))
				if err := os.Mkdir(dir, 0o755); err != nil {
					t.Fatal(err)
				}
				dirs = append(dirs, dir)
			}
			blockedDir := filepath.Join(s.cgroupRoot, ocispec.GVisorCgroupLeaf(uid, blocked))
			marker := filepath.Join(blockedDir, "remaining")
			if err := os.WriteFile(marker, []byte("remaining"), 0o600); err != nil {
				t.Fatal(err)
			}
			if err := s.removeActorCgroups(context.Background(), uid, appContainers); err == nil {
				t.Fatal("cleanup succeeded despite failed cgroup removal")
			}
			for _, dir := range dirs {
				_, err := os.Stat(dir)
				if dir == blockedDir {
					if err != nil {
						t.Errorf("failed cgroup was discarded: %v", err)
					}
				} else if !os.IsNotExist(err) {
					t.Errorf("cleanup skipped %s: %v", dir, err)
				}
			}
			if err := os.Remove(marker); err != nil {
				t.Fatal(err)
			}
			for range 2 {
				if err := s.removeActorCgroups(context.Background(), uid, appContainers); err != nil {
					t.Fatalf("cleanup retry: %v", err)
				}
			}
		})
	}
}

func terminationFixture(t *testing.T) (*AteWorkerService, *hostedActor, *ateworkerpb.ActorDirs, []string) {
	t.Helper()
	attribution := testActor
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	hosted := &hostedActor{attribution: attribution, ctx: ctx, cancel: cancel}
	dirs := &ateworkerpb.ActorDirs{RootDir: t.TempDir(), OciBundleDir: t.TempDir()}
	markers := []string{filepath.Join(runscStateDir(dirs), "state"), pidFilePath(dirs, ocispec.SandboxID)}
	for _, path := range markers {
		if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte("retained"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	s := &AteWorkerService{
		cgroupRoot: t.TempDir(),
		actors:     map[string]*hostedActor{attribution.UID: hosted},
		flags:      &flagStore{cfg: &flagConfig{}},
		tunnel:     &atunnel.Tunnel{Ingress: &ingress.Server{}, Egress: &egress.Proxy{}},
	}
	return s, hosted, dirs, markers
}

func assertTerminationRetained(t *testing.T, s *AteWorkerService, hosted *hostedActor, markers []string) {
	t.Helper()
	if s.lookupActor(hosted.attribution.UID) != hosted || hosted.ctx.Err() != nil {
		t.Error("failed termination discarded the hosted actor")
	}
	for _, path := range markers {
		if data, err := os.ReadFile(path); err != nil || string(data) != "retained" {
			t.Errorf("failed termination discarded %s: data=%q err=%v", path, data, err)
		}
	}
}

func TestTerminateWorkloadRetainsStateUntilCgroupsAreRemoved(t *testing.T) {
	for _, leaf := range []string{"_sandbox", "_ckpt"} {
		t.Run(leaf, func(t *testing.T) {
			s, hosted, dirs, markers := terminationFixture(t)
			uid := hosted.attribution.UID
			cgroupDir := filepath.Join(s.cgroupRoot, ocispec.GVisorCgroupLeaf(uid, leaf))
			if err := os.Mkdir(cgroupDir, 0o755); err != nil {
				t.Fatal(err)
			}
			remaining := filepath.Join(cgroupDir, "remaining")
			if err := os.WriteFile(remaining, []byte("remaining"), 0o600); err != nil {
				t.Fatal(err)
			}
			runscPath := filepath.Join(t.TempDir(), "missing-runsc")
			for range 2 {
				if err := s.terminateWorkload(context.Background(), hosted.attribution.Ref, uid, runscPath, dirs, appContainers, false); err == nil {
					t.Fatal("termination succeeded despite failed cleanup")
				}
				assertTerminationRetained(t, s, hosted, markers)
			}
			if err := os.Remove(remaining); err != nil {
				t.Fatal(err)
			}
			// Runtime command errors still reach the caller once execution is gone.
			if err := s.terminateWorkload(context.Background(), hosted.attribution.Ref, uid, runscPath, dirs, appContainers, false); err == nil {
				t.Fatal("missing runtime executable error was lost")
			}
			if s.lookupActor(uid) != nil {
				t.Error("termination retained the actor after cgroups were removed")
			}
			for _, path := range markers {
				if _, err := os.Stat(path); !os.IsNotExist(err) {
					t.Errorf("termination retained %s: %v", path, err)
				}
			}
		})
	}
}

func TestTerminateWorkloadRetainsLiveSandboxWithoutCgroups(t *testing.T) {
	s, hosted, dirs, markers := terminationFixture(t)
	cmd, sandbox := startFakeSandbox(t)
	fd, err := unix.FcntlInt(uintptr(sandbox.pidfd), unix.F_DUPFD_CLOEXEC, 0)
	if err != nil {
		t.Fatal(err)
	}
	hosted.sandbox = &sandboxProcess{pid: sandbox.pid, pidfd: fd}
	t.Cleanup(func() {
		if hosted.sandbox != nil {
			hosted.sandbox.Close()
			hosted.sandbox = nil
		}
	})
	runscPath := filepath.Join(t.TempDir(), "missing-runsc")
	uid := hosted.attribution.UID
	if err := s.terminateWorkload(context.Background(), hosted.attribution.Ref, uid, runscPath, dirs, appContainers, false); err == nil {
		t.Fatal("termination succeeded with a live sandbox")
	}
	assertTerminationRetained(t, s, hosted, markers)
	if err := cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	_ = cmd.Wait()
	_ = s.terminateWorkload(context.Background(), hosted.attribution.Ref, uid, runscPath, dirs, appContainers, false)
	if s.lookupActor(uid) != nil {
		t.Error("termination retained the actor after sandbox exit")
	}
}

func TestCheckpointRetriesRetainedCleanup(t *testing.T) {
	origActorsDir := nodepath.ActorsDir
	nodepath.ActorsDir = t.TempDir()
	t.Cleanup(func() { nodepath.ActorsDir = origActorsDir })
	synctest.Test(t, func(t *testing.T) {
		s, hosted, dirs, markers := terminationFixture(t)
		uid := hosted.attribution.UID
		s.locks, s.inFlight = actorlock.New(), actorlock.NewInFlight()
		s.maxActors = 1
		s.actorLogger = actorlog.NewActorLogger(io.Discard, false)
		hosted.usage = workerstats.NewActivation(time.Now(), false)
		rec := withUsageRecorder(s)
		leaf := s.sandboxLeaf(uid)
		mustMkdir(t, leaf)
		const remaining = "remaining"
		blocker := filepath.Join(leaf, remaining)
		mustWrite(t, blocker, remaining)
		actorDirs := testDirs()
		actorDirs.RootDir, actorDirs.OciBundleDir = dirs.RootDir, dirs.OciBundleDir
		actorDirs.CheckpointDir = t.TempDir()
		actorDirs.SandboxAssetsDir = nodepath.ActorSandboxAssetsDir(uid)
		mustMkdir(t, actorDirs.SandboxAssetsDir)
		runscPath := filepath.Join(actorDirs.SandboxAssetsDir, "runsc")
		truePath, err := exec.LookPath("true")
		if err != nil {
			t.Fatal(err)
		}
		fixture, err := os.ReadFile(truePath)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(runscPath, fixture, 0o700); err != nil {
			t.Fatal(err)
		}
		caller, cancel := context.WithCancel(t.Context())
		defer cancel()
		req := &ateworkerpb.CheckpointWorkloadRequest{
			Atespace: hosted.attribution.Ref.Atespace, ActorName: hosted.attribution.Ref.Name,
			ActorUid: uid, Spec: testSpec(), ActorDirs: actorDirs, RunscPath: runscPath,
			Fidelity: ateworkerpb.SnapshotFidelity_SNAPSHOT_FIDELITY_ROOTFS,
		}
		if _, err := s.CheckpointWorkload(caller, req); err != nil {
			t.Fatal(err)
		}
		cancel()
		synctest.Wait()
		assertTerminationRetained(t, s, hosted, markers)
		if _, _, err := s.admitActor(resources.ActorAttribution{UID: otherActorUID}, "", 0); err == nil {
			t.Fatal("retained execution released capacity")
		}
		// Atelet can remove runtime files after the checkpoint returns.
		if err := os.Remove(runscPath); err != nil {
			t.Fatal(err)
		}
		if err := resetRunscStateAndPidFileDirs(actorDirs); err != nil {
			t.Fatal(err)
		}
		if err := os.Remove(blocker); err != nil {
			t.Fatal(err)
		}
		select {
		case <-hosted.ctx.Done():
		case <-time.After(5 * time.Second):
			t.Fatal("checkpoint cleanup did not release the actor")
		}
		synctest.Wait()
		if s.lookupActor(uid) != nil {
			t.Fatal("checkpoint cleanup retained the actor")
		}
		if _, err := os.Stat(leaf); !os.IsNotExist(err) {
			t.Errorf("checkpoint cleanup retained its cgroup: %v", err)
		}
		if got := rec.Kinds(); len(got) != 1 || got[0] != ateattr.StatsKindFinal {
			t.Errorf("usage records = %v, want one final record", got)
		}
		next, _, err := s.admitActor(resources.ActorAttribution{UID: otherActorUID}, "", 0)
		if err != nil {
			t.Fatalf("checkpoint cleanup did not release capacity: %v", err)
		}
		t.Cleanup(next.cancel)
	})
}

func TestCheckpointCleanupDoesNotStopReplacement(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s, hosted, dirs, markers := terminationFixture(t)
		s.locks = actorlock.New()
		uid := hosted.attribution.UID
		if !s.locks.Lock(t.Context(), uid) {
			t.Fatal("could not lock the actor")
		}
		s.retryCheckpointTermination(hosted, "", dirs, appContainers)
		s.retryCheckpointTermination(hosted, "", dirs, appContainers)
		synctest.Wait()
		replacement, _, err := s.admitActor(hosted.attribution, "", 0)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(replacement.cancel)
		s.locks.Unlock(uid)
		synctest.Wait()
		assertTerminationRetained(t, s, replacement, markers)
	})
}

func TestCheckpointCleanupRetryBudget(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s, hosted, dirs, markers := terminationFixture(t)
		s.locks = actorlock.New()
		s.maxActors = 1
		hosted.usage = workerstats.NewActivation(time.Now(), false)
		rec := withUsageRecorder(s)
		uid := hosted.attribution.UID
		leaf := s.sandboxLeaf(uid)
		mustMkdir(t, leaf)
		const remaining = "remaining"
		blocker := filepath.Join(leaf, remaining)
		mustWrite(t, blocker, remaining)
		s.retryCheckpointTermination(hosted, "", dirs, appContainers)
		synctest.Wait()
		assertTerminationRetained(t, s, hosted, markers)
		func() {
			if !s.locks.Lock(t.Context(), uid) {
				t.Fatal("could not lock the actor")
			}
			defer s.locks.Unlock(uid)
			// Fake time includes the retry's wait for the actor lock.
			<-time.After(checkpointCleanupRetryBudget)
			synctest.Wait()
			if err := os.Remove(blocker); err != nil {
				t.Fatal(err)
			}
		}()
		// Repeated scheduling must not reset an activation's expired budget.
		s.retryCheckpointTermination(hosted, "", dirs, appContainers)
		<-time.After(checkpointCleanupRetryInterval)
		synctest.Wait()
		assertTerminationRetained(t, s, hosted, markers)
		if _, err := os.Stat(leaf); err != nil {
			t.Errorf("expired retry removed its cgroup: %v", err)
		}
		if _, _, err := s.admitActor(resources.ActorAttribution{UID: otherActorUID}, "", 0); err == nil {
			t.Error("expired retry released capacity")
		}
		if got := rec.Kinds(); len(got) != 0 {
			t.Errorf("usage records = %v, want no final record for retained execution", got)
		}
	})
}

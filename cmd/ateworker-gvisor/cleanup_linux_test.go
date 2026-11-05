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
	"os"
	"path/filepath"
	"testing"

	"go.ate.dev/substrate/internal/atunnel"
	"go.ate.dev/substrate/internal/atunnel/egress"
	"go.ate.dev/substrate/internal/atunnel/ingress"
	"go.ate.dev/substrate/internal/ocispec"
	"go.ate.dev/substrate/internal/proto/ateworkerpb"
	"go.ate.dev/substrate/internal/resources"
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
	attribution := resources.ActorAttribution{Ref: resources.ActorRef{Atespace: "test", Name: "actor"}, UID: "actor-a"}
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
				if err := s.terminateWorkload(context.Background(), hosted.attribution.Ref, uid, runscPath, dirs, appContainers); err == nil {
					t.Fatal("termination succeeded despite failed cleanup")
				}
				assertTerminationRetained(t, s, hosted, markers)
			}
			if err := os.Remove(remaining); err != nil {
				t.Fatal(err)
			}
			// Runtime command errors still reach the caller once execution is gone.
			if err := s.terminateWorkload(context.Background(), hosted.attribution.Ref, uid, runscPath, dirs, appContainers); err == nil {
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
	if err := s.terminateWorkload(context.Background(), hosted.attribution.Ref, uid, runscPath, dirs, appContainers); err == nil {
		t.Fatal("termination succeeded with a live sandbox")
	}
	assertTerminationRetained(t, s, hosted, markers)
	if err := cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	_ = cmd.Wait()
	_ = s.terminateWorkload(context.Background(), hosted.attribution.Ref, uid, runscPath, dirs, appContainers)
	if s.lookupActor(uid) != nil {
		t.Error("termination retained the actor after sandbox exit")
	}
}

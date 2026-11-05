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
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"go.ate.dev/substrate/internal/actorlock"
	"go.ate.dev/substrate/internal/actorlog"
	"go.ate.dev/substrate/internal/atunnel"
	"go.ate.dev/substrate/internal/atunnel/egress"
	"go.ate.dev/substrate/internal/atunnel/ingress"
	"go.ate.dev/substrate/internal/linuxsys"
	"go.ate.dev/substrate/internal/nodepath"
	"go.ate.dev/substrate/internal/ocispec"
	"go.ate.dev/substrate/internal/proto/ateworkerpb"
	"go.ate.dev/substrate/internal/roottest"
	"golang.org/x/sys/unix"
)

type cancelStartupLog struct{ cancel context.CancelFunc }

func (w cancelStartupLog) Write(p []byte) (int, error) {
	if bytes.Contains(p, []byte("cancel-startup")) {
		w.cancel()
	}
	return len(p), nil
}

func TestFailedStartupVerifiesCleanup(t *testing.T) {
	roottest.Require(t, "creates actor network namespaces")
	origActorsDir := nodepath.ActorsDir
	nodepath.ActorsDir = t.TempDir()
	t.Cleanup(func() { nodepath.ActorsDir = origActorsDir })
	fixture, err := os.ReadFile("testdata/fail-create.sh")
	if err != nil {
		t.Fatal(err)
	}
	for _, operation := range []string{"Run", "Restore"} {
		for _, blocked := range []bool{false, true} {
			for _, canceled := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/blocked=%t/canceled=%t", operation, blocked, canceled), func(t *testing.T) {
					ctx, cancel := context.WithCancel(t.Context())
					defer cancel()
					proxy, err := egress.New(linuxsys.TCPOriginalDestination)
					if err != nil {
						t.Fatal(err)
					}
					s := &AteWorkerService{
						locks:       actorlock.New(),
						inFlight:    actorlock.NewInFlight(),
						actors:      map[string]*hostedActor{},
						maxActors:   1,
						cgroupRoot:  t.TempDir(),
						flags:       &flagStore{cfg: &flagConfig{}},
						tunnel:      &atunnel.Tunnel{Ingress: &ingress.Server{}, Egress: proxy, EgressPort: 18080},
						actorLogger: actorlog.NewActorLogger(cancelStartupLog{cancel}, false),
					}
					s.epoch.Store(1)
					t.Cleanup(func() {
						if err := s.unhostActor(context.Background(), testActor.UID); err != nil {
							t.Error(err)
						}
					})
					dirs := testDirs()
					dirs.RootDir = t.TempDir()
					dirs.OciBundleDir = filepath.Join(dirs.RootDir, "bundles")
					dirs.RestoreDir = filepath.Join(dirs.RootDir, "restore")
					if err := os.MkdirAll(dirs.SandboxAssetsDir, 0o700); err != nil {
						t.Fatal(err)
					}
					runscPath := filepath.Join(dirs.SandboxAssetsDir, "runsc")
					if err := os.WriteFile(runscPath, fixture, 0o700); err != nil {
						t.Fatal(err)
					}
					cgroupDir := filepath.Join(s.cgroupRoot, ocispec.GVisorCgroupLeaf(testActor.UID, ocispec.SandboxID))
					t.Setenv("CLEANUP_TEST_CGROUP", cgroupDir)
					t.Setenv("CLEANUP_TEST_BLOCKED", strconv.FormatBool(blocked))
					t.Setenv("CLEANUP_TEST_CANCEL", strconv.FormatBool(canceled))
					switch operation {
					case "Run":
						req := validRunRequest()
						req.ActorDirs, req.RunscPath = dirs, runscPath
						_, err = s.RunWorkload(ctx, req)
					case "Restore":
						req := validRestoreRequest()
						req.ActorDirs, req.RunscPath = dirs, runscPath
						req.Fidelity = ateworkerpb.SnapshotFidelity_SNAPSHOT_FIDELITY_VOLUMES
						_, err = s.RestoreWorkload(ctx, req)
					}
					if err == nil || !strings.Contains(err.Error(), "while creating the sandbox") {
						t.Fatalf("startup returned %v, want sandbox create failure", err)
					}
					if _, markerErr := os.Stat(cgroupDir + ".created"); markerErr != nil {
						t.Fatalf("startup did not reach the runtime fixture: %v; startup error: %v", markerErr, err)
					}
					if canceled && ctx.Err() == nil {
						t.Fatal("startup did not reach the cancellation barrier")
					}
					if !strings.Contains(err.Error(), "while cleaning up runsc containers") {
						t.Errorf("startup discarded the runtime cleanup error: %v", err)
					}
					markers := []string{filepath.Join(runscStateDir(dirs), "state"), pidFilePath(dirs, ocispec.SandboxID)}
					if blocked {
						hosted := s.lookupActor(testActor.UID)
						if hosted == nil {
							t.Fatal("failed startup discarded the actor before cgroup removal")
						}
						assertTerminationRetained(t, s, hosted, markers)
						if !strings.Contains(err.Error(), "while removing cgroup") {
							t.Errorf("startup discarded the cgroup cleanup error: %v", err)
						}
						return
					}
					if s.lookupActor(testActor.UID) != nil {
						t.Error("failed startup retained the actor after execution cleanup")
					}
					for _, path := range append(markers, cgroupDir) {
						if _, err := os.Stat(path); !os.IsNotExist(err) {
							t.Errorf("failed startup retained %s: %v", path, err)
						}
					}
				})
			}
		}
	}
}

func TestStartupPreservesUnverifiedExecution(t *testing.T) {
	origActorsDir := nodepath.ActorsDir
	nodepath.ActorsDir = t.TempDir()
	t.Cleanup(func() { nodepath.ActorsDir = origActorsDir })
	for _, operation := range []string{"Run", "Restore"} {
		for _, blocked := range []string{"_sandbox", "_ckpt", "live sandbox"} {
			t.Run(operation+"/"+blocked, func(t *testing.T) {
				s, hosted, fixtureDirs, markers := terminationFixture(t)
				delete(s.actors, hosted.attribution.UID)
				hosted.attribution = testActor
				s.actors[testActor.UID] = hosted
				s.locks, s.inFlight = actorlock.New(), actorlock.NewInFlight()
				s.actorLogger = actorlog.NewActorLogger(io.Discard, false)
				s.epoch.Store(1)
				if blocked == "live sandbox" {
					_, sandbox := startFakeSandbox(t)
					fd, err := unix.FcntlInt(uintptr(sandbox.pidfd), unix.F_DUPFD_CLOEXEC, 0)
					if err != nil {
						t.Fatal(err)
					}
					hosted.sandbox = &sandboxProcess{pid: sandbox.pid, pidfd: fd}
					t.Cleanup(func() {
						if hosted.sandbox != nil {
							hosted.sandbox.Close()
						}
					})
				} else {
					cgroupDir := filepath.Join(s.cgroupRoot, ocispec.GVisorCgroupLeaf(testActor.UID, blocked))
					if err := os.Mkdir(cgroupDir, 0o700); err != nil {
						t.Fatal(err)
					}
					if err := os.WriteFile(filepath.Join(cgroupDir, "remaining"), nil, 0o600); err != nil {
						t.Fatal(err)
					}
				}
				dirs := testDirs()
				dirs.RootDir, dirs.OciBundleDir = fixtureDirs.RootDir, fixtureDirs.OciBundleDir
				if err := os.MkdirAll(dirs.SandboxAssetsDir, 0o700); err != nil {
					t.Fatal(err)
				}
				runscPath := filepath.Join(dirs.SandboxAssetsDir, "runsc")
				if err := os.WriteFile(runscPath, nil, 0o700); err != nil {
					t.Fatal(err)
				}
				for range 2 {
					var err error
					switch operation {
					case "Run":
						req := validRunRequest()
						req.ActorDirs, req.RunscPath = dirs, runscPath
						_, err = s.RunWorkload(t.Context(), req)
					case "Restore":
						req := validRestoreRequest()
						req.ActorDirs, req.RunscPath = dirs, runscPath
						_, err = s.RestoreWorkload(t.Context(), req)
					}
					if err == nil {
						t.Fatal("startup replaced unverified execution")
					}
					wantError := "while removing cgroup"
					if blocked == "live sandbox" {
						wantError = "has not exited"
					}
					if !strings.Contains(err.Error(), wantError) {
						t.Fatalf("startup returned %v, want %q", err, wantError)
					}
					assertTerminationRetained(t, s, hosted, markers)
					if blocked == "live sandbox" && (hosted.sandbox == nil || hosted.sandbox.checkExited() == nil) {
						t.Error("startup discarded the live sandbox pidfd")
					}
				}
			})
		}
	}
}

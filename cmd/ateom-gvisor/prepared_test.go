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
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/opencontainers/runtime-spec/specs-go"
	"github.com/vishvananda/netns"
	"golang.org/x/sys/unix"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/agent-substrate/substrate/internal/actorlog"
	"github.com/agent-substrate/substrate/internal/ateomnet"
	"github.com/agent-substrate/substrate/internal/ateompath"
	"github.com/agent-substrate/substrate/internal/atunnel"
	"github.com/agent-substrate/substrate/internal/imagecache"
	"github.com/agent-substrate/substrate/internal/proto/ateompb"
	"github.com/agent-substrate/substrate/internal/roottest"
)

func TestPreparedSandboxRejectsMismatchedRun(t *testing.T) {
	s := &AteomService{
		lock: newCancelableMutex(),
		prepared: &preparedSandbox{
			actorUID:    "actor-a",
			runscPath:   "/runsc",
			cpuMilli:    1000,
			memoryBytes: 1 << 30,
		},
	}
	req := &ateompb.PrepareSandboxRequest{ActorUid: "actor-a", RunscPath: "/runsc", CpuMilli: 1000, MemoryBytes: 1 << 30}
	if _, err := s.PrepareSandbox(context.Background(), req); err != nil {
		t.Fatalf("idempotent PrepareSandbox: %v", err)
	}
	_, err := s.RunWorkload(context.Background(), &ateompb.RunWorkloadRequest{
		ActorUid: "actor-a", RunscPath: "/runsc", CpuMilli: 500, MemoryBytes: 1 << 30,
	})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("mismatched RunWorkload code = %v, want %v", status.Code(err), codes.FailedPrecondition)
	}
}

// TestPreparedSandboxLifecycleWithRunsc crosses the actual gVisor boundary:
// PrepareSandbox boots the Sentry, then RunWorkload adds an application to it.
// Set RUNSC_TEST_BINARY and run this test as root (a throwaway user/net/mount
// namespace is sufficient).
func TestPreparedSandboxLifecycleWithRunsc(t *testing.T) {
	runscPath := os.Getenv("RUNSC_TEST_BINARY")
	if runscPath == "" {
		t.Skip("set RUNSC_TEST_BINARY to exercise the real gVisor lifecycle")
	}
	roottest.Require(t, "creating network namespaces, veth pairs, and nftables rules")
	runscPath, err := filepath.Abs(runscPath)
	if err != nil {
		t.Fatal(err)
	}

	withPreparedSandboxNetNS(t, func(interior netns.NsHandle) {
		oldActorsDir := ateompath.ActorsDir
		ateompath.ActorsDir = t.TempDir()
		defer func() { ateompath.ActorsDir = oldActorsDir }()

		const actorUID = "prepared-sandbox-test"
		for _, dir := range []string{ateompath.PIDFileDir(actorUID), ateompath.RunSCStateDir(actorUID)} {
			if err := os.MkdirAll(dir, 0o700); err != nil {
				t.Fatal(err)
			}
		}
		writeRunscTestBundle(t, actorUID, "pause", "sandbox")
		writeRunscTestBundle(t, actorUID, "app", "container")

		wrapper := filepath.Join(t.TempDir(), "runsc")
		script := fmt.Sprintf("#!/bin/sh\n"+
			"previous=\n"+
			"for argument in \"$@\"; do\n"+
			"  if [ \"$previous\" = \"-bundle\" ]; then\n"+
			"    sed -i '/\"cgroupsPath\":/d' \"$argument/config.json\"\n"+
			"  fi\n"+
			"  previous=\"$argument\"\n"+
			"done\n"+
			"exec %q --TESTONLY-unsafe-nonroot --network=none --ignore-cgroups \"$@\"\n", runscPath)
		if err := os.WriteFile(wrapper, []byte(script), 0o700); err != nil {
			t.Fatal(err)
		}

		s := NewService(interior, actorlog.NewActorLogger(io.Discard, false), &atunnel.Server{}, &atunnel.Egress{}, 0, "", "", "")
		ctx := context.Background()
		rcmd := &runsc{path: wrapper, actorUID: actorUID}
		if _, err := s.PrepareSandbox(ctx, &ateompb.PrepareSandboxRequest{ActorUid: actorUID, RunscPath: wrapper}); err != nil {
			t.Fatalf("PrepareSandbox: %v", err)
		}
		defer func() {
			_ = rcmd.cmdDelete(context.Background(), "app")
			_ = rcmd.cmdDelete(context.Background(), "pause")
			_ = unix.Unmount(filepath.Join(ateompath.RunSCStateDir(actorUID), "null-netns"), unix.MNT_DETACH)
			_ = imagecache.UnmountAllUnder(ateompath.OCIBundleDir(actorUID))
			_ = ateomnet.CleanupActorNetwork(context.Background(), interior)
		}()
		if err := rcmd.cmdState(ctx, "pause"); err != nil {
			t.Fatalf("prepared pause container is not running: %v", err)
		}
		// Fail after the pause container is deleted, then verify that retrying
		// finishes the remaining cleanup without trying to delete it again.
		closedNetNS, err := netns.Get()
		if err != nil {
			t.Fatal(err)
		}
		closedNetNS.Close()
		s.interiorNetNS = closedNetNS
		if _, err := s.DiscardPreparedSandbox(ctx, &ateompb.DiscardPreparedSandboxRequest{ActorUid: actorUID}); err == nil {
			t.Fatal("DiscardPreparedSandbox succeeded with a closed network namespace")
		}
		s.interiorNetNS = interior
		if _, err := s.DiscardPreparedSandbox(ctx, &ateompb.DiscardPreparedSandboxRequest{ActorUid: actorUID}); err != nil {
			t.Fatalf("retrying DiscardPreparedSandbox: %v", err)
		}
		if err := rcmd.cmdState(ctx, "pause"); err == nil {
			t.Fatal("pause container still exists after DiscardPreparedSandbox")
		}
		if _, err := s.PrepareSandbox(ctx, &ateompb.PrepareSandboxRequest{ActorUid: actorUID, RunscPath: wrapper}); err != nil {
			t.Fatalf("PrepareSandbox after discard: %v", err)
		}
		if err := rcmd.cmdState(ctx, "app"); err == nil {
			t.Fatal("application started before RunWorkload")
		}

		if _, err := s.RunWorkload(ctx, &ateompb.RunWorkloadRequest{
			Atespace: "test", ActorName: "actor", ActorUid: actorUID, RunscPath: wrapper,
			Spec: &ateompb.WorkloadSpec{Containers: []*ateompb.Container{{Name: "app"}}},
		}); err != nil {
			t.Fatalf("RunWorkload: %v", err)
		}
		if err := rcmd.cmdState(ctx, "app"); err != nil {
			t.Fatalf("application did not join the prepared sandbox: %v", err)
		}
	})
}

func withPreparedSandboxNetNS(t *testing.T, fn func(netns.NsHandle)) {
	t.Helper()
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	original, err := netns.Get()
	if err != nil {
		t.Fatal(err)
	}
	defer original.Close()
	defer func() {
		if err := netns.Set(original); err != nil {
			t.Errorf("restoring original netns: %v", err)
		}
	}()
	pod, err := netns.New()
	if err != nil {
		t.Fatal(err)
	}
	defer pod.Close()
	interior, err := netns.New()
	if err != nil {
		t.Fatal(err)
	}
	defer interior.Close()
	if err := netns.Set(pod); err != nil {
		t.Fatal(err)
	}
	fn(interior)
}

func writeRunscTestBundle(t *testing.T, actorUID, name, containerType string) {
	t.Helper()
	bundle := ateompath.OCIBundlePath(actorUID, name)
	binDir := filepath.Join(bundle, "rootfs", "bin")
	if err := os.MkdirAll(binDir, 0o700); err != nil {
		t.Fatal(err)
	}
	busybox, err := os.ReadFile("/bin/busybox")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(binDir, "busybox"), busybox, 0o700); err != nil {
		t.Fatal(err)
	}
	annotations := map[string]string{
		"io.kubernetes.cri.container-type": containerType,
		"io.kubernetes.cri.container-name": name,
	}
	if containerType == "container" {
		annotations["io.kubernetes.cri.sandbox-id"] = "pause"
	}
	spec := specs.Spec{
		Version: specs.Version,
		Process: &specs.Process{User: specs.User{}, Args: []string{"/bin/busybox", "sh", "-c", "while :; do /bin/busybox sleep 3600; done"}, Cwd: "/"},
		Root:    &specs.Root{Path: "rootfs"},
		Linux: &specs.Linux{Namespaces: []specs.LinuxNamespace{
			{Type: specs.PIDNamespace}, {Type: specs.NetworkNamespace}, {Type: specs.IPCNamespace}, {Type: specs.UTSNamespace}, {Type: specs.MountNamespace},
		}},
		Annotations: annotations,
	}
	data, err := json.Marshal(spec)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(bundle, "config.json"), data, 0o600); err != nil {
		t.Fatal(err)
	}
}

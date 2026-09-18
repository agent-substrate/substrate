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

//go:build linux

package main

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/internal/ateompath"
	"github.com/agent-substrate/substrate/internal/proto/ateompb"
	"github.com/agent-substrate/substrate/internal/sandboxd"
)

func TestRestoreUsesFreshRuntimeIDs(t *testing.T) {
	spec := &ateompb.WorkloadSpec{Containers: []*ateompb.Container{{Name: "app"}}}
	const netNSPath = "/run/netns/ateom:test"
	source, err := buildSandbox("ns", "template", "actor", spec, false, netNSPath)
	if err != nil {
		t.Fatal(err)
	}
	restored, err := buildSandbox("ns", "template", "actor", spec, true, netNSPath)
	if err != nil {
		t.Fatal(err)
	}
	if source.SandboxID == restored.SandboxID || source.Tasks[0].TaskID == restored.Tasks[0].TaskID {
		t.Fatalf("restore reused source IDs: source=%+v restored=%+v", source, restored)
	}
	if source.Tasks[0].CheckpointKey != restored.Tasks[0].CheckpointKey {
		t.Fatal("restore changed checkpoint key")
	}
	if source.NetNSPath != netNSPath || restored.NetNSPath != netNSPath {
		t.Fatalf("netns paths = %q, %q; want %q", source.NetNSPath, restored.NetNSPath, netNSPath)
	}
}

type fakeActorNetwork struct {
	path     string
	cleanups int
}

type trackingRuntime struct {
	deletes   int
	drains    int
	deleteErr error
}

func (*trackingRuntime) Run(context.Context, *sandboxd.Sandbox) error { return nil }
func (*trackingRuntime) Checkpoint(context.Context, string, string, string, map[string]string) error {
	return nil
}
func (*trackingRuntime) Restore(_ context.Context, sb *sandboxd.Sandbox, _, _ string) error {
	// Let the FIFO consumers created by prepareSandboxIO connect and exit.
	for _, task := range sb.Tasks {
		for _, path := range []string{task.Stdout, task.Stderr} {
			f, err := os.OpenFile(path, os.O_WRONLY, 0)
			if err != nil {
				return err
			}
			if err := f.Close(); err != nil {
				return err
			}
		}
	}
	return nil
}
func (r *trackingRuntime) Delete(context.Context, string, []string) error {
	r.deletes++
	return r.deleteErr
}
func (r *trackingRuntime) Drain(context.Context, *sandboxd.Sandbox) error {
	r.drains++
	return nil
}
func (*trackingRuntime) Stats(context.Context, *sandboxd.Sandbox) (*sandboxd.TaskStats, error) {
	return &sandboxd.TaskStats{MemoryCurrentBytes: 10, MemoryPeakBytes: 20, MemoryWorkingSetBytes: 8, CPUUsageUsec: 30}, nil
}

func (*fakeActorNetwork) Setup(context.Context) error { return nil }
func (n *fakeActorNetwork) Cleanup(context.Context) error {
	n.cleanups++
	return nil
}
func (n *fakeActorNetwork) Path() string { return n.path }

func TestRejectIfActorRunning(t *testing.T) {
	svc := &service{runs: map[string]*sandboxd.Sandbox{}}
	if err := svc.rejectIfActorRunning("requested"); err != nil {
		t.Fatalf("empty worker rejected activation: %v", err)
	}
	svc.runs["active"] = &sandboxd.Sandbox{SandboxID: "sandbox-1"}
	if err := svc.rejectIfActorRunning("requested"); err == nil {
		t.Fatal("worker accepted a second actor")
	}
}

func TestValidateKataRuntimeAssetPaths(t *testing.T) {
	bundle := filepath.Join(t.TempDir(), "runtime-bundle")
	if err := os.MkdirAll(filepath.Join(bundle, "bin"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(bundle, "share"), 0o700); err != nil {
		t.Fatal(err)
	}
	paths := map[string]string{
		kataRuntimeBundleAssetName: bundle,
		"kata-shim":                filepath.Join(bundle, "bin", "containerd-shim-kata-v2"),
		"kata-qemu":                filepath.Join(bundle, "bin", "qemu-system-x86_64"),
		"kata-kernel":              filepath.Join(bundle, "share", "vmlinux"),
		"kata-initrd":              filepath.Join(bundle, "share", "kata.initrd"),
	}
	for name, path := range paths {
		if name == kataRuntimeBundleAssetName {
			continue
		}
		mode := os.FileMode(0o600)
		if name == "kata-shim" || name == "kata-qemu" {
			mode = 0o700
		}
		if err := os.WriteFile(path, []byte(name), mode); err != nil {
			t.Fatal(err)
		}
	}
	config := filepath.Join(t.TempDir(), "configuration.toml")
	paths[kataRuntimeConfigAssetName] = config
	configText := "path = \"" + paths["kata-qemu"] + "\"\n" +
		"kernel = \"" + paths["kata-kernel"] + "\"\n" +
		"initrd = \"" + paths["kata-initrd"] + "\"\n" +
		"shared_fs = \"none\"\npassfd_listener_port = 0\n"
	if err := os.WriteFile(config, []byte(configText), 0o600); err != nil {
		t.Fatal(err)
	}
	identity, err := validateKataRuntimeAssetPaths(paths)
	if err != nil {
		t.Fatal(err)
	}
	if identity == "" {
		t.Fatal("runtime identity is empty")
	}

	t.Run("rejects host defaults", func(t *testing.T) {
		bad := mapsClone(paths)
		badConfig := filepath.Join(t.TempDir(), "configuration.toml")
		bad[kataRuntimeConfigAssetName] = badConfig
		if err := os.WriteFile(badConfig, []byte(configText+"firmware = \"/etc/kata-containers/firmware.fd\"\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := validateKataRuntimeAssetPaths(bad); err == nil {
			t.Fatal("host-default runtime config accepted")
		}
	})

	t.Run("rejects bundle escape", func(t *testing.T) {
		bad := mapsClone(paths)
		outside := filepath.Join(t.TempDir(), "qemu")
		if err := os.WriteFile(outside, []byte("qemu"), 0o700); err != nil {
			t.Fatal(err)
		}
		bad["kata-qemu"] = outside
		if _, err := validateKataRuntimeAssetPaths(bad); err == nil {
			t.Fatal("runtime file outside verified bundle accepted")
		}
	})
}

func mapsClone(in map[string]string) map[string]string {
	out := make(map[string]string, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}

func TestCheckpointFiles(t *testing.T) {
	root := t.TempDir()
	if err := os.Mkdir(filepath.Join(root, "vm"), 0o700); err != nil {
		t.Fatal(err)
	}
	for name, content := range map[string]string{
		"manifest.json": "manifest",
		"vm/memory":     "memory",
		"vm/state":      "state",
	} {
		if err := os.WriteFile(filepath.Join(root, filepath.FromSlash(name)), []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
	}

	got, err := checkpointFiles(root)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"manifest.json", "vm/memory", "vm/state"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("checkpointFiles() = %v, want %v", got, want)
	}
}

func TestCheckpointFilesRejectsUnsafeEntries(t *testing.T) {
	if _, err := checkpointFiles("relative/checkpoint"); err == nil {
		t.Fatal("relative checkpoint root accepted")
	}

	root := t.TempDir()
	outside := filepath.Join(t.TempDir(), "outside")
	if err := os.WriteFile(outside, []byte("outside"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(root, "state")); err != nil {
		t.Fatal(err)
	}
	if _, err := checkpointFiles(root); err == nil {
		t.Fatal("checkpoint symlink accepted")
	}
}

type checkpointRuntime struct{}

func (*checkpointRuntime) Run(context.Context, *sandboxd.Sandbox) error { return nil }
func (*checkpointRuntime) Restore(context.Context, *sandboxd.Sandbox, string, string) error {
	return nil
}
func (*checkpointRuntime) Delete(context.Context, string, []string) error { return nil }
func (*checkpointRuntime) Checkpoint(_ context.Context, _ string, output, _ string, _ map[string]string) error {
	if err := os.MkdirAll(output, 0o700); err != nil {
		return err
	}
	return os.Symlink("missing", filepath.Join(output, "unsafe-link"))
}

func TestCheckpointDropsRunningStateAfterSuccessfulDelete(t *testing.T) {
	const namespace, template, actorID = "ns", "template", "checkpoint-state-test"
	checkpointDir := ateompath.CheckpointStateDir(actorID)
	if err := os.RemoveAll(checkpointDir); err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(checkpointDir)

	key := actorKey(namespace, template, actorID)
	network := &fakeActorNetwork{path: "/run/netns/ateom:test"}
	svc := newService(&checkpointRuntime{}, network)
	svc.runs[key] = &sandboxd.Sandbox{
		SandboxID: "sandbox-1",
		Tasks:     []*sandboxd.Task{{CheckpointKey: "app", TaskID: "task-1"}},
	}
	_, err := svc.CheckpointWorkload(context.Background(), &ateompb.CheckpointWorkloadRequest{
		ActorTemplateAtespace: namespace,
		ActorTemplateName:     template,
		ActorUid:              actorID,
		OperationId:           "8d830f4d-0813-44ef-8495-3d95af80e27c",
	})
	if err == nil {
		t.Fatal("checkpoint accepted an unsafe output entry")
	}
	if _, exists := svc.runs[key]; exists {
		t.Fatal("actor remained marked running after successful sandbox deletion")
	}
	if network.cleanups != 1 {
		t.Fatalf("actor netns cleanup count = %d, want 1", network.cleanups)
	}
}

func TestRestoreReadinessFailureRollsBackRuntime(t *testing.T) {
	origActorsDir := ateompath.ActorsDir
	ateompath.ActorsDir = t.TempDir()
	t.Cleanup(func() { ateompath.ActorsDir = origActorsDir })

	const namespace, template, actorID = "ns", "template", "restore-ready-test"
	if err := os.MkdirAll(ateompath.RestoreStateDir(actorID), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(ateompath.OCIBundlePath(actorID, "app"), 0o700); err != nil {
		t.Fatal(err)
	}
	runtime := &trackingRuntime{}
	network := &fakeActorNetwork{path: "/run/netns/ateom:test"}
	svc := newService(runtime, network)
	wantErr := errors.New("not ready")
	svc.waitReady = func(context.Context, []*ateompb.Container, string) error {
		return wantErr
	}

	_, err := svc.RestoreWorkload(context.Background(), &ateompb.RestoreWorkloadRequest{
		ActorTemplateAtespace: namespace,
		ActorTemplateName:     template,
		ActorUid:              actorID,
		OperationId:           "8d830f4d-0813-44ef-8495-3d95af80e27c",
		Spec:                  &ateompb.WorkloadSpec{Containers: []*ateompb.Container{{Name: "app"}}},
	})
	if !errors.Is(err, wantErr) {
		t.Fatalf("RestoreWorkload error = %v, want readiness failure", err)
	}
	if runtime.deletes != 1 {
		t.Fatalf("runtime Delete calls = %d, want 1", runtime.deletes)
	}
	if network.cleanups != 1 {
		t.Fatalf("network Cleanup calls = %d, want 1", network.cleanups)
	}
	if len(svc.runs) != 0 {
		t.Fatalf("running state survived successful rollback: %+v", svc.runs)
	}
}

func TestTerminateWorkloadDeletesRuntimeAndIsIdempotent(t *testing.T) {
	origActorsDir := ateompath.ActorsDir
	ateompath.ActorsDir = t.TempDir()
	t.Cleanup(func() { ateompath.ActorsDir = origActorsDir })

	const namespace, template, actorID = "ns", "template", "terminate-test"
	key := actorKey(namespace, template, actorID)
	runtime := &trackingRuntime{}
	network := &fakeActorNetwork{path: "/run/netns/ateom:test"}
	svc := newService(runtime, network)
	svc.runs[key] = &sandboxd.Sandbox{SandboxID: "sandbox-1", Tasks: []*sandboxd.Task{{TaskID: "task-1"}}}
	svc.runClients[key] = runtime

	req := &ateompb.TerminateWorkloadRequest{
		ActorTemplateAtespace: namespace,
		ActorTemplateName:     template,
		ActorUid:              actorID,
	}
	if _, err := svc.TerminateWorkload(context.Background(), req); err != nil {
		t.Fatalf("TerminateWorkload: %v", err)
	}
	if runtime.deletes != 1 || network.cleanups != 1 || len(svc.runs) != 0 {
		t.Fatalf("terminate result: deletes=%d cleanups=%d runs=%d", runtime.deletes, network.cleanups, len(svc.runs))
	}
	if _, err := svc.TerminateWorkload(context.Background(), req); err != nil {
		t.Fatalf("idempotent TerminateWorkload: %v", err)
	}
	if runtime.deletes != 1 {
		t.Fatalf("idempotent terminate called Delete again: %d", runtime.deletes)
	}
}

func TestTerminateWorkloadRetainsStateWhenDeleteFails(t *testing.T) {
	const namespace, template, actorID = "ns", "template", "terminate-retry-test"
	key := actorKey(namespace, template, actorID)
	runtime := &trackingRuntime{deleteErr: errors.New("delete failed")}
	svc := newService(runtime, &fakeActorNetwork{path: "/run/netns/ateom:test"})
	svc.runs[key] = &sandboxd.Sandbox{SandboxID: "sandbox-1"}
	svc.runClients[key] = runtime

	_, err := svc.TerminateWorkload(context.Background(), &ateompb.TerminateWorkloadRequest{
		ActorTemplateAtespace: namespace, ActorTemplateName: template, ActorUid: actorID,
	})
	if err == nil {
		t.Fatal("TerminateWorkload succeeded despite runtime Delete failure")
	}
	if svc.runs[key] == nil {
		t.Fatal("terminate dropped retry state after runtime Delete failure")
	}
}

func TestKataStatsAreAttributedToActiveActor(t *testing.T) {
	runtime := &trackingRuntime{}
	svc := newService(runtime, &fakeActorNetwork{path: "/run/netns/test"})
	sb := &sandboxd.Sandbox{SandboxID: "sandbox", Tasks: []*sandboxd.Task{{TaskID: "task"}}}
	svc.setActive("space", "actor", "uid", "templates", "template", "key", sb, runtime)

	resp, err := svc.GetWorkloadStats(context.Background(), &ateompb.GetWorkloadStatsRequest{ActorUid: "uid"})
	if err != nil {
		t.Fatal(err)
	}
	got := resp.GetSample()
	if got.GetSandboxClass() != ateompb.SandboxClass_SANDBOX_CLASS_KATA ||
		got.GetSource() != ateompb.StatsSource_STATS_SOURCE_GUEST_AGENT ||
		got.GetMemoryCurrentBytes() != 10 || got.GetCpuUsageUsec() != 30 ||
		got.GetAtespace() != "space" || got.GetActorName() != "actor" {
		t.Fatalf("unexpected sample: %+v", got)
	}
}

func TestGracefulShutdownDrainsThenDeletes(t *testing.T) {
	runtime := &trackingRuntime{}
	network := &fakeActorNetwork{path: "/run/netns/test"}
	svc := newService(runtime, network)
	sb := &sandboxd.Sandbox{SandboxID: "sandbox", Tasks: []*sandboxd.Task{{TaskID: "task"}}}
	svc.runs["key"] = sb
	svc.runClients["key"] = runtime
	svc.setActive("space", "actor", "uid", "templates", "template", "key", sb, runtime)

	if err := svc.gracefulShutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	if runtime.drains != 1 || runtime.deletes != 1 || network.cleanups != 1 {
		t.Fatalf("drains=%d deletes=%d cleanups=%d", runtime.drains, runtime.deletes, network.cleanups)
	}
	if svc.active.value.Load() != nil {
		t.Fatal("active workload was retained after shutdown")
	}
	if err := svc.rejectIfDraining(); err == nil {
		t.Fatal("draining worker accepted activation")
	}
}

func TestGracefulShutdownCancelsInFlightActivation(t *testing.T) {
	svc := newService(&trackingRuntime{}, &fakeActorNetwork{path: "/run/netns/test"})
	svc.mu.Lock()
	rpcCtx, cancelRPC := context.WithCancel(context.Background())
	svc.setActiveRPC(rpcRunWorkload, cancelRPC)
	released := make(chan struct{})
	go func() {
		<-rpcCtx.Done()
		svc.clearActiveRPC()
		svc.mu.Unlock()
		close(released)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := svc.gracefulShutdown(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case <-released:
	case <-time.After(time.Second):
		t.Fatal("graceful shutdown did not cancel the active RunWorkload RPC")
	}
}

func TestGracefulShutdownRespectsLockDeadline(t *testing.T) {
	svc := newService(&trackingRuntime{}, &fakeActorNetwork{path: "/run/netns/test"})
	svc.mu.Lock()
	defer svc.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := svc.gracefulShutdown(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("gracefulShutdown error = %v, want context deadline exceeded", err)
	}
}

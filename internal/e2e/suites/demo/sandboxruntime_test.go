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

package demo

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"go.ate.dev/substrate/internal/e2e"
	"go.ate.dev/substrate/internal/hardware"
	"go.ate.dev/substrate/internal/resources"
	atev1alpha1 "go.ate.dev/substrate/pkg/api/v1alpha1"
	"go.ate.dev/substrate/pkg/proto/ateapipb"
	"google.golang.org/protobuf/testing/protocmp"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// gvisorSecondReleaseAssets is a second gVisor release (nightly 2026-10-20)
// that includes the multi-bundle filesystem checkpoint and no-root-container
// support from 2026-10-17+ while producing a distinct sandbox_bundle_hash.
var gvisorSecondReleaseAssets = map[string]map[string]atev1alpha1.AssetFile{
	"amd64": {
		"gvisor": {
			URL:    "gs://gvisor/releases/nightly/2026-10-20/x86_64/gvisor.tar.zstd",
			SHA256: "d4f85f39e33cb51e62962af6e950339867be2601439e86a68a7cce1f55847df4",
		},
	},
	"arm64": {
		"gvisor": {
			URL:    "gs://gvisor/releases/nightly/2026-10-20/aarch64/gvisor.tar.zstd",
			SHA256: "5bc2917a9dc0d6235d86a7d90621d9f09d9771d84233a9c0be56464d2181fec5",
		},
	},
}

// TestSandboxRuntimeUpgrade covers keeping a suspended actor's MEMORY snapshot
// restorable across a SandboxConfig defaultVersion upgrade:
//  1. An actor boots on v1, increments its memory, rootfs, and volume counters,
//     and suspends; its snapshot is stamped with v1's runtime.
//  2. The pool's SandboxConfig adds v2 as defaultVersion while keeping v1
//     Enabled; the worker re-registers with v2 as default_runtime and v1 in
//     restorable_runtimes.
//  3. Resuming the actor matches v1 via restorable_runtimes, preserves the
//     in-memory counter, records v1 on WorkerAssignment.sandbox_runtime, and
//     re-stamps v1 on the next suspend.
func TestSandboxRuntimeUpgrade(t *testing.T) {
	t.Parallel()
	f := setupSuspendedV1Actor(t, "runtime-upgrade")

	// 2. Add v2 (Enabled) and flip defaultVersion from v1 to v2 while v1 stays Enabled.
	updateSandboxConfigVersions(t, f.ctx, f.clients, f.configName, f.v2.Name, []atev1alpha1.SandboxVersionConfig{f.v1, f.v2})
	waitForWorkerRestorableUpgrade(t, f.ctx, f.clients, f.workerRef, f.v1Runtime)

	// 3. Resume the actor: it matches v1 via restorable_runtimes and restores MEMORY.
	resumed2, err := e2e.ResumeActorAwaitCapacity(t, f.ctx, f.clients, &ateapipb.ResumeActorRequest{Actor: f.actorRef})
	if err != nil {
		t.Fatalf("failed to resume Actor via restorable runtime: %v", err)
	}
	waitForActorState(f.ctx, t, f.clients, f.actorName, ateapipb.ActorState_ACTOR_STATE_RUNNING)

	assignment2 := resumed2.GetActor().GetStatus().GetWorkerAssignment()
	if diff := cmp.Diff(f.v1Runtime, assignment2.GetSandboxRuntime(), protocmp.Transform()); diff != "" {
		t.Fatalf("workerAssignment.sandbox_runtime after restorable resume (-want v1 +got):\n%s", diff)
	}

	resp, err := callActor(t, resources.ActorRef{Atespace: demoAtespace, Name: f.actorName})
	if err != nil {
		t.Fatalf("failed to call actor after restorable resume: %v", err)
	}
	validateCounterResponse(t, resp, "after restorable resume", 3, 3)
	validateRootfsCounter(t, resp, "after restorable resume", 3)

	// 4. Suspend again and verify the new snapshot is still stamped with v1.
	suspended2, err := f.clients.SubstrateAPI.SuspendActor(f.ctx, &ateapipb.SuspendActorRequest{
		Actor:   f.actorRef,
		Options: &ateapipb.SuspendOptions{Mode: ateapipb.DurabilityMode_DURABILITY_MODE_SYNCHRONOUS},
	})
	if err != nil {
		t.Fatalf("failed to suspend Actor a second time: %v", err)
	}
	waitForActorState(f.ctx, t, f.clients, f.actorName, ateapipb.ActorState_ACTOR_STATE_SUSPENDED)

	snap2 := durableSnapshot(suspended2.GetActor().GetStatus())
	if snap2.GetGeneration() <= f.snap1.GetGeneration() {
		t.Fatalf("second snapshot generation = %d, want > %d", snap2.GetGeneration(), f.snap1.GetGeneration())
	}
	if diff := cmp.Diff(f.v1Runtime, snap2.GetSandboxRuntime(), protocmp.Transform()); diff != "" {
		t.Fatalf("second snapshot sandbox_runtime mismatch (-want v1 +got):\n%s", diff)
	}
}

// TestSandboxRuntimeDisableDropsToRootfs covers disabling a SandboxConfig
// version after an actor has suspended on it:
//  1. An actor boots on v1, increments its memory, rootfs, and volume counters
//     twice, and suspends; its snapshot is stamped with v1's runtime.
//  2. The pool's SandboxConfig switches defaultVersion to v2 and marks v1
//     Disabled; the worker re-registers with v2 as default_runtime and drops v1
//     from restorable_runtimes.
//  3. Resuming the actor drops from MEMORY to ROOTFS on v2: rootfs and volume
//     counters continue while the memory counter resets, and
//     WorkerAssignment.sandbox_runtime records v2.
//  4. Suspending the actor re-stamps the new MEMORY snapshot with v2, and the
//     next resume restores MEMORY on v2.
func TestSandboxRuntimeDisableDropsToRootfs(t *testing.T) {
	t.Parallel()
	f := setupSuspendedV1Actor(t, "runtime-disable")

	// 2. Switch defaultVersion to v2 and mark v1 Disabled.
	disabled := atev1alpha1.SandboxVersionStateDisabled
	v1Disabled := f.v1
	v1Disabled.State = &disabled
	updateSandboxConfigVersions(t, f.ctx, f.clients, f.configName, f.v2.Name, []atev1alpha1.SandboxVersionConfig{v1Disabled, f.v2})
	v2Runtime := waitForWorkerRuntimeDisabled(t, f.ctx, f.clients, f.workerRef, f.v1Runtime)

	// 3. Resume the actor: no worker can restore v1, so it drops to ROOTFS on v2.
	resumed2, err := e2e.ResumeActorAwaitCapacity(t, f.ctx, f.clients, &ateapipb.ResumeActorRequest{Actor: f.actorRef})
	if err != nil {
		t.Fatalf("failed to resume Actor after v1 disabled: %v", err)
	}
	waitForActorState(f.ctx, t, f.clients, f.actorName, ateapipb.ActorState_ACTOR_STATE_RUNNING)

	assignment2 := resumed2.GetActor().GetStatus().GetWorkerAssignment()
	if diff := cmp.Diff(v2Runtime, assignment2.GetSandboxRuntime(), protocmp.Transform()); diff != "" {
		t.Fatalf("workerAssignment.sandbox_runtime after ROOTFS fallback (-want v2 +got):\n%s", diff)
	}

	resp, err := callActor(t, resources.ActorRef{Atespace: demoAtespace, Name: f.actorName})
	if err != nil {
		t.Fatalf("failed to call actor after ROOTFS fallback: %v", err)
	}
	validateCounterResponse(t, resp, "after ROOTFS fallback", 1, 3)
	validateRootfsCounter(t, resp, "after ROOTFS fallback", 3)

	// 4. Suspend and verify the new MEMORY snapshot is stamped with v2, then
	// resume again to confirm a warm MEMORY restore on v2.
	suspended2, err := f.clients.SubstrateAPI.SuspendActor(f.ctx, &ateapipb.SuspendActorRequest{
		Actor:   f.actorRef,
		Options: &ateapipb.SuspendOptions{Mode: ateapipb.DurabilityMode_DURABILITY_MODE_SYNCHRONOUS},
	})
	if err != nil {
		t.Fatalf("failed to suspend Actor on v2: %v", err)
	}
	waitForActorState(f.ctx, t, f.clients, f.actorName, ateapipb.ActorState_ACTOR_STATE_SUSPENDED)

	snap2 := durableSnapshot(suspended2.GetActor().GetStatus())
	if snap2.GetGeneration() <= f.snap1.GetGeneration() {
		t.Fatalf("second snapshot generation = %d, want > %d", snap2.GetGeneration(), f.snap1.GetGeneration())
	}
	if got := snap2.GetFidelity(); got != ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY {
		t.Fatalf("snapshot fidelity after v2 suspend = %v, want MEMORY", got)
	}
	if diff := cmp.Diff(v2Runtime, snap2.GetSandboxRuntime(), protocmp.Transform()); diff != "" {
		t.Fatalf("second snapshot sandbox_runtime mismatch (-want v2 +got):\n%s", diff)
	}

	resumed3, err := e2e.ResumeActorAwaitCapacity(t, f.ctx, f.clients, &ateapipb.ResumeActorRequest{Actor: f.actorRef})
	if err != nil {
		t.Fatalf("failed to resume Actor on v2: %v", err)
	}
	waitForActorState(f.ctx, t, f.clients, f.actorName, ateapipb.ActorState_ACTOR_STATE_RUNNING)
	if diff := cmp.Diff(v2Runtime, resumed3.GetActor().GetStatus().GetWorkerAssignment().GetSandboxRuntime(), protocmp.Transform()); diff != "" {
		t.Fatalf("workerAssignment.sandbox_runtime on v2 MEMORY resume (-want v2 +got):\n%s", diff)
	}

	resp, err = callActor(t, resources.ActorRef{Atespace: demoAtespace, Name: f.actorName})
	if err != nil {
		t.Fatalf("failed to call actor after v2 MEMORY resume: %v", err)
	}
	validateCounterResponse(t, resp, "after v2 MEMORY resume", 2, 4)
	validateRootfsCounter(t, resp, "after v2 MEMORY resume", 4)
}

// TestWorkerPoolSwitchSandboxConfig covers switching a WorkerPool's configRef
// to another SandboxConfig of the same class:
//  1. A pool starts on configA (v1), builds a golden ActorTemplate stamped with
//     v1, and resumes actor1 on v1 (memory=1, file=1, rootfs=1).
//  2. While actor1 is RUNNING on v1, the pool's configRef switches to configB
//     (v2); the worker re-registers with v2 as default_runtime while actor1
//     continues running on v1 (memory=2, file=2, rootfs=2).
//  3. Suspending actor1 stamps the snapshot with v1 (from
//     WorkerAssignment.sandbox_runtime, not the worker's new default_runtime),
//     and resuming actor1 on configB drops to ROOTFS on v2 (memory=1, file=3,
//     rootfs=3).
//  4. Creating and resuming a fresh actor2 from the v1-stamped golden template
//     while the pool is on configB also drops the borrowed golden snapshot to
//     ROOTFS on v2 (memory=1, file=1, rootfs=1).
func TestWorkerPoolSwitchSandboxConfig(t *testing.T) {
	t.Parallel()
	nsObj := e2e.CreateNamespace(t)
	ctx := context.Background()
	clients := e2e.GetClients()

	env, err := e2e.CheckEnv("BUCKET_NAME")
	if err != nil {
		t.Fatalf("CheckEnv failed: %v", err)
	}

	class := atev1alpha1.SandboxClassGvisor
	if e2e.IsMicroVM() {
		class = atev1alpha1.SandboxClassMicroVM
	}
	baseConfig := newestClassDefault(t, ctx, clients, class)
	v1 := defaultVersionFromConfig(t, baseConfig, "v1")
	v2 := secondSandboxVersion(v1, "v2", class)

	configAName := "e2e-switch-a-" + nsObj.Name
	configBName := "e2e-switch-b-" + nsObj.Name
	poolName := "runtime-switch"
	for _, cfg := range []struct {
		name string
		ver  atev1alpha1.SandboxVersionConfig
	}{
		{name: configAName, ver: v1},
		{name: configBName, ver: v2},
	} {
		if _, err := clients.SubstrateK8s.ApiV1alpha1().SandboxConfigs().Create(ctx, &atev1alpha1.SandboxConfig{
			ObjectMeta: metav1.ObjectMeta{Name: cfg.name},
			Spec: atev1alpha1.SandboxConfigSpec{
				SandboxClass:   class,
				DefaultVersion: cfg.ver.Name,
				Versions:       []atev1alpha1.SandboxVersionConfig{cfg.ver},
			},
		}, metav1.CreateOptions{}); err != nil {
			t.Fatalf("failed to create SandboxConfig %s: %v", cfg.name, err)
		}
	}
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		_ = clients.SubstrateK8s.ApiV1alpha1().WorkerPools(nsObj.Name).Delete(cleanupCtx, poolName, metav1.DeleteOptions{})
		for _, name := range []string{configAName, configBName} {
			if err := clients.SubstrateK8s.ApiV1alpha1().SandboxConfigs().Delete(cleanupCtx, name, metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
				t.Errorf("failed to delete SandboxConfig %s: %v", name, err)
			}
		}
	})

	tmplName := poolName + "-" + nsObj.Name
	tmpl := e2e.CreateSubstrateCounterTemplate(ctx, t, clients, nsObj.Name, e2e.SubstrateTemplateOptions{
		Atespace:          demoAtespace,
		Name:              tmplName,
		PoolName:          poolName,
		PoolReplicas:      1,
		SandboxConfigName: configAName,
		Labels:            map[string]string{"demo": nsObj.Name},
		SnapshotConfig: &ateapipb.SnapshotConfig{
			StorageLocation:   "gs://" + env["BUCKET_NAME"] + "/ate-demo-" + tmplName,
			PreferredFidelity: ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY,
		},
	})

	actor1Name := "switch-actor1-" + nsObj.Name
	actor2Name := "switch-actor2-" + nsObj.Name
	actor1Ref := &ateapipb.ObjectRef{Atespace: demoAtespace, Name: actor1Name}
	actor2Ref := &ateapipb.ObjectRef{Atespace: demoAtespace, Name: actor2Name}
	for _, ref := range []*ateapipb.ObjectRef{actor1Ref, actor2Ref} {
		t.Cleanup(func() {
			cleanupCtx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer cancel()
			_, _ = clients.SubstrateAPI.SuspendActor(cleanupCtx, &ateapipb.SuspendActorRequest{Actor: ref})
			_, _ = clients.SubstrateAPI.DeleteActor(cleanupCtx, &ateapipb.DeleteActorRequest{Actor: ref})
		})
	}

	// 1. Create and resume actor1 on configA (v1), and call it once.
	if _, err := clients.SubstrateAPI.CreateActor(ctx, &ateapipb.CreateActorRequest{Actor: &ateapipb.Actor{
		Metadata:      &ateapipb.ResourceMetadata{Atespace: demoAtespace, Name: actor1Name},
		ActorTemplate: e2e.TemplateRef(tmpl),
	}}); err != nil {
		t.Fatalf("failed to create actor1: %v", err)
	}
	waitForActorState(ctx, t, clients, actor1Name, ateapipb.ActorState_ACTOR_STATE_SUSPENDED)

	resumed1, err := e2e.ResumeActorAwaitCapacity(t, ctx, clients, &ateapipb.ResumeActorRequest{Actor: actor1Ref})
	if err != nil {
		t.Fatalf("failed to resume actor1 on configA: %v", err)
	}
	waitForActorState(ctx, t, clients, actor1Name, ateapipb.ActorState_ACTOR_STATE_RUNNING)

	assignment1 := resumed1.GetActor().GetStatus().GetWorkerAssignment()
	v1Runtime := assignment1.GetSandboxRuntime()
	workerRef := assignment1.GetWorker()

	resp, err := callActor(t, resources.ActorRef{Atespace: demoAtespace, Name: actor1Name})
	if err != nil {
		t.Fatalf("failed to call actor1 on configA: %v", err)
	}
	validateCounterResponse(t, resp, "on configA before switch", 1, 1)
	validateRootfsCounter(t, resp, "on configA before switch", 1)

	// 2. Switch the pool's configRef to configB while actor1 is still RUNNING.
	updateWorkerPoolSandboxConfig(t, ctx, clients, nsObj.Name, poolName, configBName)
	v2Runtime := waitForWorkerRuntimeDisabled(t, ctx, clients, workerRef, v1Runtime)

	// Verify actor1 is untouched while running and stamps v1 when suspended.
	resp, err = callActor(t, resources.ActorRef{Atespace: demoAtespace, Name: actor1Name})
	if err != nil {
		t.Fatalf("failed to call running actor1 after pool config switch: %v", err)
	}
	validateCounterResponse(t, resp, "running actor1 after pool switch", 2, 2)
	validateRootfsCounter(t, resp, "running actor1 after pool switch", 2)

	// 3. Suspend actor1: its snapshot must carry v1 (its assignment's runtime), not v2.
	suspended1, err := clients.SubstrateAPI.SuspendActor(ctx, &ateapipb.SuspendActorRequest{
		Actor:   actor1Ref,
		Options: &ateapipb.SuspendOptions{Mode: ateapipb.DurabilityMode_DURABILITY_MODE_SYNCHRONOUS},
	})
	if err != nil {
		t.Fatalf("failed to suspend actor1: %v", err)
	}
	waitForActorState(ctx, t, clients, actor1Name, ateapipb.ActorState_ACTOR_STATE_SUSPENDED)
	if diff := cmp.Diff(v1Runtime, durableSnapshot(suspended1.GetActor().GetStatus()).GetSandboxRuntime(), protocmp.Transform()); diff != "" {
		t.Fatalf("actor1 snapshot stamp after pool switch (-want v1 +got):\n%s", diff)
	}

	// Resume actor1 on configB: it drops from MEMORY to ROOTFS on v2.
	resumed1OnB, err := e2e.ResumeActorAwaitCapacity(t, ctx, clients, &ateapipb.ResumeActorRequest{Actor: actor1Ref})
	if err != nil {
		t.Fatalf("failed to resume actor1 on configB: %v", err)
	}
	waitForActorState(ctx, t, clients, actor1Name, ateapipb.ActorState_ACTOR_STATE_RUNNING)
	if diff := cmp.Diff(v2Runtime, resumed1OnB.GetActor().GetStatus().GetWorkerAssignment().GetSandboxRuntime(), protocmp.Transform()); diff != "" {
		t.Fatalf("actor1 assignment runtime on configB (-want v2 +got):\n%s", diff)
	}

	resp, err = callActor(t, resources.ActorRef{Atespace: demoAtespace, Name: actor1Name})
	if err != nil {
		t.Fatalf("failed to call actor1 after ROOTFS fallback on configB: %v", err)
	}
	validateCounterResponse(t, resp, "actor1 on configB", 1, 3)
	validateRootfsCounter(t, resp, "actor1 on configB", 3)

	if _, err := clients.SubstrateAPI.SuspendActor(ctx, &ateapipb.SuspendActorRequest{
		Actor:   actor1Ref,
		Options: &ateapipb.SuspendOptions{Mode: ateapipb.DurabilityMode_DURABILITY_MODE_SYNCHRONOUS},
	}); err != nil {
		t.Fatalf("failed to suspend actor1 on configB: %v", err)
	}
	waitForActorState(ctx, t, clients, actor1Name, ateapipb.ActorState_ACTOR_STATE_SUSPENDED)

	// 4. Create actor2 from the v1-stamped golden template and resume it on configB (C4).
	created2, err := clients.SubstrateAPI.CreateActor(ctx, &ateapipb.CreateActorRequest{Actor: &ateapipb.Actor{
		Metadata:      &ateapipb.ResourceMetadata{Atespace: demoAtespace, Name: actor2Name},
		ActorTemplate: e2e.TemplateRef(tmpl),
	}})
	if err != nil {
		t.Fatalf("failed to create actor2 from golden template: %v", err)
	}
	if diff := cmp.Diff(v1Runtime, durableSnapshot(created2.GetStatus()).GetSandboxRuntime(), protocmp.Transform()); diff != "" {
		t.Fatalf("actor2 borrowed golden snapshot stamp (-want v1 +got):\n%s", diff)
	}

	resumed2, err := e2e.ResumeActorAwaitCapacity(t, ctx, clients, &ateapipb.ResumeActorRequest{Actor: actor2Ref})
	if err != nil {
		t.Fatalf("failed to resume actor2 from v1 golden snapshot on configB: %v", err)
	}
	waitForActorState(ctx, t, clients, actor2Name, ateapipb.ActorState_ACTOR_STATE_RUNNING)
	if diff := cmp.Diff(v2Runtime, resumed2.GetActor().GetStatus().GetWorkerAssignment().GetSandboxRuntime(), protocmp.Transform()); diff != "" {
		t.Fatalf("actor2 assignment runtime after golden ROOTFS fallback (-want v2 +got):\n%s", diff)
	}

	resp, err = callActor(t, resources.ActorRef{Atespace: demoAtespace, Name: actor2Name})
	if err != nil {
		t.Fatalf("failed to call actor2 after golden ROOTFS fallback: %v", err)
	}
	validateCounterResponse(t, resp, "actor2 after golden ROOTFS fallback", 1, 1)
	validateRootfsCounter(t, resp, "actor2 after golden ROOTFS fallback", 1)
}

type sandboxRuntimeFixture struct {
	ctx        context.Context
	clients    *e2e.Clients
	configName string
	v1, v2     atev1alpha1.SandboxVersionConfig
	actorName  string
	actorRef   *ateapipb.ObjectRef
	workerRef  *ateapipb.ObjectRef
	v1Runtime  *ateapipb.ActorSandboxRuntime
	snap1      *ateapipb.Snapshot
}

// setupSuspendedV1Actor creates a per-test SandboxConfig (at v1), WorkerPool,
// ActorTemplate, and Actor, runs the actor on v1 for two calls (memory=2,
// file=2, rootfs=2), suspends it synchronously, and verifies its MEMORY
// snapshot is stamped with v1.
func setupSuspendedV1Actor(t *testing.T, prefix string) *sandboxRuntimeFixture {
	t.Helper()
	nsObj := e2e.CreateNamespace(t)
	ctx := context.Background()
	clients := e2e.GetClients()

	env, err := e2e.CheckEnv("BUCKET_NAME")
	if err != nil {
		t.Fatalf("CheckEnv failed: %v", err)
	}

	class := atev1alpha1.SandboxClassGvisor
	if e2e.IsMicroVM() {
		class = atev1alpha1.SandboxClassMicroVM
	}
	baseConfig := newestClassDefault(t, ctx, clients, class)
	v1 := defaultVersionFromConfig(t, baseConfig, "v1")
	v2 := secondSandboxVersion(v1, "v2", class)

	configName := "e2e-" + prefix + "-" + nsObj.Name
	if _, err := clients.SubstrateK8s.ApiV1alpha1().SandboxConfigs().Create(ctx, &atev1alpha1.SandboxConfig{
		ObjectMeta: metav1.ObjectMeta{Name: configName},
		Spec: atev1alpha1.SandboxConfigSpec{
			SandboxClass:   class,
			DefaultVersion: v1.Name,
			Versions:       []atev1alpha1.SandboxVersionConfig{v1},
		},
	}, metav1.CreateOptions{}); err != nil {
		t.Fatalf("failed to create SandboxConfig %s: %v", configName, err)
	}
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		_ = clients.SubstrateK8s.ApiV1alpha1().WorkerPools(nsObj.Name).Delete(cleanupCtx, prefix, metav1.DeleteOptions{})
		if err := clients.SubstrateK8s.ApiV1alpha1().SandboxConfigs().Delete(cleanupCtx, configName, metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
			t.Errorf("failed to delete SandboxConfig %s: %v", configName, err)
		}
	})

	tmplName := prefix + "-" + nsObj.Name
	tmpl := e2e.CreateSubstrateCounterTemplate(ctx, t, clients, nsObj.Name, e2e.SubstrateTemplateOptions{
		Atespace: demoAtespace,
		Name:     tmplName,
		PoolName: prefix,
		// Single worker so GetWorker on workerRef observes the only candidate
		// before the second resume.
		PoolReplicas:      1,
		SandboxConfigName: configName,
		Labels:            map[string]string{"demo": nsObj.Name},
		SnapshotConfig: &ateapipb.SnapshotConfig{
			StorageLocation:   "gs://" + env["BUCKET_NAME"] + "/ate-demo-" + tmplName,
			PreferredFidelity: ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY,
		},
	})

	actorName := prefix + "-" + nsObj.Name
	actorRef := &ateapipb.ObjectRef{Atespace: demoAtespace, Name: actorName}
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		_, _ = clients.SubstrateAPI.SuspendActor(cleanupCtx, &ateapipb.SuspendActorRequest{Actor: actorRef})
		_, _ = clients.SubstrateAPI.DeleteActor(cleanupCtx, &ateapipb.DeleteActorRequest{Actor: actorRef})
	})

	if _, err := clients.SubstrateAPI.CreateActor(ctx, &ateapipb.CreateActorRequest{Actor: &ateapipb.Actor{
		Metadata:      &ateapipb.ResourceMetadata{Atespace: demoAtespace, Name: actorName},
		ActorTemplate: e2e.TemplateRef(tmpl),
	}}); err != nil {
		t.Fatalf("failed to create Actor: %v", err)
	}
	waitForActorState(ctx, t, clients, actorName, ateapipb.ActorState_ACTOR_STATE_SUSPENDED)

	resumed1, err := e2e.ResumeActorAwaitCapacity(t, ctx, clients, &ateapipb.ResumeActorRequest{Actor: actorRef})
	if err != nil {
		t.Fatalf("failed to resume Actor on v1: %v", err)
	}
	waitForActorState(ctx, t, clients, actorName, ateapipb.ActorState_ACTOR_STATE_RUNNING)

	assignment1 := resumed1.GetActor().GetStatus().GetWorkerAssignment()
	v1Runtime := assignment1.GetSandboxRuntime()
	if runtimeAttribute(v1Runtime, hardware.AttrSandboxBundleHash) == "" {
		t.Fatalf("initial assignment runtime %v has no %q attribute", v1Runtime, hardware.AttrSandboxBundleHash)
	}
	workerRef := assignment1.GetWorker()
	if workerRef.GetName() == "" {
		t.Fatalf("initial assignment %v has no worker reference", assignment1)
	}

	for i := 1; i <= 2; i++ {
		resp, err := callActor(t, resources.ActorRef{Atespace: demoAtespace, Name: actorName})
		if err != nil {
			t.Fatalf("failed to call actor on v1 (call %d): %v", i, err)
		}
		validateCounterResponse(t, resp, "on v1", i, i)
		validateRootfsCounter(t, resp, "on v1", i)
	}

	suspended1, err := clients.SubstrateAPI.SuspendActor(ctx, &ateapipb.SuspendActorRequest{
		Actor:   actorRef,
		Options: &ateapipb.SuspendOptions{Mode: ateapipb.DurabilityMode_DURABILITY_MODE_SYNCHRONOUS},
	})
	if err != nil {
		t.Fatalf("failed to suspend Actor on v1: %v", err)
	}
	waitForActorState(ctx, t, clients, actorName, ateapipb.ActorState_ACTOR_STATE_SUSPENDED)

	snap1 := durableSnapshot(suspended1.GetActor().GetStatus())
	if got := snap1.GetFidelity(); got != ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY {
		t.Fatalf("snapshot fidelity after v1 suspend = %v, want MEMORY", got)
	}
	if diff := cmp.Diff(v1Runtime, snap1.GetSandboxRuntime(), protocmp.Transform()); diff != "" {
		t.Fatalf("snapshot sandbox_runtime mismatch after v1 suspend (-want +got):\n%s", diff)
	}

	return &sandboxRuntimeFixture{
		ctx:        ctx,
		clients:    clients,
		configName: configName,
		v1:         v1,
		v2:         v2,
		actorName:  actorName,
		actorRef:   actorRef,
		workerRef:  workerRef,
		v1Runtime:  v1Runtime,
		snap1:      snap1,
	}
}

// newestClassDefault returns the cluster's installed class-default
// SandboxConfig for class.
func newestClassDefault(t *testing.T, ctx context.Context, clients *e2e.Clients, class atev1alpha1.SandboxClass) *atev1alpha1.SandboxConfig {
	t.Helper()
	list, err := clients.SubstrateK8s.ApiV1alpha1().SandboxConfigs().List(ctx, metav1.ListOptions{})
	if err != nil {
		t.Fatalf("failed to list SandboxConfigs: %v", err)
	}
	var best *atev1alpha1.SandboxConfig
	for i := range list.Items {
		sc := &list.Items[i]
		if sc.Spec.SandboxClass != class || sc.Annotations[atev1alpha1.SandboxConfigClassDefaultAnnotation] != "true" || !sc.DeletionTimestamp.IsZero() {
			continue
		}
		if best == nil || best.CreationTimestamp.Before(&sc.CreationTimestamp) ||
			(best.CreationTimestamp.Equal(&sc.CreationTimestamp) && sc.Name < best.Name) {
			best = sc
		}
	}
	if best == nil {
		t.Fatalf("no SandboxConfig of class %q is annotated %s=true; the install ships one, see manifests/ate-install", class, atev1alpha1.SandboxConfigClassDefaultAnnotation)
	}
	return best
}

// defaultVersionFromConfig extracts sc's default version and returns a copy
// renamed to name with state Enabled.
func defaultVersionFromConfig(t *testing.T, sc *atev1alpha1.SandboxConfig, name string) atev1alpha1.SandboxVersionConfig {
	t.Helper()
	for i := range sc.Spec.Versions {
		if sc.Spec.Versions[i].Name == sc.Spec.DefaultVersion {
			v := *sc.Spec.Versions[i].DeepCopy()
			v.Name = name
			enabled := atev1alpha1.SandboxVersionStateEnabled
			v.State = &enabled
			return v
		}
	}
	t.Fatalf("SandboxConfig %s has no version matching defaultVersion %q", sc.Name, sc.Spec.DefaultVersion)
	return atev1alpha1.SandboxVersionConfig{}
}

// secondSandboxVersion returns a second Enabled version for class whose assets
// produce a different sandbox_bundle_hash than v1. On gVisor it uses a second
// gVisor release tarball; on microVM it reuses v1's cached assets plus an
// additional entry so no second kata asset set is needed in the bucket.
func secondSandboxVersion(v1 atev1alpha1.SandboxVersionConfig, name string, class atev1alpha1.SandboxClass) atev1alpha1.SandboxVersionConfig {
	v2 := *v1.DeepCopy()
	v2.Name = name
	enabled := atev1alpha1.SandboxVersionStateEnabled
	v2.State = &enabled
	if class == atev1alpha1.SandboxClassGvisor {
		v2.Assets = gvisorSecondReleaseAssets
		return v2
	}
	for arch, files := range v2.Assets {
		files["extra"] = files["virtiofsd"]
		v2.Assets[arch] = files
	}
	return v2
}

// updateSandboxConfigVersions updates configName's defaultVersion and versions list.
func updateSandboxConfigVersions(t *testing.T, ctx context.Context, clients *e2e.Clients, configName, defaultVersion string, versions []atev1alpha1.SandboxVersionConfig) {
	t.Helper()
	for {
		sc, err := clients.SubstrateK8s.ApiV1alpha1().SandboxConfigs().Get(ctx, configName, metav1.GetOptions{})
		if err != nil {
			t.Fatalf("failed to get SandboxConfig %s: %v", configName, err)
		}
		sc.Spec.DefaultVersion = defaultVersion
		sc.Spec.Versions = versions
		_, err = clients.SubstrateK8s.ApiV1alpha1().SandboxConfigs().Update(ctx, sc, metav1.UpdateOptions{})
		if apierrors.IsConflict(err) {
			continue
		}
		if err != nil {
			t.Fatalf("failed to update SandboxConfig %s: %v", configName, err)
		}
		return
	}
}

// updateWorkerPoolSandboxConfig updates poolName's sandboxClasses[].configRef
// to point at configName.
func updateWorkerPoolSandboxConfig(t *testing.T, ctx context.Context, clients *e2e.Clients, namespace, poolName, configName string) {
	t.Helper()
	for {
		wp, err := clients.SubstrateK8s.ApiV1alpha1().WorkerPools(namespace).Get(ctx, poolName, metav1.GetOptions{})
		if err != nil {
			t.Fatalf("failed to get WorkerPool %s/%s: %v", namespace, poolName, err)
		}
		for i := range wp.Spec.SandboxClasses {
			wp.Spec.SandboxClasses[i].ConfigRef = &atev1alpha1.SandboxConfigReference{Name: configName}
		}
		_, err = clients.SubstrateK8s.ApiV1alpha1().WorkerPools(namespace).Update(ctx, wp, metav1.UpdateOptions{})
		if apierrors.IsConflict(err) {
			continue
		}
		if err != nil {
			t.Fatalf("failed to update WorkerPool %s/%s: %v", namespace, poolName, err)
		}
		return
	}
}

// waitForWorkerRestorableUpgrade polls GetWorker until workerRef reports a
// default_runtime with a different bundle hash than v1Runtime while still
// matching v1Runtime via restorable_runtimes.
func waitForWorkerRestorableUpgrade(t *testing.T, ctx context.Context, clients *e2e.Clients, workerRef *ateapipb.ObjectRef, v1Runtime *ateapipb.ActorSandboxRuntime) {
	t.Helper()
	const timeout = 2 * time.Minute
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	v1Hash := runtimeAttribute(v1Runtime, hardware.AttrSandboxBundleHash)
	var lastClasses []*ateapipb.WorkerSandboxClass
	for {
		w, err := clients.SubstrateAPI.GetWorker(ctx, &ateapipb.GetWorkerRequest{Worker: workerRef})
		if err == nil {
			status := w.GetStatus()
			lastClasses = status.GetSandboxClasses()
			defHash := runtimeAttribute(hardware.ColdBootRuntime(status), hardware.AttrSandboxBundleHash)
			if defHash != "" && defHash != v1Hash && hardware.WorkerMatches(status, v1Runtime) {
				t.Logf("Worker %s re-registered: default=%s, restorable=[%s]", workerRef.GetName(), defHash, v1Hash)
				return
			}
		}
		select {
		case <-ctx.Done():
			raw, _ := json.Marshal(lastClasses)
			t.Fatalf("timed out after %v waiting for Worker %s to report a new default_runtime and v1 (%s) in restorable_runtimes (last sandbox_classes=%s, err=%v)",
				timeout, workerRef.GetName(), v1Hash, raw, err)
		case <-time.After(500 * time.Millisecond):
		}
	}
}

// waitForWorkerRuntimeDisabled polls GetWorker until workerRef reports a
// default_runtime with a different bundle hash than v1Runtime and no longer
// matches v1Runtime in default_runtime or restorable_runtimes.
func waitForWorkerRuntimeDisabled(t *testing.T, ctx context.Context, clients *e2e.Clients, workerRef *ateapipb.ObjectRef, v1Runtime *ateapipb.ActorSandboxRuntime) *ateapipb.ActorSandboxRuntime {
	t.Helper()
	const timeout = 2 * time.Minute
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	v1Hash := runtimeAttribute(v1Runtime, hardware.AttrSandboxBundleHash)
	var lastClasses []*ateapipb.WorkerSandboxClass
	for {
		w, err := clients.SubstrateAPI.GetWorker(ctx, &ateapipb.GetWorkerRequest{Worker: workerRef})
		if err == nil {
			status := w.GetStatus()
			lastClasses = status.GetSandboxClasses()
			defRuntime := hardware.ColdBootRuntime(status)
			defHash := runtimeAttribute(defRuntime, hardware.AttrSandboxBundleHash)
			if defHash != "" && defHash != v1Hash && !hardware.WorkerMatches(status, v1Runtime) {
				t.Logf("Worker %s re-registered with v1 disabled: default=%s", workerRef.GetName(), defHash)
				return defRuntime
			}
		}
		select {
		case <-ctx.Done():
			raw, _ := json.Marshal(lastClasses)
			t.Fatalf("timed out after %v waiting for Worker %s to report a new default_runtime and drop v1 (%s) (last sandbox_classes=%s, err=%v)",
				timeout, workerRef.GetName(), v1Hash, raw, err)
		case <-time.After(500 * time.Millisecond):
		}
	}
}

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
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"go.ate.dev/substrate/api/ateapi/v1"
	atev1 "go.ate.dev/substrate/api/infra/k8s/ate/v1"
	"go.ate.dev/substrate/internal/e2e"
	"go.ate.dev/substrate/internal/hardware"
	"go.ate.dev/substrate/internal/resources"
	"google.golang.org/protobuf/testing/protocmp"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// gvisorSecondReleaseAssets is a second gVisor release (nightly 2026-10-20)
// that includes the multi-bundle filesystem checkpoint and no-root-container
// support from 2026-10-17+ while producing a distinct sandbox_bundle_hash.
var gvisorSecondReleaseAssets = map[string]map[string]atev1.AssetFile{
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
	f := setupSuspendedV1Actor(t, "runtime-upgrade", ateapi.DurabilityMode_DURABILITY_MODE_SYNCHRONOUS)

	// 2. Add v2 (Enabled) and flip defaultVersion from v1 to v2 while v1 stays Enabled.
	updateSandboxConfigVersions(t, f.ctx, f.clients, f.configName, f.v2.Name, []atev1.SandboxVersionConfig{f.v1, f.v2})
	waitForWorkerRestorableUpgrade(t, f.ctx, f.clients, f.workerRef, f.v1Runtime)

	// 3. Resume the actor: it matches v1 via restorable_runtimes and restores MEMORY.
	resumed2, err := e2e.ResumeActorAwaitCapacity(t, f.ctx, f.clients, &ateapi.ResumeActorRequest{Actor: f.actorRef})
	if err != nil {
		t.Fatalf("failed to resume Actor via restorable runtime: %v", err)
	}
	waitForActorState(f.ctx, t, f.clients, f.actorName, ateapi.ActorState_ACTOR_STATE_RUNNING)

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
	suspended2, err := f.clients.SubstrateAPI.SuspendActor(f.ctx, &ateapi.SuspendActorRequest{
		Actor:   f.actorRef,
		Options: &ateapi.SuspendOptions{Mode: ateapi.DurabilityMode_DURABILITY_MODE_SYNCHRONOUS},
	})
	if err != nil {
		t.Fatalf("failed to suspend Actor a second time: %v", err)
	}
	waitForActorState(f.ctx, t, f.clients, f.actorName, ateapi.ActorState_ACTOR_STATE_SUSPENDED)

	snap2 := durableSnapshot(suspended2.GetActor().GetStatus())
	if snap2.GetGeneration() <= f.snap1.GetGeneration() {
		t.Fatalf("second snapshot generation = %d, want > %d", snap2.GetGeneration(), f.snap1.GetGeneration())
	}
	if diff := cmp.Diff(f.v1Runtime, snap2.GetSandboxRuntime(), protocmp.Transform()); diff != "" {
		t.Fatalf("second snapshot sandbox_runtime mismatch (-want v1 +got):\n%s", diff)
	}
}

// TestSandboxRuntimeDisableDropsToRootfs covers disabling a SandboxConfig
// version after an actor (and a Tag created from it) has suspended on it:
//  1. An actor boots on v1, increments its memory, rootfs, and volume counters
//     twice, and suspends; its snapshot and a published Tag created from it are
//     stamped with v1's runtime.
//  2. The pool's SandboxConfig switches defaultVersion to v2 and marks v1
//     Disabled; the worker re-registers with v2 as default_runtime and drops v1
//     from restorable_runtimes.
//  3. Resuming the actor drops from MEMORY to ROOTFS on v2: rootfs and volume
//     counters continue while the memory counter resets, and
//     WorkerAssignment.sandbox_runtime records v2.
//  4. Suspending the actor re-stamps the new MEMORY snapshot with v2, and the
//     next resume restores MEMORY on v2.
//  5. An actor cloned from the v1-stamped Tag also drops to ROOTFS on v2
//     (memory=1, file=3, rootfs=3).
//  6. An actor holding the v1-stamped Tag snapshot whose ActorTemplate is
//     replaced via UpdateActor while v1 is Disabled drops to VOLUMES (not
//     ROOTFS) on v2: both memory and rootfs counters reset while the volume
//     counter continues (memory=1, file=3, rootfs=1).
func TestSandboxRuntimeDisableDropsToRootfs(t *testing.T) {
	t.Parallel()
	f := setupSuspendedV1Actor(t, "runtime-disable", ateapi.DurabilityMode_DURABILITY_MODE_SYNCHRONOUS)

	tagRef := &ateapi.ObjectRef{Atespace: demoAtespace, Name: "tag-" + f.actorName}
	t.Cleanup(func() {
		_, _ = f.clients.SubstrateAPI.DeleteTag(context.Background(), &ateapi.DeleteTagRequest{Tag: tagRef})
	})
	tag, err := f.clients.SubstrateAPI.CreateTag(f.ctx, &ateapi.CreateTagRequest{
		Tag: &ateapi.Tag{
			Metadata:    &ateapi.ResourceMetadata{Atespace: demoAtespace, Name: tagRef.GetName()},
			Scope:       ateapi.TagScope_TAG_SCOPE_ATESPACE,
			SourceActor: f.actorRef,
		},
	})
	if err != nil {
		t.Fatalf("failed to create Tag from v1 actor: %v", err)
	}
	if diff := cmp.Diff(f.v1Runtime, tag.GetStatus().GetSnapshot().GetSandboxRuntime(), protocmp.Transform()); diff != "" {
		t.Fatalf("tag snapshot stamp mismatch (-want v1 +got):\n%s", diff)
	}
	tag.Scope = ateapi.TagScope_TAG_SCOPE_PUBLISHED
	if _, err := f.clients.SubstrateAPI.UpdateTag(f.ctx, &ateapi.UpdateTagRequest{Tag: tag}); err != nil {
		t.Fatalf("failed to publish Tag: %v", err)
	}

	// 2. Switch defaultVersion to v2 and mark v1 Disabled.
	disabled := atev1.SandboxVersionStateDisabled
	v1Disabled := f.v1
	v1Disabled.State = &disabled
	updateSandboxConfigVersions(t, f.ctx, f.clients, f.configName, f.v2.Name, []atev1.SandboxVersionConfig{v1Disabled, f.v2})
	v2Runtime := waitForWorkerRuntimeDisabled(t, f.ctx, f.clients, f.workerRef, f.v1Runtime)

	// 3. Resume the actor: no worker can restore v1, so it drops to ROOTFS on v2.
	resumed2, err := e2e.ResumeActorAwaitCapacity(t, f.ctx, f.clients, &ateapi.ResumeActorRequest{Actor: f.actorRef})
	if err != nil {
		t.Fatalf("failed to resume Actor after v1 disabled: %v", err)
	}
	waitForActorState(f.ctx, t, f.clients, f.actorName, ateapi.ActorState_ACTOR_STATE_RUNNING)

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
	suspended2, err := f.clients.SubstrateAPI.SuspendActor(f.ctx, &ateapi.SuspendActorRequest{
		Actor:   f.actorRef,
		Options: &ateapi.SuspendOptions{Mode: ateapi.DurabilityMode_DURABILITY_MODE_SYNCHRONOUS},
	})
	if err != nil {
		t.Fatalf("failed to suspend Actor on v2: %v", err)
	}
	waitForActorState(f.ctx, t, f.clients, f.actorName, ateapi.ActorState_ACTOR_STATE_SUSPENDED)

	snap2 := durableSnapshot(suspended2.GetActor().GetStatus())
	if snap2.GetGeneration() <= f.snap1.GetGeneration() {
		t.Fatalf("second snapshot generation = %d, want > %d", snap2.GetGeneration(), f.snap1.GetGeneration())
	}
	if got := snap2.GetFidelity(); got != ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY {
		t.Fatalf("snapshot fidelity after v2 suspend = %v, want MEMORY", got)
	}
	if diff := cmp.Diff(v2Runtime, snap2.GetSandboxRuntime(), protocmp.Transform()); diff != "" {
		t.Fatalf("second snapshot sandbox_runtime mismatch (-want v2 +got):\n%s", diff)
	}

	resumed3, err := e2e.ResumeActorAwaitCapacity(t, f.ctx, f.clients, &ateapi.ResumeActorRequest{Actor: f.actorRef})
	if err != nil {
		t.Fatalf("failed to resume Actor on v2: %v", err)
	}
	waitForActorState(f.ctx, t, f.clients, f.actorName, ateapi.ActorState_ACTOR_STATE_RUNNING)
	if diff := cmp.Diff(v2Runtime, resumed3.GetActor().GetStatus().GetWorkerAssignment().GetSandboxRuntime(), protocmp.Transform()); diff != "" {
		t.Fatalf("workerAssignment.sandbox_runtime on v2 MEMORY resume (-want v2 +got):\n%s", diff)
	}

	resp, err = callActor(t, resources.ActorRef{Atespace: demoAtespace, Name: f.actorName})
	if err != nil {
		t.Fatalf("failed to call actor after v2 MEMORY resume: %v", err)
	}
	validateCounterResponse(t, resp, "after v2 MEMORY resume", 2, 4)
	validateRootfsCounter(t, resp, "after v2 MEMORY resume", 4)

	if _, err := f.clients.SubstrateAPI.SuspendActor(f.ctx, &ateapi.SuspendActorRequest{
		Actor:   f.actorRef,
		Options: &ateapi.SuspendOptions{Mode: ateapi.DurabilityMode_DURABILITY_MODE_SYNCHRONOUS},
	}); err != nil {
		t.Fatalf("failed to suspend Actor before cloning from Tag: %v", err)
	}
	waitForActorState(f.ctx, t, f.clients, f.actorName, ateapi.ActorState_ACTOR_STATE_SUSPENDED)

	// 5. Clone an actor from the v1-stamped Tag and resume it on v2: it also
	// drops from MEMORY to ROOTFS (C5).
	cloneName := "clone-" + f.actorName
	cloneRef := &ateapi.ObjectRef{Atespace: demoAtespace, Name: cloneName}
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		_, _ = f.clients.SubstrateAPI.SuspendActor(cleanupCtx, &ateapi.SuspendActorRequest{Actor: cloneRef})
		_, _ = f.clients.SubstrateAPI.DeleteActor(cleanupCtx, &ateapi.DeleteActorRequest{Actor: cloneRef})
	})

	cloned, err := f.clients.SubstrateAPI.CreateActor(f.ctx, &ateapi.CreateActorRequest{
		Actor: &ateapi.Actor{
			Metadata:      &ateapi.ResourceMetadata{Atespace: demoAtespace, Name: cloneName},
			ActorTemplate: f.templateRef,
			SourceTag:     tagRef,
		},
	})
	if err != nil {
		t.Fatalf("failed to create cloned Actor from Tag: %v", err)
	}
	if diff := cmp.Diff(f.v1Runtime, durableSnapshot(cloned.GetStatus()).GetSandboxRuntime(), protocmp.Transform()); diff != "" {
		t.Fatalf("cloned actor borrowed tag snapshot stamp (-want v1 +got):\n%s", diff)
	}

	cloneResumed, err := e2e.ResumeActorAwaitCapacity(t, f.ctx, f.clients, &ateapi.ResumeActorRequest{Actor: cloneRef})
	if err != nil {
		t.Fatalf("failed to resume cloned Actor after v1 disabled: %v", err)
	}
	waitForActorState(f.ctx, t, f.clients, cloneName, ateapi.ActorState_ACTOR_STATE_RUNNING)
	if diff := cmp.Diff(v2Runtime, cloneResumed.GetActor().GetStatus().GetWorkerAssignment().GetSandboxRuntime(), protocmp.Transform()); diff != "" {
		t.Fatalf("cloned actor assignment runtime after ROOTFS fallback (-want v2 +got):\n%s", diff)
	}

	resp, err = callActor(t, resources.ActorRef{Atespace: demoAtespace, Name: cloneName})
	if err != nil {
		t.Fatalf("failed to call cloned actor after ROOTFS fallback: %v", err)
	}
	validateCounterResponse(t, resp, "cloned actor after ROOTFS fallback", 1, 3)
	validateRootfsCounter(t, resp, "cloned actor after ROOTFS fallback", 3)

	if _, err := f.clients.SubstrateAPI.SuspendActor(f.ctx, &ateapi.SuspendActorRequest{
		Actor:   cloneRef,
		Options: &ateapi.SuspendOptions{Mode: ateapi.DurabilityMode_DURABILITY_MODE_SYNCHRONOUS},
	}); err != nil {
		t.Fatalf("failed to suspend cloned Actor before template replacement test: %v", err)
	}
	waitForActorState(f.ctx, t, f.clients, cloneName, ateapi.ActorState_ACTOR_STATE_SUSPENDED)

	// 6. Create a second ActorTemplate (tmplB) targeting the same v2 pool,
	// create an actor from the v1-stamped Tag, repoint it at tmplB via
	// UpdateActor, and resume it while v1 is Disabled: template replacement
	// takes precedence over runtime fallback and drops the restore to VOLUMES
	// (C6 x C3: memory=1, file=3, rootfs=1).
	tmplBName := "tmpl-b-" + f.actorName
	tmplBRef := &ateapi.ObjectRef{Atespace: demoAtespace, Name: tmplBName}
	tmplBSpec := &ateapi.ActorTemplate{
		Metadata:       &ateapi.ResourceMetadata{Atespace: demoAtespace, Name: tmplBName},
		WorkerSelector: f.tmpl.GetWorkerSelector(),
		Containers:     f.tmpl.GetContainers(),
		Resources:      f.tmpl.GetResources(),
		Volumes:        f.tmpl.GetVolumes(),
		SnapshotConfig: &ateapi.SnapshotConfig{
			StorageLocation:      f.tmpl.GetSnapshotConfig().GetStorageLocation() + "-b",
			PreferredFidelity:    ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY,
			GoldenSnapshotConfig: &ateapi.GoldenSnapshotConfig{Mode: ateapi.GoldenSnapshotMode_GOLDEN_SNAPSHOT_MODE_DISABLED},
		},
	}
	if _, err := f.clients.SubstrateAPI.CreateActorTemplate(f.ctx, &ateapi.CreateActorTemplateRequest{ActorTemplate: tmplBSpec}); err != nil {
		t.Fatalf("failed to create second ActorTemplate %s: %v", tmplBName, err)
	}
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		_, _ = f.clients.SubstrateAPI.DeleteActorTemplate(cleanupCtx, &ateapi.DeleteActorTemplateRequest{ActorTemplate: tmplBRef})
	})

	repointName := "repoint-" + f.actorName
	repointRef := &ateapi.ObjectRef{Atespace: demoAtespace, Name: repointName}
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		_, _ = f.clients.SubstrateAPI.SuspendActor(cleanupCtx, &ateapi.SuspendActorRequest{Actor: repointRef})
		_, _ = f.clients.SubstrateAPI.DeleteActor(cleanupCtx, &ateapi.DeleteActorRequest{Actor: repointRef})
	})

	repointed, err := f.clients.SubstrateAPI.CreateActor(f.ctx, &ateapi.CreateActorRequest{
		Actor: &ateapi.Actor{
			Metadata:      &ateapi.ResourceMetadata{Atespace: demoAtespace, Name: repointName},
			ActorTemplate: f.templateRef,
			SourceTag:     tagRef,
		},
	})
	if err != nil {
		t.Fatalf("failed to create repoint Actor from Tag: %v", err)
	}
	if _, err := f.clients.SubstrateAPI.UpdateActor(f.ctx, &ateapi.UpdateActorRequest{
		Actor: &ateapi.Actor{
			Metadata:      repointed.GetMetadata(),
			ActorTemplate: tmplBRef,
		},
	}); err != nil {
		t.Fatalf("failed to update Actor %s to template %s: %v", repointName, tmplBName, err)
	}

	repointResumed, err := e2e.ResumeActorAwaitCapacity(t, f.ctx, f.clients, &ateapi.ResumeActorRequest{Actor: repointRef})
	if err != nil {
		t.Fatalf("failed to resume repointed Actor after v1 disabled: %v", err)
	}
	waitForActorState(f.ctx, t, f.clients, repointName, ateapi.ActorState_ACTOR_STATE_RUNNING)
	if diff := cmp.Diff(v2Runtime, repointResumed.GetActor().GetStatus().GetWorkerAssignment().GetSandboxRuntime(), protocmp.Transform()); diff != "" {
		t.Fatalf("repointed actor assignment runtime after VOLUMES fallback (-want v2 +got):\n%s", diff)
	}

	resp, err = callActor(t, resources.ActorRef{Atespace: demoAtespace, Name: repointName})
	if err != nil {
		t.Fatalf("failed to call repointed actor after VOLUMES fallback: %v", err)
	}
	validateCounterResponse(t, resp, "repointed actor after VOLUMES fallback", 1, 3)
	validateRootfsCounter(t, resp, "repointed actor after VOLUMES fallback", 1)
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

	class := atev1.SandboxClassGvisor
	if e2e.IsMicroVM() {
		class = atev1.SandboxClassMicroVM
	}
	baseConfig := newestClassDefault(t, ctx, clients, class)
	v1 := defaultVersionFromConfig(t, baseConfig, "v1")
	v2 := secondSandboxVersion(v1, "v2", class)

	configAName := "e2e-switch-a-" + nsObj.Name
	configBName := "e2e-switch-b-" + nsObj.Name
	poolName := "runtime-switch"
	for _, cfg := range []struct {
		name string
		ver  atev1.SandboxVersionConfig
	}{
		{name: configAName, ver: v1},
		{name: configBName, ver: v2},
	} {
		if _, err := clients.SubstrateK8s.AteV1().SandboxConfigs().Create(ctx, &atev1.SandboxConfig{
			ObjectMeta: metav1.ObjectMeta{Name: cfg.name},
			Spec: atev1.SandboxConfigSpec{
				SandboxClass:   class,
				DefaultVersion: cfg.ver.Name,
				Versions:       []atev1.SandboxVersionConfig{cfg.ver},
			},
		}, metav1.CreateOptions{}); err != nil {
			t.Fatalf("failed to create SandboxConfig %s: %v", cfg.name, err)
		}
	}
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		_ = clients.SubstrateK8s.AteV1().WorkerPools(nsObj.Name).Delete(cleanupCtx, poolName, metav1.DeleteOptions{})
		for _, name := range []string{configAName, configBName} {
			if err := clients.SubstrateK8s.AteV1().SandboxConfigs().Delete(cleanupCtx, name, metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
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
		SnapshotConfig: &ateapi.SnapshotConfig{
			StorageLocation:   "gs://" + env["BUCKET_NAME"] + "/ate-demo-" + tmplName,
			PreferredFidelity: ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY,
		},
	})

	actor1Name := "switch-actor1-" + nsObj.Name
	actor2Name := "switch-actor2-" + nsObj.Name
	actor1Ref := &ateapi.ObjectRef{Atespace: demoAtespace, Name: actor1Name}
	actor2Ref := &ateapi.ObjectRef{Atespace: demoAtespace, Name: actor2Name}
	for _, ref := range []*ateapi.ObjectRef{actor1Ref, actor2Ref} {
		t.Cleanup(func() {
			cleanupCtx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer cancel()
			_, _ = clients.SubstrateAPI.SuspendActor(cleanupCtx, &ateapi.SuspendActorRequest{Actor: ref})
			_, _ = clients.SubstrateAPI.DeleteActor(cleanupCtx, &ateapi.DeleteActorRequest{Actor: ref})
		})
	}

	// 1. Create and resume actor1 on configA (v1), and call it once.
	if _, err := clients.SubstrateAPI.CreateActor(ctx, &ateapi.CreateActorRequest{Actor: &ateapi.Actor{
		Metadata:      &ateapi.ResourceMetadata{Atespace: demoAtespace, Name: actor1Name},
		ActorTemplate: e2e.TemplateRef(tmpl),
	}}); err != nil {
		t.Fatalf("failed to create actor1: %v", err)
	}
	waitForActorState(ctx, t, clients, actor1Name, ateapi.ActorState_ACTOR_STATE_SUSPENDED)

	resumed1, err := e2e.ResumeActorAwaitCapacity(t, ctx, clients, &ateapi.ResumeActorRequest{Actor: actor1Ref})
	if err != nil {
		t.Fatalf("failed to resume actor1 on configA: %v", err)
	}
	waitForActorState(ctx, t, clients, actor1Name, ateapi.ActorState_ACTOR_STATE_RUNNING)

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
	suspended1, err := clients.SubstrateAPI.SuspendActor(ctx, &ateapi.SuspendActorRequest{
		Actor:   actor1Ref,
		Options: &ateapi.SuspendOptions{Mode: ateapi.DurabilityMode_DURABILITY_MODE_SYNCHRONOUS},
	})
	if err != nil {
		t.Fatalf("failed to suspend actor1: %v", err)
	}
	waitForActorState(ctx, t, clients, actor1Name, ateapi.ActorState_ACTOR_STATE_SUSPENDED)
	if diff := cmp.Diff(v1Runtime, durableSnapshot(suspended1.GetActor().GetStatus()).GetSandboxRuntime(), protocmp.Transform()); diff != "" {
		t.Fatalf("actor1 snapshot stamp after pool switch (-want v1 +got):\n%s", diff)
	}

	// Resume actor1 on configB: it drops from MEMORY to ROOTFS on v2.
	resumed1OnB, err := e2e.ResumeActorAwaitCapacity(t, ctx, clients, &ateapi.ResumeActorRequest{Actor: actor1Ref})
	if err != nil {
		t.Fatalf("failed to resume actor1 on configB: %v", err)
	}
	waitForActorState(ctx, t, clients, actor1Name, ateapi.ActorState_ACTOR_STATE_RUNNING)
	if diff := cmp.Diff(v2Runtime, resumed1OnB.GetActor().GetStatus().GetWorkerAssignment().GetSandboxRuntime(), protocmp.Transform()); diff != "" {
		t.Fatalf("actor1 assignment runtime on configB (-want v2 +got):\n%s", diff)
	}

	resp, err = callActor(t, resources.ActorRef{Atespace: demoAtespace, Name: actor1Name})
	if err != nil {
		t.Fatalf("failed to call actor1 after ROOTFS fallback on configB: %v", err)
	}
	validateCounterResponse(t, resp, "actor1 on configB", 1, 3)
	validateRootfsCounter(t, resp, "actor1 on configB", 3)

	if _, err := clients.SubstrateAPI.SuspendActor(ctx, &ateapi.SuspendActorRequest{
		Actor:   actor1Ref,
		Options: &ateapi.SuspendOptions{Mode: ateapi.DurabilityMode_DURABILITY_MODE_SYNCHRONOUS},
	}); err != nil {
		t.Fatalf("failed to suspend actor1 on configB: %v", err)
	}
	waitForActorState(ctx, t, clients, actor1Name, ateapi.ActorState_ACTOR_STATE_SUSPENDED)

	// 4. Create actor2 from the v1-stamped golden template and resume it on configB (C4).
	created2, err := clients.SubstrateAPI.CreateActor(ctx, &ateapi.CreateActorRequest{Actor: &ateapi.Actor{
		Metadata:      &ateapi.ResourceMetadata{Atespace: demoAtespace, Name: actor2Name},
		ActorTemplate: e2e.TemplateRef(tmpl),
	}})
	if err != nil {
		t.Fatalf("failed to create actor2 from golden template: %v", err)
	}
	if diff := cmp.Diff(v1Runtime, durableSnapshot(created2.GetStatus()).GetSandboxRuntime(), protocmp.Transform()); diff != "" {
		t.Fatalf("actor2 borrowed golden snapshot stamp (-want v1 +got):\n%s", diff)
	}

	resumed2, err := e2e.ResumeActorAwaitCapacity(t, ctx, clients, &ateapi.ResumeActorRequest{Actor: actor2Ref})
	if err != nil {
		t.Fatalf("failed to resume actor2 from v1 golden snapshot on configB: %v", err)
	}
	waitForActorState(ctx, t, clients, actor2Name, ateapi.ActorState_ACTOR_STATE_RUNNING)
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

// TestPausedActorSandboxRuntime covers a paused actor (DURABILITY_MODE_ASYNCHRONOUS)
// holding a node-local MEMORY checkpoint across SandboxConfig version changes:
//  1. An actor boots on v1, increments its memory, rootfs, and volume counters
//     twice, and pauses; its node-local MEMORY checkpoint is stamped with v1.
//  2. The pool's SandboxConfig switches defaultVersion to v2 while keeping v1
//     Enabled; resuming the paused actor matches v1 via restorable_runtimes on
//     the pinned node without detaching the local checkpoint, preserving the
//     in-memory counter (memory=3, file=3, rootfs=3).
//  3. The actor pauses again on v1, and SandboxConfig marks v1 Disabled.
//  4. Resuming the paused actor cannot relax the runtime while pinned to its
//     local checkpoint, so ResumeActor detaches the checkpoint to durable
//     storage first and then restores at ROOTFS fidelity on v2 (memory=1,
//     file=4, rootfs=4).
func TestPausedActorSandboxRuntime(t *testing.T) {
	t.Parallel()
	f := setupSuspendedV1Actor(t, "runtime-paused", ateapi.DurabilityMode_DURABILITY_MODE_ASYNCHRONOUS)

	// 2. Add v2 (Enabled) and flip defaultVersion from v1 to v2 while v1 stays Enabled.
	updateSandboxConfigVersions(t, f.ctx, f.clients, f.configName, f.v2.Name, []atev1.SandboxVersionConfig{f.v1, f.v2})
	waitForWorkerRestorableUpgrade(t, f.ctx, f.clients, f.workerRef, f.v1Runtime)

	// Resume from the node-local checkpoint via restorable_runtimes.
	resumed2, err := e2e.ResumeActorAwaitCapacity(t, f.ctx, f.clients, &ateapi.ResumeActorRequest{Actor: f.actorRef})
	if err != nil {
		t.Fatalf("failed to resume paused Actor via restorable runtime: %v", err)
	}
	waitForActorState(f.ctx, t, f.clients, f.actorName, ateapi.ActorState_ACTOR_STATE_RUNNING)

	if diff := cmp.Diff(f.v1Runtime, resumed2.GetActor().GetStatus().GetWorkerAssignment().GetSandboxRuntime(), protocmp.Transform()); diff != "" {
		t.Fatalf("workerAssignment.sandbox_runtime after local restorable resume (-want v1 +got):\n%s", diff)
	}
	if !hasLocalSnapshot(resumed2.GetActor().GetStatus()) {
		t.Fatalf("expected local checkpoint to remain attached after matching restorable_runtimes, got %v", resumed2.GetActor().GetStatus().GetSnapshots())
	}

	resp, err := callActor(t, resources.ActorRef{Atespace: demoAtespace, Name: f.actorName})
	if err != nil {
		t.Fatalf("failed to call actor after local restorable resume: %v", err)
	}
	validateCounterResponse(t, resp, "after local restorable resume", 3, 3)
	validateRootfsCounter(t, resp, "after local restorable resume", 3)

	// 3. Pause the actor again on v1, then mark v1 Disabled.
	paused2, err := f.clients.SubstrateAPI.SuspendActor(f.ctx, &ateapi.SuspendActorRequest{
		Actor:   f.actorRef,
		Options: &ateapi.SuspendOptions{Mode: ateapi.DurabilityMode_DURABILITY_MODE_ASYNCHRONOUS},
	})
	if err != nil {
		t.Fatalf("failed to pause Actor a second time: %v", err)
	}
	waitForActorPaused(f.ctx, t, f.clients, f.actorName)

	snap2 := localSnapshot(paused2.GetActor().GetStatus())
	if snap2.GetGeneration() <= f.snap1.GetGeneration() {
		t.Fatalf("second local snapshot generation = %d, want > %d", snap2.GetGeneration(), f.snap1.GetGeneration())
	}
	if diff := cmp.Diff(f.v1Runtime, snap2.GetSandboxRuntime(), protocmp.Transform()); diff != "" {
		t.Fatalf("second local snapshot sandbox_runtime mismatch (-want v1 +got):\n%s", diff)
	}

	disabled := atev1.SandboxVersionStateDisabled
	v1Disabled := f.v1
	v1Disabled.State = &disabled
	updateSandboxConfigVersions(t, f.ctx, f.clients, f.configName, f.v2.Name, []atev1.SandboxVersionConfig{v1Disabled, f.v2})
	v2Runtime := waitForWorkerRuntimeDisabled(t, f.ctx, f.clients, f.workerRef, f.v1Runtime)

	// 4. Resume the paused actor: ResumeActor detaches the local checkpoint to
	// durable storage before relaxing the runtime to v2 (ROOTFS fallback).
	resumed3, err := e2e.ResumeActorAwaitCapacity(t, f.ctx, f.clients, &ateapi.ResumeActorRequest{Actor: f.actorRef})
	if err != nil {
		t.Fatalf("failed to resume paused Actor after v1 disabled: %v", err)
	}
	waitForActorState(f.ctx, t, f.clients, f.actorName, ateapi.ActorState_ACTOR_STATE_RUNNING)

	status3 := resumed3.GetActor().GetStatus()
	if diff := cmp.Diff(v2Runtime, status3.GetWorkerAssignment().GetSandboxRuntime(), protocmp.Transform()); diff != "" {
		t.Fatalf("workerAssignment.sandbox_runtime after paused detach and ROOTFS fallback (-want v2 +got):\n%s", diff)
	}
	if hasLocalSnapshot(status3) {
		t.Fatalf("local snapshot survived runtime fallback on a paused actor: %v", status3.GetSnapshots())
	}
	detachedDurable := durableSnapshot(status3)
	if detachedDurable.GetGeneration() != snap2.GetGeneration() {
		t.Fatalf("durable snapshot generation after detach = %d, want %d", detachedDurable.GetGeneration(), snap2.GetGeneration())
	}
	if diff := cmp.Diff(f.v1Runtime, detachedDurable.GetSandboxRuntime(), protocmp.Transform()); diff != "" {
		t.Fatalf("detached durable snapshot sandbox_runtime mismatch (-want v1 +got):\n%s", diff)
	}

	resp, err = callActor(t, resources.ActorRef{Atespace: demoAtespace, Name: f.actorName})
	if err != nil {
		t.Fatalf("failed to call actor after paused detach and ROOTFS fallback: %v", err)
	}
	validateCounterResponse(t, resp, "after paused detach and ROOTFS fallback", 1, 4)
	validateRootfsCounter(t, resp, "after paused detach and ROOTFS fallback", 4)
}

// TestSchedulerPrefersCompatibleWorkerPool covers runtime-aware placement across
// two WorkerPools matching the same ActorTemplate selector:
//  1. An actor boots on poolA (configA at v1), increments its counters twice
//     (memory=2, file=2, rootfs=2), and suspends with a v1-stamped MEMORY
//     snapshot.
//  2. A second pool, poolB (configB at v2), starts in the same namespace with
//     the same selector labels and reaches WORKER_STATE_ACTIVE.
//  3. Resuming the actor with both pools free selects poolA's compatible worker
//     and restores at MEMORY fidelity (memory=3, file=3, rootfs=3).
//  4. After suspending again on v1, configA disables v1 and switches to v2 so
//     neither pool advertises v1; resuming falls back to ROOTFS on v2
//     (memory=1, file=4, rootfs=4).
func TestSchedulerPrefersCompatibleWorkerPool(t *testing.T) {
	t.Parallel()
	f := setupSuspendedV1Actor(t, "runtime-prefer-pool", ateapi.DurabilityMode_DURABILITY_MODE_SYNCHRONOUS)

	// 2. Create configB (v2) and poolB with the same labels as poolA, then wait
	// until poolB's worker is ACTIVE on v2.
	configBName := f.configName + "-b"
	if _, err := f.clients.SubstrateK8s.AteV1().SandboxConfigs().Create(f.ctx, &atev1.SandboxConfig{
		ObjectMeta: metav1.ObjectMeta{Name: configBName},
		Spec: atev1.SandboxConfigSpec{
			SandboxClass:   f.class,
			DefaultVersion: f.v2.Name,
			Versions:       []atev1.SandboxVersionConfig{f.v2},
		},
	}, metav1.CreateOptions{}); err != nil {
		t.Fatalf("failed to create SandboxConfig %s: %v", configBName, err)
	}

	poolA, err := f.clients.SubstrateK8s.AteV1().WorkerPools(f.namespace).Get(f.ctx, f.poolName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("failed to get WorkerPool %s/%s: %v", f.namespace, f.poolName, err)
	}
	sandboxClassesB := slices.Clone(poolA.Spec.SandboxClasses)
	for i := range sandboxClassesB {
		sandboxClassesB[i].ConfigRef = &atev1.SandboxConfigReference{Name: configBName}
	}
	poolBName := f.poolName + "-b"
	if _, err := f.clients.SubstrateK8s.AteV1().WorkerPools(f.namespace).Create(f.ctx, &atev1.WorkerPool{
		ObjectMeta: metav1.ObjectMeta{
			Name:      poolBName,
			Namespace: f.namespace,
			Labels:    poolA.Labels,
		},
		Spec: atev1.WorkerPoolSpec{
			Replicas:       1,
			WorkerImage:    poolA.Spec.WorkerImage,
			SandboxClasses: sandboxClassesB,
		},
	}, metav1.CreateOptions{}); err != nil {
		t.Fatalf("failed to create WorkerPool %s/%s: %v", f.namespace, poolBName, err)
	}
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		_ = f.clients.SubstrateK8s.AteV1().WorkerPools(f.namespace).Delete(cleanupCtx, poolBName, metav1.DeleteOptions{})
		if err := f.clients.SubstrateK8s.AteV1().SandboxConfigs().Delete(cleanupCtx, configBName, metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
			t.Errorf("failed to delete SandboxConfig %s: %v", configBName, err)
		}
	})

	v2Runtime := waitForPoolWorkerActive(t, f.ctx, f.clients, f.namespace, poolBName, f.v1Runtime)

	// 3. Resume with both poolA (v1) and poolB (v2) free: the scheduler must
	// choose poolA's compatible worker and perform a MEMORY restore.
	resumed2, err := e2e.ResumeActorAwaitCapacity(t, f.ctx, f.clients, &ateapi.ResumeActorRequest{Actor: f.actorRef})
	if err != nil {
		t.Fatalf("failed to resume Actor with both pools free: %v", err)
	}
	waitForActorState(f.ctx, t, f.clients, f.actorName, ateapi.ActorState_ACTOR_STATE_RUNNING)

	assignment2 := resumed2.GetActor().GetStatus().GetWorkerAssignment()
	if got := assignment2.GetWorker().GetName(); got != f.workerRef.GetName() {
		t.Fatalf("assigned worker = %q (pool %q), want poolA's compatible worker %q", got, assignment2.GetWorkerPool(), f.workerRef.GetName())
	}
	if diff := cmp.Diff(f.v1Runtime, assignment2.GetSandboxRuntime(), protocmp.Transform()); diff != "" {
		t.Fatalf("workerAssignment.sandbox_runtime on preferred pool (-want v1 +got):\n%s", diff)
	}

	resp, err := callActor(t, resources.ActorRef{Atespace: demoAtespace, Name: f.actorName})
	if err != nil {
		t.Fatalf("failed to call actor after preferred-pool MEMORY restore: %v", err)
	}
	validateCounterResponse(t, resp, "after preferred-pool MEMORY restore", 3, 3)
	validateRootfsCounter(t, resp, "after preferred-pool MEMORY restore", 3)

	// 4. Suspend on poolA (stamped v1), disable v1 on configA, and resume:
	// neither pool advertises v1, so the actor drops to ROOTFS on v2.
	suspended2, err := f.clients.SubstrateAPI.SuspendActor(f.ctx, &ateapi.SuspendActorRequest{
		Actor:   f.actorRef,
		Options: &ateapi.SuspendOptions{Mode: ateapi.DurabilityMode_DURABILITY_MODE_SYNCHRONOUS},
	})
	if err != nil {
		t.Fatalf("failed to suspend Actor a second time: %v", err)
	}
	waitForActorState(f.ctx, t, f.clients, f.actorName, ateapi.ActorState_ACTOR_STATE_SUSPENDED)
	if diff := cmp.Diff(f.v1Runtime, durableSnapshot(suspended2.GetActor().GetStatus()).GetSandboxRuntime(), protocmp.Transform()); diff != "" {
		t.Fatalf("second snapshot sandbox_runtime mismatch (-want v1 +got):\n%s", diff)
	}

	disabled := atev1.SandboxVersionStateDisabled
	v1Disabled := f.v1
	v1Disabled.State = &disabled
	updateSandboxConfigVersions(t, f.ctx, f.clients, f.configName, f.v2.Name, []atev1.SandboxVersionConfig{v1Disabled, f.v2})
	waitForWorkerRuntimeDisabled(t, f.ctx, f.clients, f.workerRef, f.v1Runtime)

	resumed3, err := e2e.ResumeActorAwaitCapacity(t, f.ctx, f.clients, &ateapi.ResumeActorRequest{Actor: f.actorRef})
	if err != nil {
		t.Fatalf("failed to resume Actor after v1 disabled on both pools: %v", err)
	}
	waitForActorState(f.ctx, t, f.clients, f.actorName, ateapi.ActorState_ACTOR_STATE_RUNNING)

	if diff := cmp.Diff(v2Runtime, resumed3.GetActor().GetStatus().GetWorkerAssignment().GetSandboxRuntime(), protocmp.Transform()); diff != "" {
		t.Fatalf("workerAssignment.sandbox_runtime after ROOTFS fallback (-want v2 +got):\n%s", diff)
	}

	resp, err = callActor(t, resources.ActorRef{Atespace: demoAtespace, Name: f.actorName})
	if err != nil {
		t.Fatalf("failed to call actor after ROOTFS fallback: %v", err)
	}
	validateCounterResponse(t, resp, "after ROOTFS fallback across pools", 1, 4)
	validateRootfsCounter(t, resp, "after ROOTFS fallback across pools", 4)
}

// TestSandboxConfigVersionMutation verifies that runtime compatibility keys on
// sandbox_bundle_hash rather than SandboxConfig version names:
//  1. An actor boots on version "v1" (bundle A1), increments its counters twice
//     (memory=2, file=2, rootfs=2), and suspends with an A1-stamped MEMORY
//     snapshot.
//  2. The SandboxConfig swaps version names while keeping defaultVersion: "v1":
//     "v1" is given bundle A2's assets and "v2" is given bundle A1's assets.
//     Resuming the actor still matches bundle A1 by hash via restorable_runtimes
//     and restores at MEMORY fidelity (memory=3, file=3, rootfs=3).
//  3. After suspending again on A1, the SandboxConfig keeps only version "v1"
//     with bundle A2's assets (an in-place asset edit under the original name
//     "v1"); resuming drops to ROOTFS on A2 (memory=1, file=4, rootfs=4).
func TestSandboxConfigVersionMutation(t *testing.T) {
	t.Parallel()
	f := setupSuspendedV1Actor(t, "runtime-version-mutation", ateapi.DurabilityMode_DURABILITY_MODE_SYNCHRONOUS)

	// 2. Swap the assets under "v1" and "v2" while keeping defaultVersion: "v1".
	// "v1" now carries v2's assets (default) and "v2" carries v1's original
	// assets (restorable).
	v1WithV2Assets := f.v2
	v1WithV2Assets.Name = f.v1.Name
	v2WithV1Assets := f.v1
	v2WithV1Assets.Name = f.v2.Name
	updateSandboxConfigVersions(t, f.ctx, f.clients, f.configName, f.v1.Name, []atev1.SandboxVersionConfig{v1WithV2Assets, v2WithV1Assets})
	waitForWorkerRestorableUpgrade(t, f.ctx, f.clients, f.workerRef, f.v1Runtime)

	resumed2, err := e2e.ResumeActorAwaitCapacity(t, f.ctx, f.clients, &ateapi.ResumeActorRequest{Actor: f.actorRef})
	if err != nil {
		t.Fatalf("failed to resume Actor after version name swap: %v", err)
	}
	waitForActorState(f.ctx, t, f.clients, f.actorName, ateapi.ActorState_ACTOR_STATE_RUNNING)

	if diff := cmp.Diff(f.v1Runtime, resumed2.GetActor().GetStatus().GetWorkerAssignment().GetSandboxRuntime(), protocmp.Transform()); diff != "" {
		t.Fatalf("workerAssignment.sandbox_runtime after version name swap (-want v1 hash +got):\n%s", diff)
	}

	resp, err := callActor(t, resources.ActorRef{Atespace: demoAtespace, Name: f.actorName})
	if err != nil {
		t.Fatalf("failed to call actor after version name swap: %v", err)
	}
	validateCounterResponse(t, resp, "after version name swap", 3, 3)
	validateRootfsCounter(t, resp, "after version name swap", 3)

	// 3. Suspend on A1, then keep only version "v1" with A2's assets (an
	// in-place asset edit of "v1"). Even though defaultVersion is still "v1",
	// the bundle hash changed, so the next resume drops to ROOTFS.
	suspended2, err := f.clients.SubstrateAPI.SuspendActor(f.ctx, &ateapi.SuspendActorRequest{
		Actor:   f.actorRef,
		Options: &ateapi.SuspendOptions{Mode: ateapi.DurabilityMode_DURABILITY_MODE_SYNCHRONOUS},
	})
	if err != nil {
		t.Fatalf("failed to suspend Actor a second time: %v", err)
	}
	waitForActorState(f.ctx, t, f.clients, f.actorName, ateapi.ActorState_ACTOR_STATE_SUSPENDED)
	if diff := cmp.Diff(f.v1Runtime, durableSnapshot(suspended2.GetActor().GetStatus()).GetSandboxRuntime(), protocmp.Transform()); diff != "" {
		t.Fatalf("second snapshot sandbox_runtime mismatch (-want v1 hash +got):\n%s", diff)
	}

	updateSandboxConfigVersions(t, f.ctx, f.clients, f.configName, f.v1.Name, []atev1.SandboxVersionConfig{v1WithV2Assets})
	v2Runtime := waitForWorkerRuntimeDisabled(t, f.ctx, f.clients, f.workerRef, f.v1Runtime)

	resumed3, err := e2e.ResumeActorAwaitCapacity(t, f.ctx, f.clients, &ateapi.ResumeActorRequest{Actor: f.actorRef})
	if err != nil {
		t.Fatalf("failed to resume Actor after in-place asset edit of v1: %v", err)
	}
	waitForActorState(f.ctx, t, f.clients, f.actorName, ateapi.ActorState_ACTOR_STATE_RUNNING)

	if diff := cmp.Diff(v2Runtime, resumed3.GetActor().GetStatus().GetWorkerAssignment().GetSandboxRuntime(), protocmp.Transform()); diff != "" {
		t.Fatalf("workerAssignment.sandbox_runtime after in-place asset edit (-want v2 hash +got):\n%s", diff)
	}

	resp, err = callActor(t, resources.ActorRef{Atespace: demoAtespace, Name: f.actorName})
	if err != nil {
		t.Fatalf("failed to call actor after in-place asset edit: %v", err)
	}
	validateCounterResponse(t, resp, "after in-place asset edit of v1", 1, 4)
	validateRootfsCounter(t, resp, "after in-place asset edit of v1", 4)
}

// TestBadVersionSHAAndConvergence verifies that flipping defaultVersion to a
// gVisor version with a wrong asset SHA256 preserves the worker's existing
// runtime registration until the SHA256 is fixed:
//  1. An actor boots on v1, increments its counters twice (memory=2, file=2,
//     rootfs=2), and suspends with a v1-stamped MEMORY snapshot.
//  2. The SandboxConfig flips defaultVersion to v2 with a wrong asset SHA256.
//     atelet's syncer fails buildFile on the default version and leaves
//     sandbox-runtimes.json on v1, so the worker keeps advertising v1 and the
//     actor still restores at MEMORY fidelity (memory=3, file=3, rootfs=3).
//  3. After suspending again on v1, v2's SHA256 is fixed in the SandboxConfig;
//     the worker converges to default_runtime=v2 with v1 in restorable_runtimes
//     and the actor restores via restorable_runtimes (memory=4, file=4,
//     rootfs=4).
func TestBadVersionSHAAndConvergence(t *testing.T) {
	if e2e.IsMicroVM() {
		t.Skip("only gVisor fetches and verifies version assets before writing sandbox-runtimes.json")
	}
	t.Parallel()
	f := setupSuspendedV1Actor(t, "runtime-bad-sha", ateapi.DurabilityMode_DURABILITY_MODE_SYNCHRONOUS)

	// 2. Flip defaultVersion to v2 with a wrong SHA256 and wait for atelet on
	// the worker's node to log the sync failure for the worker pod.
	v2Bad := *f.v2.DeepCopy()
	for arch, files := range v2Bad.Assets {
		for name, entry := range files {
			entry.SHA256 = strings.Repeat("0", 64)
			files[name] = entry
		}
		v2Bad.Assets[arch] = files
	}
	updateSandboxConfigVersions(t, f.ctx, f.clients, f.configName, v2Bad.Name, []atev1.SandboxVersionConfig{f.v1, v2Bad})
	waitForAteletSyncFailure(t, f.ctx, f.clients, f.workerRef, f.configName)

	// Verify the worker still advertises v1 as default_runtime with no
	// restorable_runtimes, and that the suspended v1 actor MEMORY-restores.
	w, err := f.clients.SubstrateAPI.GetWorker(f.ctx, &ateapi.GetWorkerRequest{Worker: f.workerRef})
	if err != nil {
		t.Fatalf("failed to get Worker %s after bad defaultVersion sync: %v", f.workerRef.GetName(), err)
	}
	if diff := cmp.Diff(f.v1Runtime, hardware.ColdBootRuntime(w.GetStatus()), protocmp.Transform()); diff != "" {
		t.Fatalf("worker default_runtime changed after bad defaultVersion sync (-want v1 +got):\n%s", diff)
	}

	resumed2, err := e2e.ResumeActorAwaitCapacity(t, f.ctx, f.clients, &ateapi.ResumeActorRequest{Actor: f.actorRef})
	if err != nil {
		t.Fatalf("failed to resume Actor while defaultVersion has a bad SHA256: %v", err)
	}
	waitForActorState(f.ctx, t, f.clients, f.actorName, ateapi.ActorState_ACTOR_STATE_RUNNING)

	if diff := cmp.Diff(f.v1Runtime, resumed2.GetActor().GetStatus().GetWorkerAssignment().GetSandboxRuntime(), protocmp.Transform()); diff != "" {
		t.Fatalf("workerAssignment.sandbox_runtime while defaultVersion has a bad SHA256 (-want v1 +got):\n%s", diff)
	}

	resp, err := callActor(t, resources.ActorRef{Atespace: demoAtespace, Name: f.actorName})
	if err != nil {
		t.Fatalf("failed to call actor while defaultVersion has a bad SHA256: %v", err)
	}
	validateCounterResponse(t, resp, "while defaultVersion has bad SHA256", 3, 3)
	validateRootfsCounter(t, resp, "while defaultVersion has bad SHA256", 3)

	// 3. Suspend on v1, fix v2's SHA256 in the SandboxConfig, and verify the
	// worker converges to default_runtime=v2 with v1 in restorable_runtimes.
	suspended2, err := f.clients.SubstrateAPI.SuspendActor(f.ctx, &ateapi.SuspendActorRequest{
		Actor:   f.actorRef,
		Options: &ateapi.SuspendOptions{Mode: ateapi.DurabilityMode_DURABILITY_MODE_SYNCHRONOUS},
	})
	if err != nil {
		t.Fatalf("failed to suspend Actor a second time: %v", err)
	}
	waitForActorState(f.ctx, t, f.clients, f.actorName, ateapi.ActorState_ACTOR_STATE_SUSPENDED)
	if diff := cmp.Diff(f.v1Runtime, durableSnapshot(suspended2.GetActor().GetStatus()).GetSandboxRuntime(), protocmp.Transform()); diff != "" {
		t.Fatalf("second snapshot sandbox_runtime mismatch (-want v1 +got):\n%s", diff)
	}

	updateSandboxConfigVersions(t, f.ctx, f.clients, f.configName, f.v2.Name, []atev1.SandboxVersionConfig{f.v1, f.v2})
	waitForWorkerRestorableUpgrade(t, f.ctx, f.clients, f.workerRef, f.v1Runtime)

	resumed3, err := e2e.ResumeActorAwaitCapacity(t, f.ctx, f.clients, &ateapi.ResumeActorRequest{Actor: f.actorRef})
	if err != nil {
		t.Fatalf("failed to resume Actor after fixing v2 SHA256: %v", err)
	}
	waitForActorState(f.ctx, t, f.clients, f.actorName, ateapi.ActorState_ACTOR_STATE_RUNNING)

	if diff := cmp.Diff(f.v1Runtime, resumed3.GetActor().GetStatus().GetWorkerAssignment().GetSandboxRuntime(), protocmp.Transform()); diff != "" {
		t.Fatalf("workerAssignment.sandbox_runtime after convergence (-want v1 +got):\n%s", diff)
	}

	resp, err = callActor(t, resources.ActorRef{Atespace: demoAtespace, Name: f.actorName})
	if err != nil {
		t.Fatalf("failed to call actor after convergence: %v", err)
	}
	validateCounterResponse(t, resp, "after fixing v2 SHA256", 4, 4)
	validateRootfsCounter(t, resp, "after fixing v2 SHA256", 4)
}

type sandboxRuntimeFixture struct {
	ctx         context.Context
	clients     *e2e.Clients
	namespace   string
	poolName    string
	class       atev1.SandboxClass
	configName  string
	v1, v2      atev1.SandboxVersionConfig
	tmpl        *ateapi.ActorTemplate
	templateRef *ateapi.ObjectRef
	actorName   string
	actorRef    *ateapi.ObjectRef
	workerRef   *ateapi.ObjectRef
	v1Runtime   *ateapi.ActorSandboxRuntime
	snap1       *ateapi.Snapshot
}

// setupSuspendedV1Actor creates a per-test SandboxConfig (at v1), WorkerPool,
// ActorTemplate, and Actor, runs the actor on v1 for two calls (memory=2,
// file=2, rootfs=2), suspends it with mode, and verifies its MEMORY snapshot
// is stamped with v1.
func setupSuspendedV1Actor(t *testing.T, prefix string, mode ateapi.DurabilityMode) *sandboxRuntimeFixture {
	t.Helper()
	nsObj := e2e.CreateNamespace(t)
	ctx := context.Background()
	clients := e2e.GetClients()

	env, err := e2e.CheckEnv("BUCKET_NAME")
	if err != nil {
		t.Fatalf("CheckEnv failed: %v", err)
	}

	class := atev1.SandboxClassGvisor
	if e2e.IsMicroVM() {
		class = atev1.SandboxClassMicroVM
	}
	baseConfig := newestClassDefault(t, ctx, clients, class)
	v1 := defaultVersionFromConfig(t, baseConfig, "v1")
	v2 := secondSandboxVersion(v1, "v2", class)

	configName := "e2e-" + prefix + "-" + nsObj.Name
	if _, err := clients.SubstrateK8s.AteV1().SandboxConfigs().Create(ctx, &atev1.SandboxConfig{
		ObjectMeta: metav1.ObjectMeta{Name: configName},
		Spec: atev1.SandboxConfigSpec{
			SandboxClass:   class,
			DefaultVersion: v1.Name,
			Versions:       []atev1.SandboxVersionConfig{v1},
		},
	}, metav1.CreateOptions{}); err != nil {
		t.Fatalf("failed to create SandboxConfig %s: %v", configName, err)
	}
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		_ = clients.SubstrateK8s.AteV1().WorkerPools(nsObj.Name).Delete(cleanupCtx, prefix, metav1.DeleteOptions{})
		if err := clients.SubstrateK8s.AteV1().SandboxConfigs().Delete(cleanupCtx, configName, metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
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
		SnapshotConfig: &ateapi.SnapshotConfig{
			StorageLocation:   "gs://" + env["BUCKET_NAME"] + "/ate-demo-" + tmplName,
			PreferredFidelity: ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY,
		},
	})

	actorName := prefix + "-" + nsObj.Name
	actorRef := &ateapi.ObjectRef{Atespace: demoAtespace, Name: actorName}
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		_, _ = clients.SubstrateAPI.SuspendActor(cleanupCtx, &ateapi.SuspendActorRequest{Actor: actorRef})
		_, _ = clients.SubstrateAPI.DeleteActor(cleanupCtx, &ateapi.DeleteActorRequest{Actor: actorRef})
	})

	if _, err := clients.SubstrateAPI.CreateActor(ctx, &ateapi.CreateActorRequest{Actor: &ateapi.Actor{
		Metadata:      &ateapi.ResourceMetadata{Atespace: demoAtespace, Name: actorName},
		ActorTemplate: e2e.TemplateRef(tmpl),
	}}); err != nil {
		t.Fatalf("failed to create Actor: %v", err)
	}
	waitForActorState(ctx, t, clients, actorName, ateapi.ActorState_ACTOR_STATE_SUSPENDED)

	resumed1, err := e2e.ResumeActorAwaitCapacity(t, ctx, clients, &ateapi.ResumeActorRequest{Actor: actorRef})
	if err != nil {
		t.Fatalf("failed to resume Actor on v1: %v", err)
	}
	waitForActorState(ctx, t, clients, actorName, ateapi.ActorState_ACTOR_STATE_RUNNING)

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

	suspended1, err := clients.SubstrateAPI.SuspendActor(ctx, &ateapi.SuspendActorRequest{
		Actor:   actorRef,
		Options: &ateapi.SuspendOptions{Mode: mode},
	})
	if err != nil {
		t.Fatalf("failed to suspend Actor on v1: %v", err)
	}
	var snap1 *ateapi.Snapshot
	if mode == ateapi.DurabilityMode_DURABILITY_MODE_ASYNCHRONOUS {
		waitForActorPaused(ctx, t, clients, actorName)
		snap1 = localSnapshot(suspended1.GetActor().GetStatus())
	} else {
		waitForActorState(ctx, t, clients, actorName, ateapi.ActorState_ACTOR_STATE_SUSPENDED)
		snap1 = durableSnapshot(suspended1.GetActor().GetStatus())
	}
	if got := snap1.GetFidelity(); got != ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY {
		t.Fatalf("snapshot fidelity after v1 suspend = %v, want MEMORY", got)
	}
	if diff := cmp.Diff(v1Runtime, snap1.GetSandboxRuntime(), protocmp.Transform()); diff != "" {
		t.Fatalf("snapshot sandbox_runtime mismatch after v1 suspend (-want +got):\n%s", diff)
	}

	return &sandboxRuntimeFixture{
		ctx:         ctx,
		clients:     clients,
		namespace:   nsObj.Name,
		poolName:    prefix,
		class:       class,
		configName:  configName,
		v1:          v1,
		v2:          v2,
		tmpl:        tmpl,
		templateRef: e2e.TemplateRef(tmpl),
		actorName:   actorName,
		actorRef:    actorRef,
		workerRef:   workerRef,
		v1Runtime:   v1Runtime,
		snap1:       snap1,
	}
}

// newestClassDefault returns the cluster's installed class-default
// SandboxConfig for class.
func newestClassDefault(t *testing.T, ctx context.Context, clients *e2e.Clients, class atev1.SandboxClass) *atev1.SandboxConfig {
	t.Helper()
	list, err := clients.SubstrateK8s.AteV1().SandboxConfigs().List(ctx, metav1.ListOptions{})
	if err != nil {
		t.Fatalf("failed to list SandboxConfigs: %v", err)
	}
	var best *atev1.SandboxConfig
	for i := range list.Items {
		sc := &list.Items[i]
		if sc.Spec.SandboxClass != class || sc.Annotations[atev1.SandboxConfigClassDefaultAnnotation] != "true" || !sc.DeletionTimestamp.IsZero() {
			continue
		}
		if best == nil || best.CreationTimestamp.Before(&sc.CreationTimestamp) ||
			(best.CreationTimestamp.Equal(&sc.CreationTimestamp) && sc.Name < best.Name) {
			best = sc
		}
	}
	if best == nil {
		t.Fatalf("no SandboxConfig of class %q is annotated %s=true; the install ships one, see manifests/ate-install", class, atev1.SandboxConfigClassDefaultAnnotation)
	}
	return best
}

// defaultVersionFromConfig extracts sc's default version and returns a copy
// renamed to name with state Enabled.
func defaultVersionFromConfig(t *testing.T, sc *atev1.SandboxConfig, name string) atev1.SandboxVersionConfig {
	t.Helper()
	for i := range sc.Spec.Versions {
		if sc.Spec.Versions[i].Name == sc.Spec.DefaultVersion {
			v := *sc.Spec.Versions[i].DeepCopy()
			v.Name = name
			enabled := atev1.SandboxVersionStateEnabled
			v.State = &enabled
			return v
		}
	}
	t.Fatalf("SandboxConfig %s has no version matching defaultVersion %q", sc.Name, sc.Spec.DefaultVersion)
	return atev1.SandboxVersionConfig{}
}

// secondSandboxVersion returns a second Enabled version for class whose assets
// produce a different sandbox_bundle_hash than v1. On gVisor it uses a second
// gVisor release tarball; on microVM it reuses v1's cached assets plus an
// additional entry so no second kata asset set is needed in the bucket.
func secondSandboxVersion(v1 atev1.SandboxVersionConfig, name string, class atev1.SandboxClass) atev1.SandboxVersionConfig {
	v2 := *v1.DeepCopy()
	v2.Name = name
	enabled := atev1.SandboxVersionStateEnabled
	v2.State = &enabled
	if class == atev1.SandboxClassGvisor {
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
func updateSandboxConfigVersions(t *testing.T, ctx context.Context, clients *e2e.Clients, configName, defaultVersion string, versions []atev1.SandboxVersionConfig) {
	t.Helper()
	for {
		sc, err := clients.SubstrateK8s.AteV1().SandboxConfigs().Get(ctx, configName, metav1.GetOptions{})
		if err != nil {
			t.Fatalf("failed to get SandboxConfig %s: %v", configName, err)
		}
		sc.Spec.DefaultVersion = defaultVersion
		sc.Spec.Versions = versions
		_, err = clients.SubstrateK8s.AteV1().SandboxConfigs().Update(ctx, sc, metav1.UpdateOptions{})
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
		wp, err := clients.SubstrateK8s.AteV1().WorkerPools(namespace).Get(ctx, poolName, metav1.GetOptions{})
		if err != nil {
			t.Fatalf("failed to get WorkerPool %s/%s: %v", namespace, poolName, err)
		}
		for i := range wp.Spec.SandboxClasses {
			wp.Spec.SandboxClasses[i].ConfigRef = &atev1.SandboxConfigReference{Name: configName}
		}
		_, err = clients.SubstrateK8s.AteV1().WorkerPools(namespace).Update(ctx, wp, metav1.UpdateOptions{})
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
func waitForWorkerRestorableUpgrade(t *testing.T, ctx context.Context, clients *e2e.Clients, workerRef *ateapi.ObjectRef, v1Runtime *ateapi.ActorSandboxRuntime) {
	t.Helper()
	const timeout = 2 * time.Minute
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	v1Hash := runtimeAttribute(v1Runtime, hardware.AttrSandboxBundleHash)
	var lastClasses []*ateapi.WorkerSandboxClass
	for {
		w, err := clients.SubstrateAPI.GetWorker(ctx, &ateapi.GetWorkerRequest{Worker: workerRef})
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
func waitForWorkerRuntimeDisabled(t *testing.T, ctx context.Context, clients *e2e.Clients, workerRef *ateapi.ObjectRef, v1Runtime *ateapi.ActorSandboxRuntime) *ateapi.ActorSandboxRuntime {
	t.Helper()
	const timeout = 2 * time.Minute
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	v1Hash := runtimeAttribute(v1Runtime, hardware.AttrSandboxBundleHash)
	var lastClasses []*ateapi.WorkerSandboxClass
	for {
		w, err := clients.SubstrateAPI.GetWorker(ctx, &ateapi.GetWorkerRequest{Worker: workerRef})
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

// waitForPoolWorkerActive polls ListWorkers until a worker belonging to
// namespace/poolName is WORKER_STATE_ACTIVE and advertises a default_runtime
// whose bundle hash differs from v1Runtime.
func waitForPoolWorkerActive(t *testing.T, ctx context.Context, clients *e2e.Clients, namespace, poolName string, v1Runtime *ateapi.ActorSandboxRuntime) *ateapi.ActorSandboxRuntime {
	t.Helper()
	const timeout = 2 * time.Minute
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	v1Hash := runtimeAttribute(v1Runtime, hardware.AttrSandboxBundleHash)
	var lastWorker *ateapi.Worker
	for {
		resp, err := clients.SubstrateAPI.ListWorkers(ctx, &ateapi.ListWorkersRequest{})
		if err == nil {
			for _, w := range resp.GetWorkers() {
				if w.GetWorkerNamespace() != namespace || w.GetWorkerPool() != poolName {
					continue
				}
				lastWorker = w
				status := w.GetStatus()
				if status.GetState() != ateapi.WorkerState_WORKER_STATE_ACTIVE {
					continue
				}
				defRuntime := hardware.ColdBootRuntime(status)
				defHash := runtimeAttribute(defRuntime, hardware.AttrSandboxBundleHash)
				if defHash != "" && defHash != v1Hash && !hardware.WorkerMatches(status, v1Runtime) {
					t.Logf("Pool %s/%s Worker %s active on v2: default=%s", namespace, poolName, w.GetMetadata().GetName(), defHash)
					return defRuntime
				}
			}
		}
		select {
		case <-ctx.Done():
			raw, _ := json.Marshal(lastWorker)
			t.Fatalf("timed out after %v waiting for pool %s/%s to have an ACTIVE worker on v2 (last worker=%s, err=%v)",
				timeout, namespace, poolName, raw, err)
		case <-time.After(500 * time.Millisecond):
		}
	}
}

// waitForAteletSyncFailure polls the atelet pod logs on workerRef's node until
// atelet logs a worker sandbox runtimes sync failure for workerRef's pod UID
// and configName.
func waitForAteletSyncFailure(t *testing.T, ctx context.Context, clients *e2e.Clients, workerRef *ateapi.ObjectRef, configName string) {
	t.Helper()
	w, err := clients.SubstrateAPI.GetWorker(ctx, &ateapi.GetWorkerRequest{Worker: workerRef})
	if err != nil {
		t.Fatalf("failed to get Worker %s: %v", workerRef.GetName(), err)
	}
	podUID := w.GetWorkerPodUid()
	nodeName := w.GetNodeName()

	pods, err := clients.K8s.CoreV1().Pods("ate-system").List(ctx, metav1.ListOptions{
		LabelSelector: "app=atelet",
		FieldSelector: "spec.nodeName=" + nodeName,
	})
	if err != nil || len(pods.Items) == 0 {
		t.Fatalf("failed to find atelet pod on node %s (pods=%d, err=%v)", nodeName, len(pods.Items), err)
	}
	ateletPod := pods.Items[0].Name

	const timeout = 2 * time.Minute
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	tailLines := int64(200)
	for {
		raw, err := clients.K8s.CoreV1().Pods("ate-system").GetLogs(ateletPod, &corev1.PodLogOptions{
			Container: "atelet",
			TailLines: &tailLines,
		}).DoRaw(ctx)
		if err == nil {
			for line := range strings.SplitSeq(string(raw), "\n") {
				if strings.Contains(line, podUID) && strings.Contains(line, configName) && strings.Contains(line, "Could not sync worker sandbox runtimes") {
					t.Logf("Observed atelet sync failure for pod %s (%s): %s", podUID, configName, line)
					return
				}
			}
		}
		select {
		case <-ctx.Done():
			t.Fatalf("timed out after %v waiting for atelet %s on node %s to log sync failure for pod %s and SandboxConfig %s (err=%v)",
				timeout, ateletPod, nodeName, podUID, configName, err)
		case <-time.After(500 * time.Millisecond):
		}
	}
}

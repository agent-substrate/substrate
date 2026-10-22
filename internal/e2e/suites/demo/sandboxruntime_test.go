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

	configName := "e2e-runtime-" + nsObj.Name
	poolName := "runtime-upgrade"
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
		_ = clients.SubstrateK8s.ApiV1alpha1().WorkerPools(nsObj.Name).Delete(cleanupCtx, poolName, metav1.DeleteOptions{})
		if err := clients.SubstrateK8s.ApiV1alpha1().SandboxConfigs().Delete(cleanupCtx, configName, metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
			t.Errorf("failed to delete SandboxConfig %s: %v", configName, err)
		}
	})

	tmplName := "runtime-upgrade-" + nsObj.Name
	tmpl := e2e.CreateSubstrateCounterTemplate(ctx, t, clients, nsObj.Name, e2e.SubstrateTemplateOptions{
		Atespace: demoAtespace,
		Name:     tmplName,
		PoolName: poolName,
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

	actorName := "runtime-upgrade-" + nsObj.Name
	actorRef := &ateapipb.ObjectRef{Atespace: demoAtespace, Name: actorName}
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		_, _ = clients.SubstrateAPI.SuspendActor(cleanupCtx, &ateapipb.SuspendActorRequest{Actor: actorRef})
		_, _ = clients.SubstrateAPI.DeleteActor(cleanupCtx, &ateapipb.DeleteActorRequest{Actor: actorRef})
	})

	// 1. Create and resume the actor on v1, call it twice, and suspend.
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
	v1Hash := runtimeAttribute(v1Runtime, hardware.AttrSandboxBundleHash)
	if v1Hash == "" {
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

	// 2. Add v2 (Enabled) and flip defaultVersion from v1 to v2 while v1 stays Enabled.
	updateSandboxConfigVersions(t, ctx, clients, configName, v2.Name, []atev1alpha1.SandboxVersionConfig{v1, v2})
	waitForWorkerRestorableUpgrade(t, ctx, clients, workerRef, v1Runtime)

	// 3. Resume the actor: it matches v1 via restorable_runtimes and restores MEMORY.
	resumed2, err := e2e.ResumeActorAwaitCapacity(t, ctx, clients, &ateapipb.ResumeActorRequest{Actor: actorRef})
	if err != nil {
		t.Fatalf("failed to resume Actor via restorable runtime: %v", err)
	}
	waitForActorState(ctx, t, clients, actorName, ateapipb.ActorState_ACTOR_STATE_RUNNING)

	assignment2 := resumed2.GetActor().GetStatus().GetWorkerAssignment()
	if diff := cmp.Diff(v1Runtime, assignment2.GetSandboxRuntime(), protocmp.Transform()); diff != "" {
		t.Fatalf("workerAssignment.sandbox_runtime after restorable resume (-want v1 +got):\n%s", diff)
	}

	resp, err := callActor(t, resources.ActorRef{Atespace: demoAtespace, Name: actorName})
	if err != nil {
		t.Fatalf("failed to call actor after restorable resume: %v", err)
	}
	validateCounterResponse(t, resp, "after restorable resume", 3, 3)
	validateRootfsCounter(t, resp, "after restorable resume", 3)

	// 4. Suspend again and verify the new snapshot is still stamped with v1.
	suspended2, err := clients.SubstrateAPI.SuspendActor(ctx, &ateapipb.SuspendActorRequest{
		Actor:   actorRef,
		Options: &ateapipb.SuspendOptions{Mode: ateapipb.DurabilityMode_DURABILITY_MODE_SYNCHRONOUS},
	})
	if err != nil {
		t.Fatalf("failed to suspend Actor a second time: %v", err)
	}
	waitForActorState(ctx, t, clients, actorName, ateapipb.ActorState_ACTOR_STATE_SUSPENDED)

	snap2 := durableSnapshot(suspended2.GetActor().GetStatus())
	if snap2.GetGeneration() <= snap1.GetGeneration() {
		t.Fatalf("second snapshot generation = %d, want > %d", snap2.GetGeneration(), snap1.GetGeneration())
	}
	if diff := cmp.Diff(v1Runtime, snap2.GetSandboxRuntime(), protocmp.Transform()); diff != "" {
		t.Fatalf("second snapshot sandbox_runtime mismatch (-want v1 +got):\n%s", diff)
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
	sc, err := clients.SubstrateK8s.ApiV1alpha1().SandboxConfigs().Get(ctx, configName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("failed to get SandboxConfig %s: %v", configName, err)
	}
	sc.Spec.DefaultVersion = defaultVersion
	sc.Spec.Versions = versions
	if _, err := clients.SubstrateK8s.ApiV1alpha1().SandboxConfigs().Update(ctx, sc, metav1.UpdateOptions{}); err != nil {
		t.Fatalf("failed to update SandboxConfig %s: %v", configName, err)
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

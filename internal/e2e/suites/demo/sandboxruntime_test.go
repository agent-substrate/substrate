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
	"go.ate.dev/substrate/api/ateapi/v1"
	atev1 "go.ate.dev/substrate/api/infra/k8s/ate/v1"
	"go.ate.dev/substrate/internal/e2e"
	"go.ate.dev/substrate/internal/hardware"
	"go.ate.dev/substrate/internal/resources"
	"google.golang.org/protobuf/testing/protocmp"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/util/retry"
)

// gvisorSecondReleaseAssets is a gVisor build with a sandbox_bundle_hash
// other than the installed class default's: the first nightly after release
// 20261026.0, which manifests/ate-install/sandboxconfig-gvisor.yaml ships, so
// it has everything ateworker-gvisor needs from that release.
var gvisorSecondReleaseAssets = map[string]map[string]atev1.AssetFile{
	"amd64": {
		"gvisor": {
			URL:    "gs://gvisor/releases/nightly/2026-10-27/x86_64/gvisor.tar.zstd",
			SHA256: "6cf41969e53892d8036d9a6470cbb2e870fbe719563fa15dbf9247bdd7911ba5",
		},
	},
	"arm64": {
		"gvisor": {
			URL:    "gs://gvisor/releases/nightly/2026-10-27/aarch64/gvisor.tar.zstd",
			SHA256: "8ffb0077d6db3d837a3790d1a43d2354ae79a216e6707452429bb9e52e2d545b",
		},
	},
}

// sandboxRuntimeFixture is a suspended actor on a pool whose SandboxConfig
// the test owns, so the test can edit versions and watch the worker
// re-register and the actor resume.
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
	// memory, file, and rootfs are the counters the actor last reported, and
	// generation that of its latest snapshot. resume, call, and suspend
	// advance them.
	memory, file, rootfs int
	generation           int32
	// v1Runtime is the runtime the actor first ran under, and v1Hash its
	// sandbox_bundle_hash.
	v1Runtime *ateapi.ActorSandboxRuntime
	v1Hash    string
}

// setupSuspendedV1Actor creates a SandboxConfig holding one version, v1,
// copied from the cluster's class default, a one-worker pool pinned to it, a
// counter ActorTemplate without a golden snapshot, and an actor. It runs the
// actor on v1 for two calls
// (memory=2, file=2, rootfs=2), suspends it synchronously, and checks the
// MEMORY snapshot is stamped with v1. f.v2 is a second version with a
// different sandbox_bundle_hash that the config does not hold yet.
func setupSuspendedV1Actor(t *testing.T, prefix string) *sandboxRuntimeFixture {
	t.Helper()
	nsObj := e2e.CreateNamespace(t)
	ctx := context.Background()
	clients := e2e.GetClients()

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
			StorageLocation:      e2e.SnapshotLocation(t, "ate-demo-"+tmplName),
			PreferredFidelity:    ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY,
			GoldenSnapshotConfig: &ateapi.GoldenSnapshotConfig{Mode: ateapi.GoldenSnapshotMode_GOLDEN_SNAPSHOT_MODE_DISABLED},
		},
	})

	actorName := prefix + "-" + nsObj.Name
	actorRef := &ateapi.ObjectRef{Atespace: demoAtespace, Name: actorName}
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		_, _ = e2e.RetryAborted(t, cleanupCtx, clients.SubstrateAPI.DeleteActor, &ateapi.DeleteActorRequest{Actor: actorRef})
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

	f := &sandboxRuntimeFixture{
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
		v1Hash:      v1Hash,
		memory:      2,
		file:        2,
		rootfs:      2,
	}
	f.suspend(t, v1Runtime)
	return f
}

// updateVersions sets the fixture's SandboxConfig defaultVersion and versions.
func (f *sandboxRuntimeFixture) updateVersions(t *testing.T, defaultVersion string, versions []atev1.SandboxVersionConfig) {
	t.Helper()
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		sc, err := f.clients.SubstrateK8s.AteV1().SandboxConfigs().Get(f.ctx, f.configName, metav1.GetOptions{})
		if err != nil {
			return err
		}
		sc.Spec.DefaultVersion = defaultVersion
		sc.Spec.Versions = versions
		_, err = f.clients.SubstrateK8s.AteV1().SandboxConfigs().Update(f.ctx, sc, metav1.UpdateOptions{})
		return err
	})
	if err != nil {
		t.Fatalf("failed to update SandboxConfig %s: %v", f.configName, err)
	}
}

// waitForWorker polls the fixture's worker until pred accepts its status, and
// returns that status.
func (f *sandboxRuntimeFixture) waitForWorker(t *testing.T, pred func(*ateapi.WorkerStatus) bool) *ateapi.WorkerStatus {
	t.Helper()
	const timeout = 2 * time.Minute
	ctx, cancel := context.WithTimeout(f.ctx, timeout)
	defer cancel()

	var last *ateapi.WorkerStatus
	for {
		w, err := f.clients.SubstrateAPI.GetWorker(ctx, &ateapi.GetWorkerRequest{Worker: f.workerRef})
		if err == nil {
			last = w.GetStatus()
			if pred(last) {
				return last
			}
		}
		select {
		case <-ctx.Done():
			raw, _ := json.Marshal(last.GetSandboxClasses())
			t.Fatalf("timed out after %v waiting for Worker %s to re-register (last sandbox_classes=%s, err=%v)",
				timeout, f.workerRef.GetName(), raw, err)
		case <-time.After(500 * time.Millisecond):
		}
	}
}

// resumeAndCheck resumes the fixture's actor and checks that the assignment
// records wantRuntime and that one call advances the counters as a restore
// at want does. It then suspends the actor again and checks the new snapshot
// is stamped with wantRuntime. It returns the status the actor resumed with.
// See resume and suspend.
func (f *sandboxRuntimeFixture) resumeAndCheck(t *testing.T, want ateapi.SnapshotFidelity, wantRuntime *ateapi.ActorSandboxRuntime) *ateapi.ActorStatus {
	t.Helper()
	status := f.resume(t, want, wantRuntime)
	f.suspend(t, wantRuntime)
	return status
}

// resume resumes the fixture's actor, checks that the assignment records
// wantRuntime, and calls the actor once: the volume counter survives every
// restore, the rootfs counter all but VOLUMES, and the memory counter only
// MEMORY. It returns the status the actor resumed with.
func (f *sandboxRuntimeFixture) resume(t *testing.T, want ateapi.SnapshotFidelity, wantRuntime *ateapi.ActorSandboxRuntime) *ateapi.ActorStatus {
	t.Helper()
	stage := "after " + want.String() + " resume"
	resumed, err := e2e.ResumeActorAwaitCapacity(t, f.ctx, f.clients, &ateapi.ResumeActorRequest{Actor: f.actorRef})
	if err != nil {
		t.Fatalf("failed to resume Actor %s: %v", f.actorName, err)
	}
	waitForActorState(f.ctx, t, f.clients, f.actorName, ateapi.ActorState_ACTOR_STATE_RUNNING)
	status := resumed.GetActor().GetStatus()
	if diff := cmp.Diff(wantRuntime, status.GetWorkerAssignment().GetSandboxRuntime(), protocmp.Transform()); diff != "" {
		t.Fatalf("workerAssignment.sandbox_runtime of %s %s (-want +got):\n%s", f.actorName, stage, diff)
	}
	switch want {
	case ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_ROOTFS:
		f.memory = 0
	case ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_VOLUMES:
		f.memory, f.rootfs = 0, 0
	}
	f.call(t, stage)
	return status
}

// call calls the running actor once and checks every counter advanced.
func (f *sandboxRuntimeFixture) call(t *testing.T, stage string) {
	t.Helper()
	f.memory++
	f.file++
	f.rootfs++
	resp, err := callActor(t, resources.ActorRef{Atespace: demoAtespace, Name: f.actorName})
	if err != nil {
		t.Fatalf("failed to call actor %s %s: %v", f.actorName, stage, err)
	}
	validateCounterResponse(t, resp, stage, f.memory, f.file)
	validateRootfsCounter(t, resp, stage, f.rootfs)
}

// suspend suspends the fixture's actor synchronously and checks the new
// MEMORY snapshot is stamped with wantRuntime.
func (f *sandboxRuntimeFixture) suspend(t *testing.T, wantRuntime *ateapi.ActorSandboxRuntime) {
	t.Helper()
	suspended, err := f.clients.SubstrateAPI.SuspendActor(f.ctx, &ateapi.SuspendActorRequest{
		Actor:   f.actorRef,
		Options: &ateapi.SuspendOptions{Mode: ateapi.DurabilityMode_DURABILITY_MODE_SYNCHRONOUS},
	})
	if err != nil {
		t.Fatalf("failed to suspend Actor %s: %v", f.actorName, err)
	}
	waitForActorState(f.ctx, t, f.clients, f.actorName, ateapi.ActorState_ACTOR_STATE_SUSPENDED)
	snap := durableSnapshot(suspended.GetActor().GetStatus())
	if snap.GetGeneration() <= f.generation {
		t.Fatalf("snapshot generation of %s = %d, want > %d", f.actorName, snap.GetGeneration(), f.generation)
	}
	if got := snap.GetFidelity(); got != ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY {
		t.Fatalf("snapshot fidelity of %s = %v, want MEMORY", f.actorName, got)
	}
	if diff := cmp.Diff(wantRuntime, snap.GetSandboxRuntime(), protocmp.Transform()); diff != "" {
		t.Fatalf("snapshot sandbox_runtime of %s (-want +got):\n%s", f.actorName, diff)
	}
	f.generation = snap.GetGeneration()
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

// defaultVersionFromConfig returns a copy of sc's default version, named name
// and Enabled.
func defaultVersionFromConfig(t *testing.T, sc *atev1.SandboxConfig, name string) atev1.SandboxVersionConfig {
	t.Helper()
	for i := range sc.Spec.Versions {
		if sc.Spec.Versions[i].Name == sc.Spec.DefaultVersion {
			return withState(withName(sc.Spec.Versions[i], name), atev1.SandboxVersionStateEnabled)
		}
	}
	t.Fatalf("SandboxConfig %s has no version matching defaultVersion %q", sc.Name, sc.Spec.DefaultVersion)
	return atev1.SandboxVersionConfig{}
}

// secondSandboxVersion returns an Enabled version named name whose assets
// produce a different sandbox_bundle_hash than v1. On gVisor it uses a second
// gVisor release tarball; on microVM it reuses v1's cached assets plus an
// extra entry, so the bucket needs no second kata asset set.
func secondSandboxVersion(v1 atev1.SandboxVersionConfig, name string, class atev1.SandboxClass) atev1.SandboxVersionConfig {
	if class == atev1.SandboxClassGvisor {
		// withName copies the assets, so no test shares the package-level map.
		v1.Assets = gvisorSecondReleaseAssets
	}
	v2 := withState(withName(v1, name), atev1.SandboxVersionStateEnabled)
	if class != atev1.SandboxClassGvisor {
		for arch, files := range v2.Assets {
			files["extra"] = files["virtiofsd"]
			v2.Assets[arch] = files
		}
	}
	return v2
}

// withName returns a copy of v named name.
func withName(v atev1.SandboxVersionConfig, name string) atev1.SandboxVersionConfig {
	v = *v.DeepCopy()
	v.Name = name
	return v
}

// withState returns a copy of v in state.
func withState(v atev1.SandboxVersionConfig, state atev1.SandboxVersionState) atev1.SandboxVersionConfig {
	v = *v.DeepCopy()
	v.State = &state
	return v
}

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

package demo

// What atecontroller does with a WorkerPool's configRef: which changes it
// refuses without disturbing the pool, and what a switch to another
// SandboxConfig does to a running actor. With a second pool on the new
// config, also which pool a resume picks.

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
	"go.ate.dev/substrate/internal/hardware"
	"google.golang.org/protobuf/testing/protocmp"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/util/retry"
)

// TestWorkerPoolSwitchSandboxConfig covers a WorkerPool's configRef: the
// changes atecontroller refuses while keeping the pool on its SandboxConfig,
// and a switch to another SandboxConfig of the same class while an actor
// runs. A second pool on that config is up before the switch, so the resume
// in step 3 also shows a worker that cannot restore the snapshot's runtime
// is filtered out while one that can has room.
//  1. An actor boots on poolA (configA at v1), increments its counters twice
//     (memory=2, file=2, rootfs=2), and suspends with a v1-stamped snapshot.
//     poolB (configB at v2) starts in the same namespace with poolA's labels.
//  2. Deleting configA while poolA uses it leaves it Terminating under
//     the workerpool-protection finalizer with Resolved=False
//     (SandboxConfigDeleting). Pointing configRef at a missing SandboxConfig
//     or one of the other sandbox class gives SandboxConfigNotFound and
//     SandboxClassMismatch. All three keep configA in
//     status.sandboxClasses[0].configRef and the worker on v1.
//  3. With both pools ACTIVE and free, the resume picks poolA's worker, the
//     only one advertising v1, and restores MEMORY (memory=3, file=3,
//     rootfs=3). While the actor runs, poolA's configRef switches to configB:
//     configA is released and deleted, the worker re-registers with v2 as
//     default_runtime, and the actor keeps running on v1 (memory=4, file=4,
//     rootfs=4).
//  4. Suspending the actor stamps its snapshot with v1, from
//     WorkerAssignment.sandbox_runtime, not the worker's new default. The
//     next resume drops to ROOTFS on v2 (memory=1, file=5, rootfs=5).
func TestWorkerPoolSwitchSandboxConfig(t *testing.T) {
	t.Parallel()
	f := setupSuspendedV1Actor(t, "config-switch")

	configBName := f.configName + "-b"
	wrongClassName := f.configName + "-wrong-class"
	poolBName := f.poolName + "-b"
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		// The pools must go first: they hold the protection finalizer on configB.
		for _, name := range []string{f.poolName, poolBName} {
			_ = f.clients.SubstrateK8s.AteV1().WorkerPools(f.namespace).Delete(cleanupCtx, name, metav1.DeleteOptions{})
		}
		for _, name := range []string{configBName, wrongClassName} {
			if err := f.clients.SubstrateK8s.AteV1().SandboxConfigs().Delete(cleanupCtx, name, metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
				t.Errorf("failed to delete SandboxConfig %s: %v", name, err)
			}
		}
	})
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
	if _, err := f.clients.SubstrateK8s.AteV1().SandboxConfigs().Create(f.ctx, wrongClassSandboxConfig(wrongClassName, f.class), metav1.CreateOptions{}); err != nil {
		t.Fatalf("failed to create wrong-class SandboxConfig %s: %v", wrongClassName, err)
	}
	// poolB copies poolA apart from its configRef.
	poolA, err := f.clients.SubstrateK8s.AteV1().WorkerPools(f.namespace).Get(f.ctx, f.poolName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("failed to get WorkerPool %s/%s: %v", f.namespace, f.poolName, err)
	}
	sandboxClassesB := slices.Clone(poolA.Spec.SandboxClasses)
	for i := range sandboxClassesB {
		sandboxClassesB[i].ConfigRef = &atev1.SandboxConfigReference{Name: configBName}
	}
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

	// 2. Delete configA while the pool uses it.
	if err := f.clients.SubstrateK8s.AteV1().SandboxConfigs().Delete(f.ctx, f.configName, metav1.DeleteOptions{}); err != nil {
		t.Fatalf("failed to delete SandboxConfig %s while in use: %v", f.configName, err)
	}
	f.waitForPoolCondition(t, f.configName, metav1.ConditionFalse, atev1.WorkerPoolReasonSandboxConfigDeleting)
	terminating, err := f.clients.SubstrateK8s.AteV1().SandboxConfigs().Get(f.ctx, f.configName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("SandboxConfig %s must stay while the pool uses it: %v", f.configName, err)
	}
	if terminating.DeletionTimestamp.IsZero() || !slices.Contains(terminating.Finalizers, atev1.SandboxConfigWorkerPoolProtectionFinalizer) {
		t.Fatalf("SandboxConfig %s in use: deletionTimestamp=%v, finalizers=%v; want Terminating under %s",
			f.configName, terminating.DeletionTimestamp, terminating.Finalizers, atev1.SandboxConfigWorkerPoolProtectionFinalizer)
	}

	// Then point configRef at a missing SandboxConfig and at one of the other
	// class.
	f.setPoolConfig(t, "nonexistent-"+f.namespace)
	f.waitForPoolCondition(t, f.configName, metav1.ConditionFalse, atev1.WorkerPoolReasonSandboxConfigNotFound)
	f.setPoolConfig(t, wrongClassName)
	f.waitForPoolCondition(t, f.configName, metav1.ConditionFalse, atev1.WorkerPoolReasonSandboxClassMismatch)

	worker, err := f.clients.SubstrateAPI.GetWorker(f.ctx, &ateapi.GetWorkerRequest{Worker: f.workerRef})
	if err != nil {
		t.Fatalf("failed to get Worker %s after the refused configRef changes: %v", f.workerRef.GetName(), err)
	}
	if diff := cmp.Diff(f.v1Runtime, hardware.ColdBootRuntime(worker.GetStatus()), protocmp.Transform()); diff != "" {
		t.Fatalf("worker default_runtime changed after the refused configRef changes (-want v1 +got):\n%s", diff)
	}

	// 3. Both pools are free; only poolA's worker can restore v1. Then switch
	// poolA to configB while the actor runs on it.
	f.waitForPoolWorker(t, poolBName)
	assignment := f.resume(t, ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, f.v1Runtime).GetWorkerAssignment()
	if got := assignment.GetWorker().GetName(); got != f.workerRef.GetName() {
		t.Fatalf("assigned worker = %q (pool %q), want poolA's worker %q", got, assignment.GetWorkerPool(), f.workerRef.GetName())
	}
	f.setPoolConfig(t, configBName)
	f.waitForPoolCondition(t, configBName, metav1.ConditionTrue, atev1.WorkerPoolReasonResolved)
	f.waitForConfigDeleted(t, f.configName)
	status := f.waitForWorker(t, func(status *ateapi.WorkerStatus) bool {
		defHash := runtimeAttribute(hardware.ColdBootRuntime(status), hardware.AttrSandboxBundleHash)
		return defHash != "" && defHash != f.v1Hash && !hardware.WorkerMatches(status, f.v1Runtime)
	})
	v2Runtime := hardware.ColdBootRuntime(status)
	f.call(t, "running across the pool switch")

	// 4. The snapshot carries the assignment's v1, and the next resume falls
	// back to ROOTFS on v2, which both pools now advertise.
	f.suspend(t, f.v1Runtime)
	f.resumeAndCheck(t, ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_ROOTFS, v2Runtime)
}

// wrongClassSandboxConfig returns a valid SandboxConfig of the sandbox class
// other than poolClass, which a pool of poolClass refuses with
// SandboxClassMismatch. Its assets are never fetched.
func wrongClassSandboxConfig(name string, poolClass atev1.SandboxClass) *atev1.SandboxConfig {
	class := atev1.SandboxClassGvisor
	assets := gvisorSecondReleaseAssets
	if poolClass == atev1.SandboxClassGvisor {
		class = atev1.SandboxClassMicroVM
		dummy := atev1.AssetFile{URL: "gs://dummy/asset", SHA256: strings.Repeat("a", 64)}
		assets = map[string]map[string]atev1.AssetFile{
			"amd64": {"cloud-hypervisor": dummy, "virtiofsd": dummy, "kata-kernel": dummy, "kata-image": dummy},
		}
	}
	return &atev1.SandboxConfig{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: atev1.SandboxConfigSpec{
			SandboxClass:   class,
			DefaultVersion: "v1",
			Versions: []atev1.SandboxVersionConfig{
				withState(atev1.SandboxVersionConfig{Name: "v1", Assets: assets}, atev1.SandboxVersionStateEnabled),
			},
		},
	}
}

// setPoolConfig points the fixture pool's sandboxClasses[].configRef at
// configName.
func (f *sandboxRuntimeFixture) setPoolConfig(t *testing.T, configName string) {
	t.Helper()
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		wp, err := f.clients.SubstrateK8s.AteV1().WorkerPools(f.namespace).Get(f.ctx, f.poolName, metav1.GetOptions{})
		if err != nil {
			return err
		}
		for i := range wp.Spec.SandboxClasses {
			wp.Spec.SandboxClasses[i].ConfigRef = &atev1.SandboxConfigReference{Name: configName}
		}
		_, err = f.clients.SubstrateK8s.AteV1().WorkerPools(f.namespace).Update(f.ctx, wp, metav1.UpdateOptions{})
		return err
	})
	if err != nil {
		t.Fatalf("failed to update WorkerPool %s/%s: %v", f.namespace, f.poolName, err)
	}
}

// waitForPoolCondition polls the fixture pool until
// status.sandboxClasses[0].configRef names wantConfig and its Resolved
// condition has wantStatus and wantReason.
func (f *sandboxRuntimeFixture) waitForPoolCondition(t *testing.T, wantConfig string, wantStatus metav1.ConditionStatus, wantReason string) {
	t.Helper()
	const timeout = time.Minute
	ctx, cancel := context.WithTimeout(f.ctx, timeout)
	defer cancel()

	var last []atev1.WorkerPoolSandboxClassStatus
	for {
		wp, err := f.clients.SubstrateK8s.AteV1().WorkerPools(f.namespace).Get(ctx, f.poolName, metav1.GetOptions{})
		if err == nil && len(wp.Status.SandboxClasses) > 0 {
			last = wp.Status.SandboxClasses
			sc := wp.Status.SandboxClasses[0]
			gotConfig := ""
			if sc.ConfigRef != nil {
				gotConfig = sc.ConfigRef.Name
			}
			for _, cond := range sc.Conditions {
				if cond.Type == atev1.WorkerPoolSandboxClassConditionResolved && gotConfig == wantConfig && cond.Status == wantStatus && cond.Reason == wantReason {
					return
				}
			}
		}
		select {
		case <-ctx.Done():
			raw, _ := json.Marshal(last)
			t.Fatalf("timed out after %v waiting for WorkerPool %s/%s status.sandboxClasses[0] configRef=%q Resolved=%s/%s (last=%s, err=%v)",
				timeout, f.namespace, f.poolName, wantConfig, wantStatus, wantReason, raw, err)
		case <-time.After(500 * time.Millisecond):
		}
	}
}

// waitForConfigDeleted polls until the SandboxConfig configName is gone.
func (f *sandboxRuntimeFixture) waitForConfigDeleted(t *testing.T, configName string) {
	t.Helper()
	const timeout = time.Minute
	ctx, cancel := context.WithTimeout(f.ctx, timeout)
	defer cancel()

	for {
		sc, err := f.clients.SubstrateK8s.AteV1().SandboxConfigs().Get(ctx, configName, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			return
		}
		select {
		case <-ctx.Done():
			var finalizers []string
			if sc != nil {
				finalizers = sc.Finalizers
			}
			t.Fatalf("timed out after %v waiting for SandboxConfig %s to be deleted (finalizers=%v, err=%v)",
				timeout, configName, finalizers, err)
		case <-time.After(500 * time.Millisecond):
		}
	}
}

// waitForPoolWorker polls ListWorkers until poolName in the fixture's
// namespace has an ACTIVE worker whose default_runtime is not v1.
func (f *sandboxRuntimeFixture) waitForPoolWorker(t *testing.T, poolName string) {
	t.Helper()
	const timeout = 2 * time.Minute
	ctx, cancel := context.WithTimeout(f.ctx, timeout)
	defer cancel()

	var last *ateapi.Worker
	for {
		resp, err := f.clients.SubstrateAPI.ListWorkers(ctx, &ateapi.ListWorkersRequest{})
		if err == nil {
			for _, w := range resp.GetWorkers() {
				if w.GetWorkerNamespace() != f.namespace || w.GetWorkerPool() != poolName {
					continue
				}
				last = w
				status := w.GetStatus()
				defHash := runtimeAttribute(hardware.ColdBootRuntime(status), hardware.AttrSandboxBundleHash)
				if status.GetState() == ateapi.WorkerState_WORKER_STATE_ACTIVE && defHash != "" && defHash != f.v1Hash && !hardware.WorkerMatches(status, f.v1Runtime) {
					return
				}
			}
		}
		select {
		case <-ctx.Done():
			raw, _ := json.Marshal(last)
			t.Fatalf("timed out after %v waiting for pool %s/%s to have an ACTIVE worker off v1 (last worker=%s, err=%v)",
				timeout, f.namespace, poolName, raw, err)
		case <-time.After(500 * time.Millisecond):
		}
	}
}

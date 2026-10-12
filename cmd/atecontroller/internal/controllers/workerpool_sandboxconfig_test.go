// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package controllers

import (
	"cmp"
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	k8errors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/util/retry"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	atev1alpha1 "github.com/agent-substrate/substrate/pkg/api/v1alpha1"
)

// testGvisorDefault is the gvisor class default TestMain creates.
const testGvisorDefault = "gvisor-default-test"

const testPauseImage = "registry.k8s.io/pause:3.10.2@sha256:f548e0e8e3dc1896ca956272154dde3314e8cc4fde0a57577ee9fa1c63f5baf4"

// testHoldFinalizer keeps a SandboxConfig on the API server after it is
// deleted, so a test can observe how a config that is being deleted is treated.
const testHoldFinalizer = "test.ate.dev/hold"

func TestResolveSandboxConfig(t *testing.T) {
	t.Parallel()
	t0 := metav1.NewTime(time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC))
	t1 := metav1.NewTime(t0.Add(time.Minute))
	sc := func(name string, class atev1alpha1.SandboxClass, isDefault bool, created metav1.Time, mutate ...func(*atev1alpha1.SandboxConfig)) atev1alpha1.SandboxConfig {
		c := *makeSandboxConfig(name, class, isDefault)
		c.CreationTimestamp = created
		for _, m := range mutate {
			m(&c)
		}
		return c
	}
	deleting := func(c *atev1alpha1.SandboxConfig) { c.DeletionTimestamp = &t1 }
	gvisor := func(ref string) atev1alpha1.WorkerPoolSandboxClass {
		e := atev1alpha1.WorkerPoolSandboxClass{Name: atev1alpha1.SandboxClassGvisor}
		if ref != "" {
			e.ConfigRef = &atev1alpha1.SandboxConfigReference{Name: ref}
		}
		return e
	}

	gv := atev1alpha1.SandboxClassGvisor

	tests := []struct {
		name    string
		entry   atev1alpha1.WorkerPoolSandboxClass
		inUse   string
		configs []atev1alpha1.SandboxConfig
		// want is the config the class uses, or "" for none.
		want string
		// wantReason is the False condition's reason, or "" for True.
		wantReason  string
		wantMessage string
	}{
		{
			name:        "explicit ref resolves even when another config is the default",
			entry:       gvisor("custom"),
			configs:     []atev1alpha1.SandboxConfig{sc("custom", gv, false, t0), sc("def", gv, true, t1)},
			want:        "custom",
			wantMessage: `resolved to SandboxConfig "custom"`,
		},
		{
			name:        "explicit ref not found",
			entry:       gvisor("missing"),
			configs:     []atev1alpha1.SandboxConfig{sc("def", gv, true, t0)},
			wantReason:  atev1alpha1.WorkerPoolReasonSandboxConfigNotFound,
			wantMessage: `SandboxConfig "missing" not found`,
		},
		{
			name:        "explicit ref not found keeps the config in use",
			entry:       gvisor("missing"),
			inUse:       "old",
			configs:     []atev1alpha1.SandboxConfig{sc("old", gv, false, t0)},
			want:        "old",
			wantReason:  atev1alpha1.WorkerPoolReasonSandboxConfigNotFound,
			wantMessage: `SandboxConfig "missing" not found; keeping "old"`,
		},
		{
			name:        "explicit ref of another class",
			entry:       gvisor("vm"),
			configs:     []atev1alpha1.SandboxConfig{sc("vm", atev1alpha1.SandboxClassMicroVM, false, t0)},
			wantReason:  atev1alpha1.WorkerPoolReasonSandboxClassMismatch,
			wantMessage: `SandboxConfig "vm" is for sandbox class "microvm", not "gvisor"`,
		},
		{
			name:        "explicit ref of another class keeps the config in use",
			entry:       gvisor("vm"),
			inUse:       "old",
			configs:     []atev1alpha1.SandboxConfig{sc("vm", atev1alpha1.SandboxClassMicroVM, false, t0), sc("old", gv, false, t0)},
			want:        "old",
			wantReason:  atev1alpha1.WorkerPoolReasonSandboxClassMismatch,
			wantMessage: `SandboxConfig "vm" is for sandbox class "microvm", not "gvisor"; keeping "old"`,
		},
		{
			name:        "explicit ref being deleted takes no new users",
			entry:       gvisor("dying"),
			configs:     []atev1alpha1.SandboxConfig{sc("dying", gv, false, t0, deleting)},
			wantReason:  atev1alpha1.WorkerPoolReasonSandboxConfigDeleting,
			wantMessage: `SandboxConfig "dying" is being deleted`,
		},
		{
			name:        "explicit ref being deleted is kept but reported by a pool already using it",
			entry:       gvisor("dying"),
			inUse:       "dying",
			configs:     []atev1alpha1.SandboxConfig{sc("dying", gv, false, t0, deleting)},
			want:        "dying",
			wantReason:  atev1alpha1.WorkerPoolReasonSandboxConfigDeleting,
			wantMessage: `SandboxConfig "dying" is being deleted; keeping "dying"`,
		},
		{
			name:        "explicit ref being deleted keeps the previous config in use",
			entry:       gvisor("dying"),
			inUse:       "old",
			configs:     []atev1alpha1.SandboxConfig{sc("dying", gv, false, t0, deleting), sc("old", gv, false, t0)},
			want:        "old",
			wantReason:  atev1alpha1.WorkerPoolReasonSandboxConfigDeleting,
			wantMessage: `SandboxConfig "dying" is being deleted; keeping "old"`,
		},
		{
			name:        "no default",
			entry:       gvisor(""),
			configs:     []atev1alpha1.SandboxConfig{sc("plain", gv, false, t0)},
			wantReason:  atev1alpha1.WorkerPoolReasonDefaultConfigNotFound,
			wantMessage: `no SandboxConfig of sandbox class "gvisor" is annotated sandboxconfig.ate.dev/is-class-default=true`,
		},
		{
			name:        "default of another class is ignored",
			entry:       gvisor(""),
			configs:     []atev1alpha1.SandboxConfig{sc("vm", atev1alpha1.SandboxClassMicroVM, true, t0)},
			wantReason:  atev1alpha1.WorkerPoolReasonDefaultConfigNotFound,
			wantMessage: `no SandboxConfig of sandbox class "gvisor" is annotated sandboxconfig.ate.dev/is-class-default=true`,
		},
		{
			name:  "annotation must be exactly true",
			entry: gvisor(""),
			configs: []atev1alpha1.SandboxConfig{sc("no", gv, false, t0, func(c *atev1alpha1.SandboxConfig) {
				c.Annotations = map[string]string{atev1alpha1.SandboxConfigClassDefaultAnnotation: "True"}
			})},
			wantReason:  atev1alpha1.WorkerPoolReasonDefaultConfigNotFound,
			wantMessage: `no SandboxConfig of sandbox class "gvisor" is annotated sandboxconfig.ate.dev/is-class-default=true`,
		},
		{
			name:        "default being deleted takes no new users",
			entry:       gvisor(""),
			configs:     []atev1alpha1.SandboxConfig{sc("dying", gv, true, t0, deleting)},
			wantReason:  atev1alpha1.WorkerPoolReasonSandboxConfigDeleting,
			wantMessage: `the default SandboxConfig for sandbox class "gvisor" is being deleted`,
		},
		{
			name:        "several defaults being deleted are reported",
			entry:       gvisor(""),
			configs:     []atev1alpha1.SandboxConfig{sc("dying-new", gv, true, t1, deleting), sc("dying-old", gv, true, t0, deleting)},
			wantReason:  atev1alpha1.WorkerPoolReasonSandboxConfigDeleting,
			wantMessage: `the default SandboxConfig for sandbox class "gvisor" is being deleted`,
		},
		{
			name:        "default being deleted is kept but reported by a pool already using it",
			entry:       gvisor(""),
			inUse:       "dying",
			configs:     []atev1alpha1.SandboxConfig{sc("dying", gv, true, t0, deleting)},
			want:        "dying",
			wantReason:  atev1alpha1.WorkerPoolReasonSandboxConfigDeleting,
			wantMessage: `the default SandboxConfig for sandbox class "gvisor" is being deleted; keeping "dying"`,
		},
		{
			name:        "a live default replaces the deleting one in use, even when older",
			entry:       gvisor(""),
			inUse:       "dying",
			configs:     []atev1alpha1.SandboxConfig{sc("dying", gv, true, t1, deleting), sc("old", gv, true, t0)},
			want:        "old",
			wantMessage: `resolved to SandboxConfig "old"`,
		},
		{
			name:        "config in use that lost the default annotation is kept but reported",
			entry:       gvisor(""),
			inUse:       "plain",
			configs:     []atev1alpha1.SandboxConfig{sc("plain", gv, false, t0)},
			want:        "plain",
			wantReason:  atev1alpha1.WorkerPoolReasonDefaultConfigNotFound,
			wantMessage: `no SandboxConfig of sandbox class "gvisor" is annotated sandboxconfig.ate.dev/is-class-default=true; keeping "plain"`,
		},
		{
			name:        "config in use that no longer exists is not kept",
			entry:       gvisor(""),
			inUse:       "gone",
			configs:     []atev1alpha1.SandboxConfig{sc("plain", gv, false, t0)},
			wantReason:  atev1alpha1.WorkerPoolReasonDefaultConfigNotFound,
			wantMessage: `no SandboxConfig of sandbox class "gvisor" is annotated sandboxconfig.ate.dev/is-class-default=true`,
		},
		{
			name:        "config in use of another class is not kept",
			entry:       gvisor(""),
			inUse:       "vm",
			configs:     []atev1alpha1.SandboxConfig{sc("vm", atev1alpha1.SandboxClassMicroVM, true, t0)},
			wantReason:  atev1alpha1.WorkerPoolReasonDefaultConfigNotFound,
			wantMessage: `no SandboxConfig of sandbox class "gvisor" is annotated sandboxconfig.ate.dev/is-class-default=true`,
		},
		{
			name:        "newest default wins",
			entry:       gvisor(""),
			configs:     []atev1alpha1.SandboxConfig{sc("a-old", gv, true, t0), sc("z-new", gv, true, t1), sc("plain", gv, false, t1)},
			want:        "z-new",
			wantMessage: `resolved to SandboxConfig "z-new"`,
		},
		{
			name:        "newest default wins regardless of list order",
			entry:       gvisor(""),
			configs:     []atev1alpha1.SandboxConfig{sc("z-new", gv, true, t1), sc("a-old", gv, true, t0)},
			want:        "z-new",
			wantMessage: `resolved to SandboxConfig "z-new"`,
		},
		{
			name:        "creation time tie goes to the first name",
			entry:       gvisor(""),
			configs:     []atev1alpha1.SandboxConfig{sc("b", gv, true, t0), sc("a", gv, true, t0)},
			want:        "a",
			wantMessage: `resolved to SandboxConfig "a"`,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, cond := resolveSandboxConfig(tc.entry, tc.inUse, tc.configs)
			gotName := ""
			if got != nil {
				gotName = got.Name
			}
			want := metav1.Condition{
				Type:    atev1alpha1.WorkerPoolSandboxClassConditionResolved,
				Status:  metav1.ConditionTrue,
				Reason:  atev1alpha1.WorkerPoolReasonResolved,
				Message: tc.wantMessage,
			}
			if tc.wantReason != "" {
				want.Status = metav1.ConditionFalse
				want.Reason = tc.wantReason
			}
			if gotName != tc.want || cond != want {
				t.Fatalf("resolveSandboxConfig = (%q, %+v), want (%q, %+v)", gotName, cond, tc.want, want)
			}
		})
	}
}

// TestWorkerPoolConfigRef covers a pool with an explicit configRef. It
// resolves only when the named SandboxConfig exists, is of the pool's class,
// and is not being deleted. It then gets its Deployment. Otherwise it reports
// why, records no config in status, and gets no Deployment.
func TestWorkerPoolConfigRef(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		// config is created before the pool. nil creates none.
		config *atev1alpha1.SandboxConfig
		// deleting marks config for deletion before the pool is created.
		deleting bool
		ref      string
		// wantReason is the Resolved=False reason to expect, or
		// "" when the pool must resolve.
		wantReason string
		// late is created once the pool reports wantReason. The pool must
		// then resolve to it.
		late *atev1alpha1.SandboxConfig
	}{
		{
			name:   "existing config of the same class",
			config: makeSandboxConfig("ref-ok", atev1alpha1.SandboxClassGvisor, false),
			ref:    "ref-ok",
		},
		{
			name:       "missing config, created later",
			ref:        "ref-late",
			wantReason: atev1alpha1.WorkerPoolReasonSandboxConfigNotFound,
			late:       makeSandboxConfig("ref-late", atev1alpha1.SandboxClassGvisor, false),
		},
		{
			name:       "config of another class",
			config:     makeSandboxConfig("ref-vm", atev1alpha1.SandboxClassMicroVM, false),
			ref:        "ref-vm",
			wantReason: atev1alpha1.WorkerPoolReasonSandboxClassMismatch,
		},
		{
			name:       "config being deleted",
			config:     makeSandboxConfig("ref-dying", atev1alpha1.SandboxClassGvisor, false),
			deleting:   true,
			ref:        "ref-dying",
			wantReason: atev1alpha1.WorkerPoolReasonSandboxConfigDeleting,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if tc.config != nil {
				if tc.deleting {
					controllerutil.AddFinalizer(tc.config, testHoldFinalizer)
				}
				createSandboxConfig(t, tc.config)
				if tc.deleting {
					t.Cleanup(func() { removeFinalizer(t, tc.config.Name, testHoldFinalizer) })
					if err := k8sClient.Delete(t.Context(), tc.config); err != nil {
						t.Fatalf("delete SandboxConfig: %v", err)
					}
				}
			}
			wp := makeWorkerPool("test-configref-"+tc.ref, "default", 1, "ateom:v1")
			wp.Spec.SandboxClasses[0].ConfigRef = &atev1alpha1.SandboxConfigReference{Name: tc.ref}
			createWorkerPool(t, wp)

			if tc.wantReason == "" {
				waitResolvedAndDeployed(t, wp, tc.ref)
				return
			}
			current := waitUnresolved(t, wp, tc.wantReason)
			if got := statusConfig(current, atev1alpha1.SandboxClassGvisor); got != "" {
				t.Errorf("status.sandboxClasses names %q, want no config for a pool that never resolved", got)
			}
			assertNoDeployment(t, wp)
			if tc.late != nil {
				createSandboxConfig(t, tc.late)
				waitResolvedAndDeployed(t, wp, tc.ref)
			}
		})
	}
}

// waitResolvedAndDeployed waits for wp to resolve to config and for wp's
// Deployment to exist.
func waitResolvedAndDeployed(t *testing.T, wp *atev1alpha1.WorkerPool, config string) {
	t.Helper()
	waitResolved(t, wp, config)
	eventually(t, func(ctx context.Context) (bool, error) {
		_, err := getDeployment(ctx, wp)
		return err == nil, nil
	})
}

// TestWorkerPoolUnresolvedKeepsConfigInUse verifies that a pool whose sandbox
// class stops resolving keeps the config it uses. It reports why, keeps the
// config in status, and the controller keeps updating its Deployment.
func TestWorkerPoolUnresolvedKeepsConfigInUse(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		pool string
		// config is created with testHoldFinalizer and named by the pool's
		// configRef. nil leaves the pool on the class default.
		config *atev1alpha1.SandboxConfig
		// missingRef, if set, breaks resolution by pointing configRef at a
		// config that does not exist, in the same update that scales the
		// pool. Otherwise config is deleted and the pool scaled afterwards.
		missingRef string
		replicas   int32
		wantReason string
	}{
		{
			name:       "spec points at a missing config",
			pool:       "test-unresolved-keeps",
			missingRef: "never-created",
			replicas:   5,
			wantReason: atev1alpha1.WorkerPoolReasonSandboxConfigNotFound,
		},
		{
			name:       "config in use marked for deletion",
			pool:       "test-ref-deleting-in-use",
			config:     makeSandboxConfig("dying-in-use", atev1alpha1.SandboxClassGvisor, false),
			replicas:   3,
			wantReason: atev1alpha1.WorkerPoolReasonSandboxConfigDeleting,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx := t.Context()
			wp := makeWorkerPool(tc.pool, "default", 1, "ateom:v1")
			inUse := testGvisorDefault
			if tc.config != nil {
				controllerutil.AddFinalizer(tc.config, testHoldFinalizer)
				createSandboxConfig(t, tc.config)
				t.Cleanup(func() { removeFinalizer(t, tc.config.Name, testHoldFinalizer) })
				inUse = tc.config.Name
				wp.Spec.SandboxClasses[0].ConfigRef = &atev1alpha1.SandboxConfigReference{Name: inUse}
			}
			createWorkerPool(t, wp)
			waitResolvedAndDeployed(t, wp, inUse)

			if tc.missingRef != "" {
				updateWorkerPoolSpec(t, ctx, wp, "point WorkerPool at a missing SandboxConfig and scale it", func(wp *atev1alpha1.WorkerPool) {
					wp.Spec.Replicas = tc.replicas
					wp.Spec.SandboxClasses[0].ConfigRef = &atev1alpha1.SandboxConfigReference{Name: tc.missingRef}
				})
			} else if err := k8sClient.Delete(ctx, tc.config); err != nil {
				t.Fatalf("delete SandboxConfig: %v", err)
			}
			current := waitUnresolved(t, wp, tc.wantReason)
			if got := statusConfig(current, atev1alpha1.SandboxClassGvisor); got != inUse {
				t.Errorf("status.sandboxClasses names %q, want the config in use %q", got, inUse)
			}

			if tc.missingRef == "" {
				updateWorkerPoolSpec(t, ctx, wp, "scale WorkerPool", func(wp *atev1alpha1.WorkerPool) {
					wp.Spec.Replicas = tc.replicas
				})
			}
			waitDeploymentReplicas(t, wp, tc.replicas)
		})
	}
}

// waitDeploymentReplicas waits for wp's Deployment to ask for replicas pods.
func waitDeploymentReplicas(t *testing.T, wp *atev1alpha1.WorkerPool, replicas int32) {
	t.Helper()
	eventually(t, func(ctx context.Context) (bool, error) {
		dep, err := getDeployment(ctx, wp)
		if err != nil {
			return false, nil
		}
		return dep.Spec.Replicas != nil && *dep.Spec.Replicas == replicas, nil
	})
}

// TestWorkerPoolDefaultNewestWins verifies that a pool without a configRef
// follows the newest class default, and keeps the config it uses, while
// reporting why, once no default is left. Each step changes the class
// defaults and checks the pool's resolution afterwards. It is not parallel
// because it owns the microvm class default.
func TestWorkerPoolDefaultNewestWins(t *testing.T) {
	mvdefA := makeSandboxConfig("mvdef-a", atev1alpha1.SandboxClassMicroVM, true)
	mvdefB := makeSandboxConfig("mvdef-b", atev1alpha1.SandboxClassMicroVM, true)
	controllerutil.AddFinalizer(mvdefB, testHoldFinalizer)
	// The steps create the configs, but they outlive each step.
	deleteOnCleanup(t, mvdefA)
	deleteOnCleanup(t, mvdefB)
	t.Cleanup(func() { removeFinalizer(t, mvdefB.Name, testHoldFinalizer) })
	create := func(t *testing.T, sc *atev1alpha1.SandboxConfig) {
		t.Helper()
		if err := k8sClient.Create(t.Context(), sc); err != nil {
			t.Fatalf("create SandboxConfig %s: %v", sc.Name, err)
		}
	}
	wp := makeWorkerPool("test-default-newest", "default", 1, "ateom:v1")
	wp.Spec.SandboxClasses[0].Name = atev1alpha1.SandboxClassMicroVM
	createWorkerPool(t, wp)

	steps := []struct {
		name string
		// act runs before the checks. nil checks the pool as created.
		act func(t *testing.T)
		// wantConfig is the config the pool's status names, or "" for none.
		wantConfig string
		// wantReason is the Resolved=False reason to expect, or "" when the
		// pool must resolve to wantConfig and get its Deployment.
		wantReason string
	}{
		{
			name:       "no class default",
			wantReason: atev1alpha1.WorkerPoolReasonDefaultConfigNotFound,
		},
		{
			name:       "a class default appears",
			act:        func(t *testing.T) { create(t, mvdefA) },
			wantConfig: "mvdef-a",
		},
		{
			name: "a newer class default wins",
			act: func(t *testing.T) {
				// creationTimestamp has one-second resolution, and the name
				// tie-break would pick mvdef-a, so only a later creation time
				// can make mvdef-b win.
				time.Sleep(1100 * time.Millisecond)
				create(t, mvdefB)
			},
			wantConfig: "mvdef-b",
		},
		{
			name:       "the default no longer used deletes at once",
			act:        func(t *testing.T) { deleteAndWait(t, mvdefA) },
			wantConfig: "mvdef-b",
		},
		{
			name: "the only default being deleted is kept",
			act: func(t *testing.T) {
				if err := k8sClient.Delete(t.Context(), mvdefB); err != nil {
					t.Fatalf("delete SandboxConfig mvdef-b: %v", err)
				}
			},
			wantConfig: "mvdef-b",
			wantReason: atev1alpha1.WorkerPoolReasonSandboxConfigDeleting,
		},
		{
			name: "no default left: the config in use is kept",
			act: func(t *testing.T) {
				err := retry.RetryOnConflict(retry.DefaultBackoff, func() error {
					sc := &atev1alpha1.SandboxConfig{}
					if err := k8sClient.Get(t.Context(), types.NamespacedName{Name: "mvdef-b"}, sc); err != nil {
						return err
					}
					delete(sc.Annotations, atev1alpha1.SandboxConfigClassDefaultAnnotation)
					return k8sClient.Update(t.Context(), sc)
				})
				if err != nil {
					t.Fatalf("remove class default annotation from mvdef-b: %v", err)
				}
			},
			wantConfig: "mvdef-b",
			wantReason: atev1alpha1.WorkerPoolReasonDefaultConfigNotFound,
		},
	}
	for _, step := range steps {
		// Each step builds on the previous one.
		if !t.Run(step.name, func(t *testing.T) {
			if step.act != nil {
				step.act(t)
			}
			if step.wantReason == "" {
				waitResolvedAndDeployed(t, wp, step.wantConfig)
				return
			}
			current := waitUnresolved(t, wp, step.wantReason)
			if got := statusConfig(current, atev1alpha1.SandboxClassMicroVM); got != step.wantConfig {
				t.Errorf("status.sandboxClasses names %q, want %q", got, step.wantConfig)
			}
			if step.wantConfig == "" {
				assertNoDeployment(t, wp)
			}
		}) {
			return
		}
	}
}

// TestWorkerPoolMayUseSandboxConfig covers the three ways a SandboxConfig
// event can concern a WorkerPool.
func TestWorkerPoolMayUseSandboxConfig(t *testing.T) {
	t.Parallel()
	sc := makeSandboxConfig("sc", atev1alpha1.SandboxClassGvisor, false)
	gvisor := atev1alpha1.WorkerPoolSandboxClass{Name: atev1alpha1.SandboxClassGvisor}
	microvm := atev1alpha1.WorkerPoolSandboxClass{Name: atev1alpha1.SandboxClassMicroVM}
	withRef := func(e atev1alpha1.WorkerPoolSandboxClass, name string) atev1alpha1.WorkerPoolSandboxClass {
		e.ConfigRef = &atev1alpha1.SandboxConfigReference{Name: name}
		return e
	}
	using := func(name string) atev1alpha1.WorkerPoolSandboxClassStatus {
		return atev1alpha1.WorkerPoolSandboxClassStatus{Name: atev1alpha1.SandboxClassMicroVM, ConfigRef: &atev1alpha1.SandboxConfigReference{Name: name}}
	}

	tests := []struct {
		name   string
		spec   []atev1alpha1.WorkerPoolSandboxClass
		status []atev1alpha1.WorkerPoolSandboxClassStatus
		want   bool
	}{
		{
			name: "runs the config's class",
			spec: []atev1alpha1.WorkerPoolSandboxClass{gvisor},
			want: true,
		},
		{
			name: "runs the config's class with another ref",
			spec: []atev1alpha1.WorkerPoolSandboxClass{withRef(gvisor, "other")},
			want: true,
		},
		{
			name: "spec names the config",
			spec: []atev1alpha1.WorkerPoolSandboxClass{withRef(microvm, "sc")},
			want: true,
		},
		{
			name:   "status names the config",
			spec:   []atev1alpha1.WorkerPoolSandboxClass{microvm},
			status: []atev1alpha1.WorkerPoolSandboxClassStatus{using("sc")},
			want:   true,
		},
		{
			name:   "another class, another config",
			spec:   []atev1alpha1.WorkerPoolSandboxClass{withRef(microvm, "other")},
			status: []atev1alpha1.WorkerPoolSandboxClassStatus{using("other")},
			want:   false,
		},
		{
			name:   "another class, no config in use",
			spec:   []atev1alpha1.WorkerPoolSandboxClass{withRef(microvm, "other")},
			status: []atev1alpha1.WorkerPoolSandboxClassStatus{{Name: atev1alpha1.SandboxClassMicroVM}},
			want:   false,
		},
		{
			name: "no sandbox classes",
			want: false,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			wp := makeWorkerPoolClasses("default", "pool", tc.spec, tc.status)
			if got := workerPoolMayUseSandboxConfig(wp, sc); got != tc.want {
				t.Errorf("workerPoolMayUseSandboxConfig = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestWorkerPoolsForSandboxConfig verifies the SandboxConfig watch maps an
// event to every pool that may use the config, across namespaces, and to
// nothing for an object of another kind or when listing pools fails.
func TestWorkerPoolsForSandboxConfig(t *testing.T) {
	t.Parallel()
	scheme := runtime.NewScheme()
	if err := atev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add scheme: %v", err)
	}
	gvisor := []atev1alpha1.WorkerPoolSandboxClass{{Name: atev1alpha1.SandboxClassGvisor}}
	microvm := []atev1alpha1.WorkerPoolSandboxClass{{Name: atev1alpha1.SandboxClassMicroVM}}
	microvmRef := []atev1alpha1.WorkerPoolSandboxClass{{Name: atev1alpha1.SandboxClassMicroVM, ConfigRef: &atev1alpha1.SandboxConfigReference{Name: "sc"}}}
	usingSC := []atev1alpha1.WorkerPoolSandboxClassStatus{{Name: atev1alpha1.SandboxClassMicroVM, ConfigRef: &atev1alpha1.SandboxConfigReference{Name: "sc"}}}
	// newReconciler returns a reconciler whose List fails with listErr, if set.
	newReconciler := func(listErr error) *WorkerPoolReconciler {
		return &WorkerPoolReconciler{Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(
			makeWorkerPoolClasses("a", "gv", gvisor, nil),
			makeWorkerPoolClasses("b", "vm", microvm, nil),
			makeWorkerPoolClasses("b", "vm-ref", microvmRef, nil),
			makeWorkerPoolClasses("b", "vm-status", microvm, usingSC),
		).WithInterceptorFuncs(interceptor.Funcs{
			List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
				if listErr != nil {
					return listErr
				}
				return c.List(ctx, list, opts...)
			},
		}).Build()}
	}
	req := func(ns, name string) reconcile.Request {
		return reconcile.Request{NamespacedName: types.NamespacedName{Namespace: ns, Name: name}}
	}

	tests := []struct {
		name string
		obj  client.Object
		// listErr fails the WorkerPool list.
		listErr error
		want    []reconcile.Request
	}{
		{
			name: "gvisor config: its class and the pools naming it",
			obj:  makeSandboxConfig("sc", atev1alpha1.SandboxClassGvisor, false),
			want: []reconcile.Request{req("a", "gv"), req("b", "vm-ref"), req("b", "vm-status")},
		},
		{
			name: "microvm config: every microvm pool",
			obj:  makeSandboxConfig("other", atev1alpha1.SandboxClassMicroVM, false),
			want: []reconcile.Request{req("b", "vm"), req("b", "vm-ref"), req("b", "vm-status")},
		},
		{
			name: "not a SandboxConfig",
			obj:  makeWorkerPoolClasses("a", "gv", gvisor, nil),
		},
		{
			name:    "list fails: nothing",
			obj:     makeSandboxConfig("sc", atev1alpha1.SandboxClassGvisor, false),
			listErr: errors.New("list failed"),
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := newReconciler(tc.listErr).workerPoolsForSandboxConfig(t.Context(), tc.obj)
			slices.SortFunc(got, func(a, b reconcile.Request) int { return cmp.Compare(a.String(), b.String()) })
			if !slices.Equal(got, tc.want) {
				t.Errorf("workerPoolsForSandboxConfig = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestResolveSandboxClasses verifies the status entries built for a pool:
// the config each class uses, a condition stamped with the pool's generation
// whose lastTransitionTime moves only when its status does, and whether
// every class resolved.
func TestResolveSandboxClasses(t *testing.T) {
	t.Parallel()
	const generation = 3
	earlier := metav1.NewTime(time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC))
	configs := []atev1alpha1.SandboxConfig{*makeSandboxConfig("def", atev1alpha1.SandboxClassGvisor, true)}
	ref := func(name string) *atev1alpha1.SandboxConfigReference {
		if name == "" {
			return nil
		}
		return &atev1alpha1.SandboxConfigReference{Name: name}
	}
	cond := func(status metav1.ConditionStatus, reason, message string, gen int64, at metav1.Time) metav1.Condition {
		return metav1.Condition{
			Type:               atev1alpha1.WorkerPoolSandboxClassConditionResolved,
			Status:             status,
			Reason:             reason,
			Message:            message,
			ObservedGeneration: gen,
			LastTransitionTime: at,
		}
	}
	resolvedDef := func(gen int64, at metav1.Time) metav1.Condition {
		return cond(metav1.ConditionTrue, atev1alpha1.WorkerPoolReasonResolved, `resolved to SandboxConfig "def"`, gen, at)
	}
	notFound := func(message string, gen int64, at metav1.Time) metav1.Condition {
		return cond(metav1.ConditionFalse, atev1alpha1.WorkerPoolReasonSandboxConfigNotFound, message, gen, at)
	}
	// now stands for a lastTransitionTime set during the call.
	var now metav1.Time

	tests := []struct {
		name string
		// specRef is the pool's configRef. "" follows the class default.
		specRef string
		prev    []atev1alpha1.WorkerPoolSandboxClassStatus
		want    []atev1alpha1.WorkerPoolSandboxClassStatus
		// wantResolved is the second return value.
		wantResolved bool
	}{
		{
			name:         "first resolve",
			want:         []atev1alpha1.WorkerPoolSandboxClassStatus{{Name: atev1alpha1.SandboxClassGvisor, ConfigRef: ref("def"), Conditions: []metav1.Condition{resolvedDef(generation, now)}}},
			wantResolved: true,
		},
		{
			name:         "still resolved keeps lastTransitionTime",
			prev:         []atev1alpha1.WorkerPoolSandboxClassStatus{{Name: atev1alpha1.SandboxClassGvisor, ConfigRef: ref("def"), Conditions: []metav1.Condition{resolvedDef(generation-1, earlier)}}},
			want:         []atev1alpha1.WorkerPoolSandboxClassStatus{{Name: atev1alpha1.SandboxClassGvisor, ConfigRef: ref("def"), Conditions: []metav1.Condition{resolvedDef(generation, earlier)}}},
			wantResolved: true,
		},
		{
			name:    "never resolved records no config",
			specRef: "missing",
			want:    []atev1alpha1.WorkerPoolSandboxClassStatus{{Name: atev1alpha1.SandboxClassGvisor, Conditions: []metav1.Condition{notFound(`SandboxConfig "missing" not found`, generation, now)}}},
		},
		{
			name:    "stops resolving keeps the config and moves lastTransitionTime",
			specRef: "missing",
			prev:    []atev1alpha1.WorkerPoolSandboxClassStatus{{Name: atev1alpha1.SandboxClassGvisor, ConfigRef: ref("def"), Conditions: []metav1.Condition{resolvedDef(generation-1, earlier)}}},
			want:    []atev1alpha1.WorkerPoolSandboxClassStatus{{Name: atev1alpha1.SandboxClassGvisor, ConfigRef: ref("def"), Conditions: []metav1.Condition{notFound(`SandboxConfig "missing" not found; keeping "def"`, generation, now)}}},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			wp := makeWorkerPool("pool", "default", 1, "ateom:v1")
			wp.Generation = generation
			wp.Spec.SandboxClasses[0].ConfigRef = ref(tc.specRef)
			wp.Status.SandboxClasses = tc.prev
			before := metav1.Now().Rfc3339Copy()
			got, resolved := resolveSandboxClasses(wp, configs)
			if resolved != tc.wantResolved {
				t.Errorf("resolved = %v, want %v", resolved, tc.wantResolved)
			}
			// Replace a lastTransitionTime set during the call with now.
			for i := range got {
				for j := range got[i].Conditions {
					if c := &got[i].Conditions[j]; !c.LastTransitionTime.Before(&before) {
						c.LastTransitionTime = now
					}
				}
			}
			if !equality.Semantic.DeepEqual(got, tc.want) {
				t.Errorf("resolveSandboxClasses = %+v, want %+v", got, tc.want)
			}
		})
	}
}

// TestEveryClassHasConfig covers when a pool without a worker Deployment
// gets one.
func TestEveryClassHasConfig(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		classes []atev1alpha1.WorkerPoolSandboxClassStatus
		want    bool
	}{
		{
			name:    "every class has a config",
			classes: []atev1alpha1.WorkerPoolSandboxClassStatus{{Name: atev1alpha1.SandboxClassGvisor, ConfigRef: &atev1alpha1.SandboxConfigReference{Name: "def"}}},
			want:    true,
		},
		{
			name: "one class has no config",
			classes: []atev1alpha1.WorkerPoolSandboxClassStatus{
				{Name: atev1alpha1.SandboxClassGvisor, ConfigRef: &atev1alpha1.SandboxConfigReference{Name: "def"}},
				{Name: atev1alpha1.SandboxClassMicroVM},
			},
			want: false,
		},
		{
			name: "no classes",
			want: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := everyClassHasConfig(tc.classes); got != tc.want {
				t.Errorf("everyClassHasConfig = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestReconcileAppliesExistingDeploymentFirst verifies that a pool with a
// worker Deployment gets it applied even when a SandboxConfig step of the
// same reconcile fails, and that a pool without one gets none when those
// steps fail.
func TestReconcileAppliesExistingDeploymentFirst(t *testing.T) {
	t.Parallel()
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatalf("add client-go scheme: %v", err)
	}
	if err := atev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add scheme: %v", err)
	}
	errInjected := errors.New("injected")
	failList := interceptor.Funcs{
		List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
			if _, ok := list.(*atev1alpha1.SandboxConfigList); ok {
				return errInjected
			}
			return c.List(ctx, list, opts...)
		},
	}
	failStatus := interceptor.Funcs{
		SubResourceUpdate: func(context.Context, client.Client, string, client.Object, ...client.SubResourceUpdateOption) error {
			return errInjected
		},
	}
	tests := []struct {
		name  string
		funcs interceptor.Funcs
		// hasDeployment is whether the pool has a Deployment, with 1 replica,
		// before the reconcile.
		hasDeployment bool
		// wantReplicas is the Deployment's replicas afterwards. 0 means no
		// Deployment.
		wantReplicas int32
	}{
		{
			name:          "list fails, Deployment exists",
			funcs:         failList,
			hasDeployment: true,
			wantReplicas:  3,
		},
		{
			name:          "status write fails, Deployment exists",
			funcs:         failStatus,
			hasDeployment: true,
			wantReplicas:  3,
		},
		{
			name:  "list fails, no Deployment",
			funcs: failList,
		},
		{
			name:  "status write fails, no Deployment",
			funcs: failStatus,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			wp := makeWorkerPool("pool", "default", 3, "ateom:v1")
			objs := []client.Object{wp, makeSandboxConfig("def", atev1alpha1.SandboxClassGvisor, true)}
			if tc.hasDeployment {
				objs = append(objs, &appsv1.Deployment{
					ObjectMeta: metav1.ObjectMeta{Name: wp.Name, Namespace: wp.Namespace},
					Spec:       appsv1.DeploymentSpec{Replicas: ptr.To[int32](1)},
				})
			}
			c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).WithStatusSubresource(wp).
				WithInterceptorFuncs(tc.funcs).Build()
			r := &WorkerPoolReconciler{Client: c}
			if _, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(wp)}); !errors.Is(err, errInjected) {
				t.Fatalf("Reconcile error = %v, want %v", err, errInjected)
			}

			dep := &appsv1.Deployment{}
			err := c.Get(t.Context(), client.ObjectKeyFromObject(wp), dep)
			if tc.wantReplicas == 0 {
				if !k8errors.IsNotFound(err) {
					t.Errorf("get Deployment error = %v, want NotFound", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("get Deployment: %v", err)
			}
			if got := ptr.Deref(dep.Spec.Replicas, 0); got != tc.wantReplicas {
				t.Errorf("Deployment replicas = %d, want %d", got, tc.wantReplicas)
			}
		})
	}
}

func TestSyncStatus(t *testing.T) {
	t.Parallel()
	scheme := runtime.NewScheme()
	if err := atev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add scheme: %v", err)
	}
	classes := func(config string) []atev1alpha1.WorkerPoolSandboxClassStatus {
		return []atev1alpha1.WorkerPoolSandboxClassStatus{{
			Name:      atev1alpha1.SandboxClassGvisor,
			ConfigRef: &atev1alpha1.SandboxConfigReference{Name: config},
			Conditions: []metav1.Condition{{
				Type:               atev1alpha1.WorkerPoolSandboxClassConditionResolved,
				Status:             metav1.ConditionTrue,
				Reason:             atev1alpha1.WorkerPoolReasonResolved,
				Message:            "resolved to SandboxConfig " + config,
				ObservedGeneration: 2,
				LastTransitionTime: metav1.NewTime(time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)),
			}},
		}}
	}
	before := atev1alpha1.WorkerPoolStatus{
		Replicas:       3,
		ReadyReplicas:  2,
		Selector:       "app=old",
		SandboxClasses: classes("old"),
	}
	dep := &appsv1.Deployment{
		Spec:   appsv1.DeploymentSpec{Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "new"}}},
		Status: appsv1.DeploymentStatus{Replicas: 5, ReadyReplicas: 4},
	}

	tests := []struct {
		name      string
		dep       *appsv1.Deployment
		classes   []atev1alpha1.WorkerPoolSandboxClassStatus
		want      atev1alpha1.WorkerPoolStatus
		wantWrite bool
	}{
		{
			name:    "nil Deployment keeps the replica fields",
			classes: classes("new"),
			want: atev1alpha1.WorkerPoolStatus{
				Replicas: 3, ReadyReplicas: 2, Selector: "app=old",
				SandboxClasses: classes("new"),
			},
			wantWrite: true,
		},
		{
			name:    "Deployment replaces the replica fields",
			dep:     dep,
			classes: classes("new"),
			want: atev1alpha1.WorkerPoolStatus{
				Replicas: 5, ReadyReplicas: 4, Selector: "app=new",
				SandboxClasses: classes("new"),
			},
			wantWrite: true,
		},
		{
			name:      "nothing changed writes nothing",
			classes:   classes("old"),
			want:      before,
			wantWrite: false,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			wp := makeWorkerPool("pool", "default", 1, "ateom:v1")
			wp.Status = *before.DeepCopy()
			c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(wp).WithStatusSubresource(wp).Build()
			stored := &atev1alpha1.WorkerPool{}
			if err := c.Get(t.Context(), client.ObjectKeyFromObject(wp), stored); err != nil {
				t.Fatalf("get WorkerPool: %v", err)
			}
			// Update refreshes stored's resourceVersion in place, so keep
			// the one from before the call.
			before := stored.ResourceVersion
			r := &WorkerPoolReconciler{Client: c}
			if err := r.syncStatus(t.Context(), stored, tc.dep, tc.classes); err != nil {
				t.Fatalf("syncStatus: %v", err)
			}
			got := &atev1alpha1.WorkerPool{}
			if err := c.Get(t.Context(), client.ObjectKeyFromObject(wp), got); err != nil {
				t.Fatalf("get WorkerPool: %v", err)
			}
			if !equality.Semantic.DeepEqual(got.Status, tc.want) {
				t.Errorf("status = %+v, want %+v", got.Status, tc.want)
			}
			if wrote := got.ResourceVersion != before; wrote != tc.wantWrite {
				t.Errorf("wrote status = %v, want %v", wrote, tc.wantWrite)
			}
		})
	}
}

// --- helpers ---

func makeSandboxConfig(name string, class atev1alpha1.SandboxClass, isDefault bool) *atev1alpha1.SandboxConfig {
	v := atev1alpha1.SandboxVersionConfig{Name: "v1"}
	if class == atev1alpha1.SandboxClassGvisor {
		v.PauseImage = testPauseImage
	}
	sc := &atev1alpha1.SandboxConfig{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: atev1alpha1.SandboxConfigSpec{
			SandboxClass:   class,
			DefaultVersion: v.Name,
			Versions:       []atev1alpha1.SandboxVersionConfig{v},
		},
	}
	if isDefault {
		sc.Annotations = map[string]string{atev1alpha1.SandboxConfigClassDefaultAnnotation: "true"}
	}
	return sc
}

func createSandboxConfig(t *testing.T, sc *atev1alpha1.SandboxConfig) {
	t.Helper()
	if err := k8sClient.Create(t.Context(), sc); err != nil {
		t.Fatalf("create SandboxConfig %s: %v", sc.Name, err)
	}
	deleteOnCleanup(t, sc)
}

func createWorkerPool(t *testing.T, wp *atev1alpha1.WorkerPool) {
	t.Helper()
	if err := k8sClient.Create(t.Context(), wp); err != nil {
		t.Fatalf("create WorkerPool %s: %v", wp.Name, err)
	}
	deleteOnCleanup(t, wp)
}

// waitResolved waits for wp's sandbox class to report Resolved=True for its
// current generation and to use config.
func waitResolved(t *testing.T, wp *atev1alpha1.WorkerPool, config string) *atev1alpha1.WorkerPool {
	t.Helper()
	return waitCondition(t, wp, func(current *atev1alpha1.WorkerPool, cond *metav1.Condition) bool {
		return cond.Status == metav1.ConditionTrue &&
			statusConfig(current, current.Spec.DefaultSandboxClass()) == config
	})
}

// waitUnresolved waits for wp's sandbox class to report Resolved=False with
// reason for its current generation.
func waitUnresolved(t *testing.T, wp *atev1alpha1.WorkerPool, reason string) *atev1alpha1.WorkerPool {
	t.Helper()
	return waitCondition(t, wp, func(_ *atev1alpha1.WorkerPool, cond *metav1.Condition) bool {
		return cond.Status == metav1.ConditionFalse && cond.Reason == reason
	})
}

func waitCondition(t *testing.T, wp *atev1alpha1.WorkerPool, ok func(*atev1alpha1.WorkerPool, *metav1.Condition) bool) *atev1alpha1.WorkerPool {
	t.Helper()
	current := &atev1alpha1.WorkerPool{}
	eventually(t, func(ctx context.Context) (bool, error) {
		if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(wp), current); err != nil {
			return false, nil
		}
		status := sandboxClassStatus(current, current.Spec.DefaultSandboxClass())
		if status == nil {
			return false, nil
		}
		cond := meta.FindStatusCondition(status.Conditions, atev1alpha1.WorkerPoolSandboxClassConditionResolved)
		return cond != nil && cond.ObservedGeneration == current.Generation && ok(current, cond), nil
	})
	return current
}

// waitGone waits for obj to no longer exist.
func waitGone(t *testing.T, obj client.Object) {
	t.Helper()
	eventually(t, func(ctx context.Context) (bool, error) {
		err := k8sClient.Get(ctx, client.ObjectKeyFromObject(obj), obj.DeepCopyObject().(client.Object))
		return k8errors.IsNotFound(err), nil
	})
}

// assertNoDeployment fails if a Deployment controlled by wp exists. envtest
// runs no garbage collector, so a same-named Deployment left by a pool from an
// earlier -count iteration does not count.
func assertNoDeployment(t *testing.T, wp *atev1alpha1.WorkerPool) {
	t.Helper()
	dep, err := getDeployment(t.Context(), wp)
	if k8errors.IsNotFound(err) {
		return
	}
	if err != nil {
		t.Fatalf("get Deployment: %v", err)
	}
	if metav1.IsControlledBy(dep, wp) {
		t.Fatalf("unresolved WorkerPool %s has a Deployment", wp.Name)
	}
}

func deleteAndWait(t *testing.T, obj client.Object) {
	t.Helper()
	if err := k8sClient.Delete(t.Context(), obj); err != nil {
		t.Fatalf("delete %s: %v", obj.GetName(), err)
	}
	waitGone(t, obj)
}

// removeFinalizer removes finalizer from the named SandboxConfig, if it still
// exists. It uses its own context because it runs as a cleanup.
func removeFinalizer(t *testing.T, name, finalizer string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	err := retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		sc := &atev1alpha1.SandboxConfig{}
		if err := k8sClient.Get(ctx, types.NamespacedName{Name: name}, sc); err != nil {
			return client.IgnoreNotFound(err)
		}
		if !controllerutil.RemoveFinalizer(sc, finalizer) {
			return nil
		}
		return k8sClient.Update(ctx, sc)
	})
	if err != nil {
		t.Errorf("cleanup: remove finalizer %s from SandboxConfig %s: %v", finalizer, name, err)
	}
}

// makeWorkerPoolClasses returns a pool in ns with the given spec and status
// sandbox classes.
func makeWorkerPoolClasses(ns, name string, spec []atev1alpha1.WorkerPoolSandboxClass, status []atev1alpha1.WorkerPoolSandboxClassStatus) *atev1alpha1.WorkerPool {
	wp := makeWorkerPool(name, ns, 1, "ateom:v1")
	wp.Spec.SandboxClasses = spec
	wp.Status.SandboxClasses = status
	return wp
}

// statusConfig returns the SandboxConfig wp's status uses for class, or "".
func statusConfig(wp *atev1alpha1.WorkerPool, class atev1alpha1.SandboxClass) string {
	if s := sandboxClassStatus(wp, class); s != nil && s.ConfigRef != nil {
		return s.ConfigRef.Name
	}
	return ""
}

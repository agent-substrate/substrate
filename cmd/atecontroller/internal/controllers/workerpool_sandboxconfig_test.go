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
	"slices"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	eventsv1 "k8s.io/api/events/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	k8errors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/events"
	"k8s.io/client-go/util/retry"
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

	tests := []struct {
		name       string
		entry      atev1alpha1.WorkerPoolSandboxClass
		inUse      string
		configs    []atev1alpha1.SandboxConfig
		want       string
		wantReason string
	}{
		{
			name:    "explicit ref resolves even when another config is the default",
			entry:   gvisor("custom"),
			configs: []atev1alpha1.SandboxConfig{sc("custom", atev1alpha1.SandboxClassGvisor, false, t0), sc("def", atev1alpha1.SandboxClassGvisor, true, t1)},
			want:    "custom",
		},
		{
			name:       "explicit ref not found",
			entry:      gvisor("missing"),
			configs:    []atev1alpha1.SandboxConfig{sc("def", atev1alpha1.SandboxClassGvisor, true, t0)},
			wantReason: atev1alpha1.WorkerPoolReasonSandboxConfigNotFound,
		},
		{
			name:       "explicit ref of another class",
			entry:      gvisor("vm"),
			configs:    []atev1alpha1.SandboxConfig{sc("vm", atev1alpha1.SandboxClassMicroVM, false, t0)},
			wantReason: atev1alpha1.WorkerPoolReasonSandboxClassMismatch,
		},
		{
			name:       "explicit ref being deleted takes no new users",
			entry:      gvisor("dying"),
			configs:    []atev1alpha1.SandboxConfig{sc("dying", atev1alpha1.SandboxClassGvisor, false, t0, deleting)},
			wantReason: atev1alpha1.WorkerPoolReasonSandboxConfigDeleting,
		},
		{
			name:    "explicit ref being deleted still resolves for a pool already using it",
			entry:   gvisor("dying"),
			inUse:   "dying",
			configs: []atev1alpha1.SandboxConfig{sc("dying", atev1alpha1.SandboxClassGvisor, false, t0, deleting)},
			want:    "dying",
		},
		{
			name:       "no default",
			entry:      gvisor(""),
			configs:    []atev1alpha1.SandboxConfig{sc("plain", atev1alpha1.SandboxClassGvisor, false, t0)},
			wantReason: atev1alpha1.WorkerPoolReasonDefaultConfigNotFound,
		},
		{
			name:       "default of another class is ignored",
			entry:      gvisor(""),
			configs:    []atev1alpha1.SandboxConfig{sc("vm", atev1alpha1.SandboxClassMicroVM, true, t0)},
			wantReason: atev1alpha1.WorkerPoolReasonDefaultConfigNotFound,
		},
		{
			name:  "annotation must be exactly true",
			entry: gvisor(""),
			configs: []atev1alpha1.SandboxConfig{sc("no", atev1alpha1.SandboxClassGvisor, false, t0, func(c *atev1alpha1.SandboxConfig) {
				c.Annotations = map[string]string{atev1alpha1.SandboxConfigClassDefaultAnnotation: "True"}
			})},
			wantReason: atev1alpha1.WorkerPoolReasonDefaultConfigNotFound,
		},
		{
			name:       "default being deleted is ignored by a pool not using it",
			entry:      gvisor(""),
			configs:    []atev1alpha1.SandboxConfig{sc("dying", atev1alpha1.SandboxClassGvisor, true, t0, deleting)},
			wantReason: atev1alpha1.WorkerPoolReasonDefaultConfigNotFound,
		},
		{
			name:    "default being deleted is kept by a pool already using it",
			entry:   gvisor(""),
			inUse:   "dying",
			configs: []atev1alpha1.SandboxConfig{sc("dying", atev1alpha1.SandboxClassGvisor, true, t0, deleting)},
			want:    "dying",
		},
		{
			name:    "a live default replaces the deleting one in use, even when older",
			entry:   gvisor(""),
			inUse:   "dying",
			configs: []atev1alpha1.SandboxConfig{sc("dying", atev1alpha1.SandboxClassGvisor, true, t1, deleting), sc("old", atev1alpha1.SandboxClassGvisor, true, t0)},
			want:    "old",
		},
		{
			name:    "config in use that lost the default annotation is kept",
			entry:   gvisor(""),
			inUse:   "plain",
			configs: []atev1alpha1.SandboxConfig{sc("plain", atev1alpha1.SandboxClassGvisor, false, t0)},
			want:    "plain",
		},
		{
			name:       "config in use that no longer exists is not kept",
			entry:      gvisor(""),
			inUse:      "gone",
			configs:    []atev1alpha1.SandboxConfig{sc("plain", atev1alpha1.SandboxClassGvisor, false, t0)},
			wantReason: atev1alpha1.WorkerPoolReasonDefaultConfigNotFound,
		},
		{
			name:       "config in use of another class is not kept",
			entry:      gvisor(""),
			inUse:      "vm",
			configs:    []atev1alpha1.SandboxConfig{sc("vm", atev1alpha1.SandboxClassMicroVM, true, t0)},
			wantReason: atev1alpha1.WorkerPoolReasonDefaultConfigNotFound,
		},
		{
			name:    "newest default wins",
			entry:   gvisor(""),
			configs: []atev1alpha1.SandboxConfig{sc("a-old", atev1alpha1.SandboxClassGvisor, true, t0), sc("z-new", atev1alpha1.SandboxClassGvisor, true, t1), sc("plain", atev1alpha1.SandboxClassGvisor, false, t1)},
			want:    "z-new",
		},
		{
			name:    "newest default wins regardless of list order",
			entry:   gvisor(""),
			configs: []atev1alpha1.SandboxConfig{sc("z-new", atev1alpha1.SandboxClassGvisor, true, t1), sc("a-old", atev1alpha1.SandboxClassGvisor, true, t0)},
			want:    "z-new",
		},
		{
			name:    "creation time tie goes to the first name",
			entry:   gvisor(""),
			configs: []atev1alpha1.SandboxConfig{sc("b", atev1alpha1.SandboxClassGvisor, true, t0), sc("a", atev1alpha1.SandboxClassGvisor, true, t0)},
			want:    "a",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, reason, message := resolveSandboxConfig(tc.entry, tc.inUse, tc.configs)
			if tc.wantReason != "" {
				if got != nil || reason != tc.wantReason || message == "" {
					t.Fatalf("resolveSandboxConfig = (%v, %q, %q), want (nil, %q, <message>)", got, reason, message, tc.wantReason)
				}
				return
			}
			if got == nil || got.Name != tc.want || reason != "" {
				t.Fatalf("resolveSandboxConfig = (%v, %q, %q), want %q", got, reason, message, tc.want)
			}
		})
	}
}

// TestSandboxConfigProtectionReconcile checks that the reconciler never adds
// the protection finalizer, and that a deleting SandboxConfig loses it only
// when neither the cache nor a live read finds a WorkerPool naming it.
func TestSandboxConfigProtectionReconcile(t *testing.T) {
	t.Parallel()
	scheme := runtime.NewScheme()
	if err := atev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add scheme: %v", err)
	}
	now := metav1.Now()
	using := func(name string) *atev1alpha1.WorkerPool {
		wp := makeWorkerPool("pool", "default", 1, "ateom:v1")
		wp.Status.SandboxClasses = []atev1alpha1.WorkerPoolSandboxClassStatus{{
			Name:      atev1alpha1.SandboxClassGvisor,
			ConfigRef: atev1alpha1.SandboxConfigReference{Name: name},
		}}
		return wp
	}
	// config builds a SandboxConfig that has the protection finalizer if
	// protected. A deleting one carries another finalizer too, as the fake
	// client, like the API server, keeps no deleting object without one.
	config := func(protected, deleting bool) *atev1alpha1.SandboxConfig {
		sc := makeSandboxConfig("sc", atev1alpha1.SandboxClassGvisor, true)
		if protected {
			controllerutil.AddFinalizer(sc, atev1alpha1.SandboxConfigWorkerPoolProtectionFinalizer)
		}
		if deleting {
			controllerutil.AddFinalizer(sc, testHoldFinalizer)
			sc.DeletionTimestamp = &now
		}
		return sc
	}
	newClient := func(objs ...client.Object) client.Client {
		return fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).
			WithIndex(&atev1alpha1.WorkerPool{}, workerPoolSandboxConfigIndex, func(obj client.Object) []string {
				return sandboxConfigsInUse(obj.(*atev1alpha1.WorkerPool))
			}).Build()
	}

	tests := []struct {
		name   string
		sc     *atev1alpha1.SandboxConfig
		cached []client.Object
		live   []client.Object
		want   bool
	}{
		{
			name: "live and unused: left alone",
			sc:   config(false, false),
			want: false,
		},
		{
			name:   "live and used but unprotected: left alone, adding is the WorkerPool controller's job",
			sc:     config(false, false),
			cached: []client.Object{using("sc")},
			want:   false,
		},
		{
			name: "live and protected: keep",
			sc:   config(true, false),
			want: true,
		},
		{
			name:   "deleting, used in cache: keep",
			sc:     config(true, true),
			cached: []client.Object{using("sc")},
			want:   true,
		},
		{
			name: "deleting, unused in cache but used live: keep",
			sc:   config(true, true),
			live: []client.Object{using("sc")},
			want: true,
		},
		{
			name:   "deleting and unused: remove",
			sc:     config(true, true),
			cached: []client.Object{using("other")},
			live:   []client.Object{using("other")},
			want:   false,
		},
		{
			name: "deleting without the finalizer: nothing to do",
			sc:   config(false, true),
			want: false,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			c := newClient(append([]client.Object{tc.sc}, tc.cached...)...)
			r := &SandboxConfigProtectionReconciler{Client: c, APIReader: newClient(tc.live...)}
			if _, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: types.NamespacedName{Name: tc.sc.Name}}); err != nil {
				t.Fatalf("Reconcile: %v", err)
			}
			got := &atev1alpha1.SandboxConfig{}
			if err := c.Get(t.Context(), types.NamespacedName{Name: tc.sc.Name}, got); err != nil {
				t.Fatalf("get SandboxConfig: %v", err)
			}
			if has := controllerutil.ContainsFinalizer(got, atev1alpha1.SandboxConfigWorkerPoolProtectionFinalizer); has != tc.want {
				t.Errorf("has protection finalizer = %v, want %v", has, tc.want)
			}
		})
	}
}

// TestReconcileProtectsBeforeRecording verifies that the WorkerPool
// controller puts the protection finalizer on a config it resolved, and does
// so before its status names the config. It is the only writer that adds the
// finalizer, so a fake client without the protection reconciler is the real
// situation, not a simplification.
func TestReconcileProtectsBeforeRecording(t *testing.T) {
	t.Parallel()
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatalf("add client-go scheme: %v", err)
	}
	if err := atev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add scheme: %v", err)
	}
	protected := func(sc *atev1alpha1.SandboxConfig) *atev1alpha1.SandboxConfig {
		controllerutil.AddFinalizer(sc, atev1alpha1.SandboxConfigWorkerPoolProtectionFinalizer)
		return sc
	}
	tests := []struct {
		name   string
		config *atev1alpha1.SandboxConfig
		// ref is the pool's configRef; "" follows the class default.
		ref string
		// wantResolved is whether the pool must end up using config.
		wantResolved bool
		// wantProtected is whether config must carry the finalizer afterwards.
		wantProtected bool
	}{
		{
			name:          "unprotected class default",
			config:        makeSandboxConfig("fresh-default", atev1alpha1.SandboxClassGvisor, true),
			wantResolved:  true,
			wantProtected: true,
		},
		{
			name:          "unprotected configRef target",
			config:        makeSandboxConfig("fresh-ref", atev1alpha1.SandboxClassGvisor, false),
			ref:           "fresh-ref",
			wantResolved:  true,
			wantProtected: true,
		},
		{
			name:          "already protected config",
			config:        protected(makeSandboxConfig("held", atev1alpha1.SandboxClassGvisor, true)),
			wantResolved:  true,
			wantProtected: true,
		},
		{
			name:          "unresolved pool protects nothing",
			config:        makeSandboxConfig("other-class", atev1alpha1.SandboxClassMicroVM, false),
			ref:           "other-class",
			wantResolved:  false,
			wantProtected: false,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			wp := makeWorkerPool("pool", "default", 1, "ateom:v1")
			if tc.ref != "" {
				wp.Spec.SandboxClasses[0].ConfigRef = &atev1alpha1.SandboxConfigReference{Name: tc.ref}
			}
			// protectedAtWrite records whether config carried the finalizer
			// at the moment the pool's status was written.
			var wroteStatus, protectedAtWrite bool
			c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(wp, tc.config).WithStatusSubresource(wp).
				WithInterceptorFuncs(interceptor.Funcs{
					SubResourceUpdate: func(ctx context.Context, c client.Client, sub string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
						sc := &atev1alpha1.SandboxConfig{}
						if err := c.Get(ctx, types.NamespacedName{Name: tc.config.Name}, sc); err != nil {
							return err
						}
						wroteStatus = true
						protectedAtWrite = controllerutil.ContainsFinalizer(sc, atev1alpha1.SandboxConfigWorkerPoolProtectionFinalizer)
						return c.SubResource(sub).Update(ctx, obj, opts...)
					},
				}).Build()
			r := &WorkerPoolReconciler{Client: c, Recorder: events.NewFakeRecorder(1)}
			if _, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(wp)}); err != nil {
				t.Fatalf("Reconcile: %v", err)
			}

			sc := &atev1alpha1.SandboxConfig{}
			if err := c.Get(t.Context(), types.NamespacedName{Name: tc.config.Name}, sc); err != nil {
				t.Fatalf("get SandboxConfig: %v", err)
			}
			if got := controllerutil.ContainsFinalizer(sc, atev1alpha1.SandboxConfigWorkerPoolProtectionFinalizer); got != tc.wantProtected {
				t.Errorf("config protected = %v, want %v", got, tc.wantProtected)
			}
			got := &atev1alpha1.WorkerPool{}
			if err := c.Get(t.Context(), client.ObjectKeyFromObject(wp), got); err != nil {
				t.Fatalf("get WorkerPool: %v", err)
			}
			if resolved := sandboxConfigInUse(got, atev1alpha1.SandboxClassGvisor) == tc.config.Name; resolved != tc.wantResolved {
				t.Errorf("status names config = %v, want %v (status %+v)", resolved, tc.wantResolved, got.Status)
			}
			if !wroteStatus {
				t.Fatalf("status was never written")
			}
			if tc.wantResolved && !protectedAtWrite {
				t.Errorf("status named the config before it carried the protection finalizer")
			}
		})
	}
}

// TestUnusedSandboxConfigDeletesAtOnce verifies that a SandboxConfig no pool
// has used carries no protection finalizer and deletes without waiting. A
// pool that uses another config is resolved in between, which shows both
// controllers have been running while the unused config existed.
func TestUnusedSandboxConfigDeletesAtOnce(t *testing.T) {
	t.Parallel()
	sc := makeSandboxConfig("unused-sc", atev1alpha1.SandboxClassGvisor, false)
	createSandboxConfig(t, sc)
	other := makeSandboxConfig("unused-sc-other", atev1alpha1.SandboxClassGvisor, false)
	createSandboxConfig(t, other)
	wp := makeWorkerPool("test-unused-sc", "default", 1, "ateom:v1")
	wp.Spec.SandboxClasses[0].ConfigRef = &atev1alpha1.SandboxConfigReference{Name: other.Name}
	createWorkerPool(t, wp)
	waitResolvedAndDeployed(t, wp, other.Name)

	got := &atev1alpha1.SandboxConfig{}
	if err := k8sClient.Get(t.Context(), types.NamespacedName{Name: sc.Name}, got); err != nil {
		t.Fatalf("get SandboxConfig: %v", err)
	}
	if controllerutil.ContainsFinalizer(got, atev1alpha1.SandboxConfigWorkerPoolProtectionFinalizer) {
		t.Fatalf("unused SandboxConfig %s carries the protection finalizer", sc.Name)
	}
	deleteAndWait(t, sc)
}

// TestWorkerPoolConfigRef covers a pool with an explicit configRef. It
// resolves only when the named SandboxConfig exists, is of the pool's class,
// and is not being deleted; it then protects the config and gets its
// Deployment. Otherwise it reports why, gets no Deployment, and emits a
// Warning event.
func TestWorkerPoolConfigRef(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		// config is created before the pool; nil creates none.
		config *atev1alpha1.SandboxConfig
		// deleting marks config for deletion before the pool is created.
		deleting bool
		ref      string
		// wantReason is the SandboxConfigResolved=False reason to expect, or
		// "" when the pool must resolve.
		wantReason string
		// late is created once the pool reports wantReason; the pool must
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
			waitUnresolved(t, wp, tc.wantReason)
			assertNoDeployment(t, wp)
			waitWarningEvent(t, wp, tc.wantReason)
			if tc.late != nil {
				createSandboxConfig(t, tc.late)
				waitResolvedAndDeployed(t, wp, tc.ref)
			}
		})
	}
}

// waitResolvedAndDeployed waits for wp to resolve to config, for config to
// carry the protection finalizer, and for wp's Deployment to exist.
func waitResolvedAndDeployed(t *testing.T, wp *atev1alpha1.WorkerPool, config string) {
	t.Helper()
	waitResolved(t, wp, config)
	waitProtectionFinalizer(t, config, true)
	eventually(t, func(ctx context.Context) (bool, error) {
		_, err := getDeployment(ctx, wp)
		return err == nil, nil
	})
}

// waitWarningEvent waits for a Warning event with reason about wp.
func waitWarningEvent(t *testing.T, wp *atev1alpha1.WorkerPool, reason string) {
	t.Helper()
	eventually(t, func(ctx context.Context) (bool, error) {
		var evs eventsv1.EventList
		if err := k8sClient.List(ctx, &evs, client.InNamespace(wp.Namespace)); err != nil {
			return false, err
		}
		for _, ev := range evs.Items {
			if ev.Regarding.Name == wp.Name && ev.Type == "Warning" && ev.Reason == reason {
				return true, nil
			}
		}
		return false, nil
	})
}

// TestWorkerPoolUnresolvedKeepsDeployment verifies that a pool whose spec
// stops resolving keeps its last resolved config in status and that the
// controller stops updating its Deployment.
func TestWorkerPoolUnresolvedKeepsDeployment(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	wp := makeWorkerPool("test-unresolved-keeps", "default", 1, "ateom:v1")
	createWorkerPool(t, wp)
	waitResolved(t, wp, testGvisorDefault)
	eventually(t, func(ctx context.Context) (bool, error) {
		_, err := getDeployment(ctx, wp)
		return err == nil, nil
	})

	updateWorkerPoolSpec(t, ctx, wp, "point WorkerPool at a missing SandboxConfig and scale it", func(wp *atev1alpha1.WorkerPool) {
		wp.Spec.Replicas = 5
		wp.Spec.SandboxClasses[0].ConfigRef = &atev1alpha1.SandboxConfigReference{Name: "never-created"}
	})
	current := waitUnresolved(t, wp, atev1alpha1.WorkerPoolReasonSandboxConfigNotFound)
	if got := sandboxConfigInUse(current, atev1alpha1.SandboxClassGvisor); got != testGvisorDefault {
		t.Errorf("status.sandboxClasses names %q, want the last resolved %q", got, testGvisorDefault)
	}
	dep, err := getDeployment(ctx, wp)
	if err != nil {
		t.Fatalf("get Deployment: %v", err)
	}
	if dep.Spec.Replicas == nil || *dep.Spec.Replicas != 1 {
		t.Errorf("Deployment replicas = %v, want 1: an unresolved pool must not update its Deployment", dep.Spec.Replicas)
	}
}

// TestSandboxConfigInUseProtection verifies that a SandboxConfig shared by two
// pools survives a delete until both pools are gone, and that the pools
// themselves delete without waiting.
//
// A third pool uses a sentinel config that is deleted at the same time as the
// shared one. The two pool deletions reach the protection reconciler through
// the same WorkerPool watch, in order, and it reconciles one config at a
// time, so once the sentinel is gone the shared config has been reconciled
// for the first deletion and found still in use.
func TestSandboxConfigInUseProtection(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	sc := makeSandboxConfig("shared-sc", atev1alpha1.SandboxClassGvisor, false)
	createSandboxConfig(t, sc)
	sentinel := makeSandboxConfig("shared-sentinel", atev1alpha1.SandboxClassGvisor, false)
	createSandboxConfig(t, sentinel)
	var pools []*atev1alpha1.WorkerPool
	for _, p := range []struct{ pool, config string }{
		{"test-shared-a", sc.Name},
		{"test-shared-b", sc.Name},
		{"test-shared-sentinel", sentinel.Name},
	} {
		wp := makeWorkerPool(p.pool, "default", 1, "ateom:v1")
		wp.Spec.SandboxClasses[0].ConfigRef = &atev1alpha1.SandboxConfigReference{Name: p.config}
		createWorkerPool(t, wp)
		waitResolved(t, wp, p.config)
		pools = append(pools, wp)
	}
	waitProtectionFinalizer(t, sc.Name, true)
	waitProtectionFinalizer(t, sentinel.Name, true)

	for _, c := range []*atev1alpha1.SandboxConfig{sc, sentinel} {
		if err := k8sClient.Delete(ctx, c); err != nil {
			t.Fatalf("delete SandboxConfig %s: %v", c.Name, err)
		}
	}
	deleteAndWait(t, pools[0])
	deleteAndWait(t, pools[2])
	waitGone(t, sentinel)
	// The shared config has been reconciled for pools[0]'s deletion; it must
	// stay for pools[1].
	got := &atev1alpha1.SandboxConfig{}
	if err := k8sClient.Get(ctx, types.NamespacedName{Name: sc.Name}, got); err != nil {
		t.Fatalf("SandboxConfig still used by %s: get: %v", pools[1].Name, err)
	}
	if got.DeletionTimestamp.IsZero() {
		t.Fatalf("SandboxConfig has no deletionTimestamp after delete")
	}

	deleteAndWait(t, pools[1])
	waitGone(t, sc)
}

// TestWorkerPoolSwitchConfigRefReleasesOld verifies that once a pool moves to
// another SandboxConfig, the one it left can be deleted.
func TestWorkerPoolSwitchConfigRefReleasesOld(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	createSandboxConfig(t, makeSandboxConfig("switch-from", atev1alpha1.SandboxClassGvisor, false))
	createSandboxConfig(t, makeSandboxConfig("switch-to", atev1alpha1.SandboxClassGvisor, false))
	wp := makeWorkerPool("test-switch-ref", "default", 1, "ateom:v1")
	wp.Spec.SandboxClasses[0].ConfigRef = &atev1alpha1.SandboxConfigReference{Name: "switch-from"}
	createWorkerPool(t, wp)
	waitResolved(t, wp, "switch-from")
	waitProtectionFinalizer(t, "switch-from", true)

	updateWorkerPoolSpec(t, ctx, wp, "switch configRef", func(wp *atev1alpha1.WorkerPool) {
		wp.Spec.SandboxClasses[0].ConfigRef.Name = "switch-to"
	})
	waitResolved(t, wp, "switch-to")
	waitProtectionFinalizer(t, "switch-to", true)
	deleteAndWait(t, &atev1alpha1.SandboxConfig{ObjectMeta: metav1.ObjectMeta{Name: "switch-from"}})
}

// TestWorkerPoolDefaultNewestWins verifies that a pool without a configRef
// follows the newest class default. It is not parallel because it owns the
// microvm class default.
func TestWorkerPoolDefaultNewestWins(t *testing.T) {
	wp := makeWorkerPool("test-default-newest", "default", 1, "ateom:v1")
	wp.Spec.SandboxClasses[0].Name = atev1alpha1.SandboxClassMicroVM
	createWorkerPool(t, wp)
	waitUnresolved(t, wp, atev1alpha1.WorkerPoolReasonDefaultConfigNotFound)
	assertNoDeployment(t, wp)

	createSandboxConfig(t, makeSandboxConfig("mvdef-a", atev1alpha1.SandboxClassMicroVM, true))
	waitResolved(t, wp, "mvdef-a")
	waitProtectionFinalizer(t, "mvdef-a", true)
	eventually(t, func(ctx context.Context) (bool, error) {
		_, err := getDeployment(ctx, wp)
		return err == nil, nil
	})

	// creationTimestamp has one-second resolution, and the name tie-break
	// would pick mvdef-a, so only a later creation time can make mvdef-b win.
	time.Sleep(1100 * time.Millisecond)
	createSandboxConfig(t, makeSandboxConfig("mvdef-b", atev1alpha1.SandboxClassMicroVM, true))
	waitResolved(t, wp, "mvdef-b")
	waitProtectionFinalizer(t, "mvdef-b", true)
	// Nothing uses mvdef-a any more, so it can go.
	deleteAndWait(t, &atev1alpha1.SandboxConfig{ObjectMeta: metav1.ObjectMeta{Name: "mvdef-a"}})
}

// TestWorkerPoolConfigRefDeletingKeepsCurrentUser verifies that a pool
// already using a SandboxConfig keeps resolving, and keeps its Deployment
// updated, after the config is marked for deletion.
func TestWorkerPoolConfigRefDeletingKeepsCurrentUser(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	sc := makeSandboxConfig("dying-in-use", atev1alpha1.SandboxClassGvisor, false)
	controllerutil.AddFinalizer(sc, testHoldFinalizer)
	createSandboxConfig(t, sc)
	t.Cleanup(func() { removeFinalizer(t, sc.Name, testHoldFinalizer) })

	wp := makeWorkerPool("test-ref-deleting-in-use", "default", 1, "ateom:v1")
	wp.Spec.SandboxClasses[0].ConfigRef = &atev1alpha1.SandboxConfigReference{Name: sc.Name}
	createWorkerPool(t, wp)
	waitResolved(t, wp, sc.Name)
	waitProtectionFinalizer(t, sc.Name, true)

	if err := k8sClient.Delete(ctx, sc); err != nil {
		t.Fatalf("delete SandboxConfig: %v", err)
	}
	got := &atev1alpha1.SandboxConfig{}
	if err := k8sClient.Get(ctx, types.NamespacedName{Name: sc.Name}, got); err != nil {
		t.Fatalf("get SandboxConfig: %v", err)
	}
	if got.DeletionTimestamp.IsZero() {
		t.Fatalf("SandboxConfig has no deletionTimestamp after delete")
	}
	// A pool that names the config only now is refused. That shows the
	// controller's cache has seen the deletion, so the reconcile below
	// resolves against a deleting config.
	late := makeWorkerPool("test-ref-deleting-in-use-late", "default", 1, "ateom:v1")
	late.Spec.SandboxClasses[0].ConfigRef = &atev1alpha1.SandboxConfigReference{Name: sc.Name}
	createWorkerPool(t, late)
	waitUnresolved(t, late, atev1alpha1.WorkerPoolReasonSandboxConfigDeleting)

	updateWorkerPoolSpec(t, ctx, wp, "scale WorkerPool", func(wp *atev1alpha1.WorkerPool) {
		wp.Spec.Replicas = 3
	})
	waitResolved(t, wp, sc.Name)
	eventually(t, func(ctx context.Context) (bool, error) {
		dep, err := getDeployment(ctx, wp)
		if err != nil {
			return false, nil
		}
		return dep.Spec.Replicas != nil && *dep.Spec.Replicas == 3, nil
	})
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
		return atev1alpha1.WorkerPoolSandboxClassStatus{Name: atev1alpha1.SandboxClassMicroVM, ConfigRef: atev1alpha1.SandboxConfigReference{Name: name}}
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
// nothing for an object of another kind.
func TestWorkerPoolsForSandboxConfig(t *testing.T) {
	t.Parallel()
	scheme := runtime.NewScheme()
	if err := atev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add scheme: %v", err)
	}
	gvisor := []atev1alpha1.WorkerPoolSandboxClass{{Name: atev1alpha1.SandboxClassGvisor}}
	microvm := []atev1alpha1.WorkerPoolSandboxClass{{Name: atev1alpha1.SandboxClassMicroVM}}
	microvmRef := []atev1alpha1.WorkerPoolSandboxClass{{Name: atev1alpha1.SandboxClassMicroVM, ConfigRef: &atev1alpha1.SandboxConfigReference{Name: "sc"}}}
	usingSC := []atev1alpha1.WorkerPoolSandboxClassStatus{{Name: atev1alpha1.SandboxClassMicroVM, ConfigRef: atev1alpha1.SandboxConfigReference{Name: "sc"}}}
	r := &WorkerPoolReconciler{Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(
		makeWorkerPoolClasses("a", "gv", gvisor, nil),
		makeWorkerPoolClasses("b", "vm", microvm, nil),
		makeWorkerPoolClasses("b", "vm-ref", microvmRef, nil),
		makeWorkerPoolClasses("b", "vm-status", microvm, usingSC),
	).Build()}
	req := func(ns, name string) reconcile.Request {
		return reconcile.Request{NamespacedName: types.NamespacedName{Namespace: ns, Name: name}}
	}

	tests := []struct {
		name string
		obj  client.Object
		want []reconcile.Request
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
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := r.workerPoolsForSandboxConfig(t.Context(), tc.obj)
			slices.SortFunc(got, func(a, b reconcile.Request) int { return cmp.Compare(a.String(), b.String()) })
			if !slices.Equal(got, tc.want) {
				t.Errorf("workerPoolsForSandboxConfig = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestRecordUnresolved verifies that a failure is reported once: again only
// when its reason or message changes or the pool resolved in between.
func TestRecordUnresolved(t *testing.T) {
	t.Parallel()
	cond := metav1.Condition{
		Type:    atev1alpha1.WorkerPoolConditionSandboxConfigResolved,
		Status:  metav1.ConditionFalse,
		Reason:  atev1alpha1.WorkerPoolReasonSandboxConfigNotFound,
		Message: `SandboxConfig "x" not found`,
	}
	prev := func(status metav1.ConditionStatus, reason, message string) []metav1.Condition {
		return []metav1.Condition{{Type: cond.Type, Status: status, Reason: reason, Message: message}}
	}

	tests := []struct {
		name       string
		conditions []metav1.Condition
		wantEvent  bool
	}{
		{
			name:      "first failure",
			wantEvent: true,
		},
		{
			name:       "same failure already reported",
			conditions: prev(metav1.ConditionFalse, cond.Reason, cond.Message),
			wantEvent:  false,
		},
		{
			name:       "another reason",
			conditions: prev(metav1.ConditionFalse, atev1alpha1.WorkerPoolReasonSandboxClassMismatch, cond.Message),
			wantEvent:  true,
		},
		{
			name:       "another message",
			conditions: prev(metav1.ConditionFalse, cond.Reason, `SandboxConfig "y" not found`),
			wantEvent:  true,
		},
		{
			name:       "resolved in between",
			conditions: prev(metav1.ConditionTrue, atev1alpha1.WorkerPoolReasonResolved, "every sandbox class resolved to a SandboxConfig"),
			wantEvent:  true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			wp := makeWorkerPool("pool", "default", 1, "ateom:v1")
			wp.Status.Conditions = tc.conditions
			rec := events.NewFakeRecorder(1)
			r := &WorkerPoolReconciler{Recorder: rec}
			r.recordUnresolved(wp, cond)
			var got []string
			for len(rec.Events) > 0 {
				got = append(got, <-rec.Events)
			}
			var want []string
			if tc.wantEvent {
				want = []string{"Warning " + cond.Reason + " " + cond.Message}
			}
			if !slices.Equal(got, want) {
				t.Errorf("events = %q, want %q", got, want)
			}
		})
	}
}

// TestSyncStatusKeepsFieldsNotGiven verifies that syncStatus leaves the
// replica fields alone without a Deployment and sandboxClasses alone without
// classes, and writes nothing when the status would not change.
func TestSyncStatusKeepsFieldsNotGiven(t *testing.T) {
	t.Parallel()
	scheme := runtime.NewScheme()
	if err := atev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add scheme: %v", err)
	}
	const generation = 2
	resolved := metav1.Condition{
		Type:    atev1alpha1.WorkerPoolConditionSandboxConfigResolved,
		Status:  metav1.ConditionTrue,
		Reason:  atev1alpha1.WorkerPoolReasonResolved,
		Message: "every sandbox class resolved to a SandboxConfig",
	}
	recorded := resolved
	recorded.ObservedGeneration = generation
	recorded.LastTransitionTime = metav1.NewTime(time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC))
	classes := func(config string) []atev1alpha1.WorkerPoolSandboxClassStatus {
		return []atev1alpha1.WorkerPoolSandboxClassStatus{{Name: atev1alpha1.SandboxClassGvisor, ConfigRef: atev1alpha1.SandboxConfigReference{Name: config}}}
	}
	before := atev1alpha1.WorkerPoolStatus{
		Replicas:       3,
		ReadyReplicas:  2,
		Selector:       "app=old",
		SandboxClasses: classes("old"),
		Conditions:     []metav1.Condition{recorded},
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
				SandboxClasses: classes("new"), Conditions: []metav1.Condition{recorded},
			},
			wantWrite: true,
		},
		{
			name: "nil classes keeps sandboxClasses",
			dep:  dep,
			want: atev1alpha1.WorkerPoolStatus{
				Replicas: 5, ReadyReplicas: 4, Selector: "app=new",
				SandboxClasses: classes("old"), Conditions: []metav1.Condition{recorded},
			},
			wantWrite: true,
		},
		{
			name:    "both given replace both",
			dep:     dep,
			classes: classes("new"),
			want: atev1alpha1.WorkerPoolStatus{
				Replicas: 5, ReadyReplicas: 4, Selector: "app=new",
				SandboxClasses: classes("new"), Conditions: []metav1.Condition{recorded},
			},
			wantWrite: true,
		},
		{
			name:      "nothing changed writes nothing",
			want:      before,
			wantWrite: false,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			wp := makeWorkerPool("pool", "default", 1, "ateom:v1")
			wp.Generation = generation
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
			if err := r.syncStatus(t.Context(), stored, tc.dep, tc.classes, resolved); err != nil {
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

// TestSetProtectionFinalizer verifies the finalizer is added or removed with
// one patch, that a matching finalizer list is left alone, and that a
// deleting config gets no finalizer.
func TestSetProtectionFinalizer(t *testing.T) {
	t.Parallel()
	scheme := runtime.NewScheme()
	if err := atev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add scheme: %v", err)
	}
	now := metav1.Now()
	// config builds a SandboxConfig that has the protection finalizer if
	// protected. A deleting one carries another finalizer too, as the fake
	// client, like the API server, keeps no deleting object without one.
	config := func(protected, deleting bool) *atev1alpha1.SandboxConfig {
		sc := makeSandboxConfig("sc", atev1alpha1.SandboxClassGvisor, false)
		if protected {
			controllerutil.AddFinalizer(sc, atev1alpha1.SandboxConfigWorkerPoolProtectionFinalizer)
		}
		if deleting {
			controllerutil.AddFinalizer(sc, testHoldFinalizer)
			sc.DeletionTimestamp = &now
		}
		return sc
	}

	tests := []struct {
		name      string
		sc        *atev1alpha1.SandboxConfig
		protect   bool
		want      bool
		wantPatch bool
	}{
		{
			name:      "add",
			sc:        config(false, false),
			protect:   true,
			want:      true,
			wantPatch: true,
		},
		{
			name:    "already present: no patch",
			sc:      config(true, false),
			protect: true,
			want:    true,
		},
		{
			name:    "deleting config gets no finalizer",
			sc:      config(false, true),
			protect: true,
			want:    false,
		},
		{
			name:      "remove",
			sc:        config(true, false),
			protect:   false,
			want:      false,
			wantPatch: true,
		},
		{
			name:    "already absent: no patch",
			sc:      config(false, false),
			protect: false,
			want:    false,
		},
		{
			name:      "deleting config is still released",
			sc:        config(true, true),
			protect:   false,
			want:      false,
			wantPatch: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			patches := 0
			c := interceptor.NewClient(fake.NewClientBuilder().WithScheme(scheme).WithObjects(tc.sc).Build(), interceptor.Funcs{
				Patch: func(ctx context.Context, c client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
					patches++
					return c.Patch(ctx, obj, patch, opts...)
				},
			})
			sc := &atev1alpha1.SandboxConfig{}
			if err := c.Get(t.Context(), client.ObjectKeyFromObject(tc.sc), sc); err != nil {
				t.Fatalf("get SandboxConfig: %v", err)
			}
			if err := setProtectionFinalizer(t.Context(), c, sc, tc.protect); err != nil {
				t.Fatalf("setProtectionFinalizer: %v", err)
			}
			got := &atev1alpha1.SandboxConfig{}
			if err := c.Get(t.Context(), client.ObjectKeyFromObject(tc.sc), got); err != nil {
				t.Fatalf("get SandboxConfig: %v", err)
			}
			if has := controllerutil.ContainsFinalizer(got, atev1alpha1.SandboxConfigWorkerPoolProtectionFinalizer); has != tc.want {
				t.Errorf("has protection finalizer = %v, want %v", has, tc.want)
			}
			if patched := patches > 0; patched != tc.wantPatch {
				t.Errorf("patched %d times, want patched = %v", patches, tc.wantPatch)
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

// waitResolved waits for wp to report SandboxConfigResolved=True for its
// current generation with its sandbox class using config.
func waitResolved(t *testing.T, wp *atev1alpha1.WorkerPool, config string) *atev1alpha1.WorkerPool {
	t.Helper()
	return waitCondition(t, wp, func(current *atev1alpha1.WorkerPool, cond *metav1.Condition) bool {
		return cond.Status == metav1.ConditionTrue &&
			sandboxConfigInUse(current, current.Spec.DefaultSandboxClass()) == config
	})
}

// waitUnresolved waits for wp to report SandboxConfigResolved=False with
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
		cond := meta.FindStatusCondition(current.Status.Conditions, atev1alpha1.WorkerPoolConditionSandboxConfigResolved)
		return cond != nil && cond.ObservedGeneration == current.Generation && ok(current, cond), nil
	})
	return current
}

// waitProtectionFinalizer waits for the SandboxConfig name to carry, or not
// carry, the protection finalizer.
func waitProtectionFinalizer(t *testing.T, name string, want bool) {
	t.Helper()
	eventually(t, func(ctx context.Context) (bool, error) {
		sc := &atev1alpha1.SandboxConfig{}
		if err := k8sClient.Get(ctx, types.NamespacedName{Name: name}, sc); err != nil {
			return false, nil
		}
		return controllerutil.ContainsFinalizer(sc, atev1alpha1.SandboxConfigWorkerPoolProtectionFinalizer) == want, nil
	})
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

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
	"fmt"
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
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	atev1 "go.ate.dev/substrate/api/infra/k8s/ate/v1"
)

// testGvisorDefault is the gvisor class default TestMain creates.
const testGvisorDefault = "gvisor-default-test"

// testHoldFinalizer keeps a SandboxConfig on the API server after it is
// deleted, so a test can observe how a config that is being deleted is treated.
const testHoldFinalizer = "test.ate.dev/hold"

func TestResolveSandboxConfig(t *testing.T) {
	t.Parallel()
	t0 := metav1.NewTime(time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC))
	t1 := metav1.NewTime(t0.Add(time.Minute))
	sc := func(name string, class atev1.SandboxClass, isDefault bool, created metav1.Time, mutate ...func(*atev1.SandboxConfig)) atev1.SandboxConfig {
		c := *makeSandboxConfig(name, class, isDefault)
		c.CreationTimestamp = created
		for _, m := range mutate {
			m(&c)
		}
		return c
	}
	deleting := func(c *atev1.SandboxConfig) { c.DeletionTimestamp = &t1 }
	protected := func(c *atev1.SandboxConfig) {
		controllerutil.AddFinalizer(c, atev1.SandboxConfigWorkerPoolProtectionFinalizer)
	}
	gvisor := func(ref string) atev1.WorkerPoolSandboxClass {
		e := atev1.WorkerPoolSandboxClass{Name: atev1.SandboxClassGvisor}
		if ref != "" {
			e.ConfigRef = &atev1.SandboxConfigReference{Name: ref}
		}
		return e
	}

	gv := atev1.SandboxClassGvisor

	tests := []struct {
		name    string
		entry   atev1.WorkerPoolSandboxClass
		inUse   string
		configs []atev1.SandboxConfig
		// want is the config the class uses, or "" for none.
		want string
		// wantReason is the False condition's reason, or "" for True.
		wantReason  string
		wantMessage string
	}{
		{
			name:        "explicit ref resolves even when another config is the default",
			entry:       gvisor("custom"),
			configs:     []atev1.SandboxConfig{sc("custom", gv, false, t0), sc("def", gv, true, t1)},
			want:        "custom",
			wantMessage: `resolved to SandboxConfig "custom"`,
		},
		{
			name:        "explicit ref not found",
			entry:       gvisor("missing"),
			configs:     []atev1.SandboxConfig{sc("def", gv, true, t0)},
			wantReason:  atev1.WorkerPoolReasonSandboxConfigNotFound,
			wantMessage: `SandboxConfig "missing" not found`,
		},
		{
			name:        "explicit ref not found keeps the config in use",
			entry:       gvisor("missing"),
			inUse:       "old",
			configs:     []atev1.SandboxConfig{sc("old", gv, false, t0)},
			want:        "old",
			wantReason:  atev1.WorkerPoolReasonSandboxConfigNotFound,
			wantMessage: `SandboxConfig "missing" not found; keeping "old"`,
		},
		{
			name:        "explicit ref of another class",
			entry:       gvisor("vm"),
			configs:     []atev1.SandboxConfig{sc("vm", atev1.SandboxClassMicroVM, false, t0)},
			wantReason:  atev1.WorkerPoolReasonSandboxClassMismatch,
			wantMessage: `SandboxConfig "vm" is for sandbox class "microvm", not "gvisor"`,
		},
		{
			name:        "explicit ref of another class keeps the config in use",
			entry:       gvisor("vm"),
			inUse:       "old",
			configs:     []atev1.SandboxConfig{sc("vm", atev1.SandboxClassMicroVM, false, t0), sc("old", gv, false, t0)},
			want:        "old",
			wantReason:  atev1.WorkerPoolReasonSandboxClassMismatch,
			wantMessage: `SandboxConfig "vm" is for sandbox class "microvm", not "gvisor"; keeping "old"`,
		},
		{
			name:        "explicit ref being deleted takes no new users",
			entry:       gvisor("dying"),
			configs:     []atev1.SandboxConfig{sc("dying", gv, false, t0, deleting)},
			wantReason:  atev1.WorkerPoolReasonSandboxConfigDeleting,
			wantMessage: `SandboxConfig "dying" is being deleted`,
		},
		{
			name:        "explicit ref being deleted is kept but reported by a pool already using it",
			entry:       gvisor("dying"),
			inUse:       "dying",
			configs:     []atev1.SandboxConfig{sc("dying", gv, false, t0, deleting, protected)},
			want:        "dying",
			wantReason:  atev1.WorkerPoolReasonSandboxConfigDeleting,
			wantMessage: `SandboxConfig "dying" is being deleted; keeping "dying"`,
		},
		{
			name:        "explicit ref being deleted and released is not kept by a pool using it",
			entry:       gvisor("dying"),
			inUse:       "dying",
			configs:     []atev1.SandboxConfig{sc("dying", gv, false, t0, deleting)},
			wantReason:  atev1.WorkerPoolReasonSandboxConfigDeleting,
			wantMessage: `SandboxConfig "dying" is being deleted`,
		},
		{
			name:        "explicit ref being deleted keeps the previous config in use",
			entry:       gvisor("dying"),
			inUse:       "old",
			configs:     []atev1.SandboxConfig{sc("dying", gv, false, t0, deleting), sc("old", gv, false, t0)},
			want:        "old",
			wantReason:  atev1.WorkerPoolReasonSandboxConfigDeleting,
			wantMessage: `SandboxConfig "dying" is being deleted; keeping "old"`,
		},
		{
			name:        "no default",
			entry:       gvisor(""),
			configs:     []atev1.SandboxConfig{sc("plain", gv, false, t0)},
			wantReason:  atev1.WorkerPoolReasonDefaultConfigNotFound,
			wantMessage: `no SandboxConfig of sandbox class "gvisor" is annotated sandboxconfig.ate.dev/is-class-default=true`,
		},
		{
			name:        "default of another class is ignored",
			entry:       gvisor(""),
			configs:     []atev1.SandboxConfig{sc("vm", atev1.SandboxClassMicroVM, true, t0)},
			wantReason:  atev1.WorkerPoolReasonDefaultConfigNotFound,
			wantMessage: `no SandboxConfig of sandbox class "gvisor" is annotated sandboxconfig.ate.dev/is-class-default=true`,
		},
		{
			name:  "annotation must be exactly true",
			entry: gvisor(""),
			configs: []atev1.SandboxConfig{sc("no", gv, false, t0, func(c *atev1.SandboxConfig) {
				c.Annotations = map[string]string{atev1.SandboxConfigClassDefaultAnnotation: "True"}
			})},
			wantReason:  atev1.WorkerPoolReasonDefaultConfigNotFound,
			wantMessage: `no SandboxConfig of sandbox class "gvisor" is annotated sandboxconfig.ate.dev/is-class-default=true`,
		},
		{
			name:        "default being deleted takes no new users",
			entry:       gvisor(""),
			configs:     []atev1.SandboxConfig{sc("dying", gv, true, t0, deleting)},
			wantReason:  atev1.WorkerPoolReasonSandboxConfigDeleting,
			wantMessage: `the default SandboxConfig for sandbox class "gvisor" is being deleted`,
		},
		{
			name:        "several defaults being deleted are reported",
			entry:       gvisor(""),
			configs:     []atev1.SandboxConfig{sc("dying-new", gv, true, t1, deleting), sc("dying-old", gv, true, t0, deleting)},
			wantReason:  atev1.WorkerPoolReasonSandboxConfigDeleting,
			wantMessage: `the default SandboxConfig for sandbox class "gvisor" is being deleted`,
		},
		{
			name:        "default being deleted is kept but reported by a pool already using it",
			entry:       gvisor(""),
			inUse:       "dying",
			configs:     []atev1.SandboxConfig{sc("dying", gv, true, t0, deleting, protected)},
			want:        "dying",
			wantReason:  atev1.WorkerPoolReasonSandboxConfigDeleting,
			wantMessage: `the default SandboxConfig for sandbox class "gvisor" is being deleted; keeping "dying"`,
		},
		{
			name:        "default being deleted and released is not kept by a pool using it",
			entry:       gvisor(""),
			inUse:       "dying",
			configs:     []atev1.SandboxConfig{sc("dying", gv, true, t0, deleting)},
			wantReason:  atev1.WorkerPoolReasonSandboxConfigDeleting,
			wantMessage: `the default SandboxConfig for sandbox class "gvisor" is being deleted`,
		},
		{
			name:        "a live default replaces the deleting one in use, even when older",
			entry:       gvisor(""),
			inUse:       "dying",
			configs:     []atev1.SandboxConfig{sc("dying", gv, true, t1, deleting), sc("old", gv, true, t0)},
			want:        "old",
			wantMessage: `resolved to SandboxConfig "old"`,
		},
		{
			name:        "config in use that lost the default annotation is kept but reported",
			entry:       gvisor(""),
			inUse:       "plain",
			configs:     []atev1.SandboxConfig{sc("plain", gv, false, t0)},
			want:        "plain",
			wantReason:  atev1.WorkerPoolReasonDefaultConfigNotFound,
			wantMessage: `no SandboxConfig of sandbox class "gvisor" is annotated sandboxconfig.ate.dev/is-class-default=true; keeping "plain"`,
		},
		{
			name:        "config in use that no longer exists is not kept",
			entry:       gvisor(""),
			inUse:       "gone",
			configs:     []atev1.SandboxConfig{sc("plain", gv, false, t0)},
			wantReason:  atev1.WorkerPoolReasonDefaultConfigNotFound,
			wantMessage: `no SandboxConfig of sandbox class "gvisor" is annotated sandboxconfig.ate.dev/is-class-default=true`,
		},
		{
			name:        "config in use of another class is not kept",
			entry:       gvisor(""),
			inUse:       "vm",
			configs:     []atev1.SandboxConfig{sc("vm", atev1.SandboxClassMicroVM, true, t0)},
			wantReason:  atev1.WorkerPoolReasonDefaultConfigNotFound,
			wantMessage: `no SandboxConfig of sandbox class "gvisor" is annotated sandboxconfig.ate.dev/is-class-default=true`,
		},
		{
			name:        "newest default wins",
			entry:       gvisor(""),
			configs:     []atev1.SandboxConfig{sc("a-old", gv, true, t0), sc("z-new", gv, true, t1), sc("plain", gv, false, t1)},
			want:        "z-new",
			wantMessage: `resolved to SandboxConfig "z-new"`,
		},
		{
			name:        "newest default wins regardless of list order",
			entry:       gvisor(""),
			configs:     []atev1.SandboxConfig{sc("z-new", gv, true, t1), sc("a-old", gv, true, t0)},
			want:        "z-new",
			wantMessage: `resolved to SandboxConfig "z-new"`,
		},
		{
			name:        "creation time tie goes to the first name",
			entry:       gvisor(""),
			configs:     []atev1.SandboxConfig{sc("b", gv, true, t0), sc("a", gv, true, t0)},
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
				Type:    atev1.WorkerPoolSandboxClassConditionResolved,
				Status:  metav1.ConditionTrue,
				Reason:  atev1.WorkerPoolReasonResolved,
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
	if err := atev1.AddToScheme(scheme); err != nil {
		t.Fatalf("add scheme: %v", err)
	}
	protected := func(sc *atev1.SandboxConfig) *atev1.SandboxConfig {
		controllerutil.AddFinalizer(sc, atev1.SandboxConfigWorkerPoolProtectionFinalizer)
		return sc
	}
	tests := []struct {
		name   string
		config *atev1.SandboxConfig
		// ref is the pool's configRef. "" follows the class default.
		ref string
		// wantResolved is whether the pool must end up using config.
		wantResolved bool
		// wantProtected is whether config must carry the finalizer afterwards.
		wantProtected bool
	}{
		{
			name:          "unprotected class default",
			config:        makeSandboxConfig("fresh-default", atev1.SandboxClassGvisor, true),
			wantResolved:  true,
			wantProtected: true,
		},
		{
			name:          "unprotected configRef target",
			config:        makeSandboxConfig("fresh-ref", atev1.SandboxClassGvisor, false),
			ref:           "fresh-ref",
			wantResolved:  true,
			wantProtected: true,
		},
		{
			name:          "already protected config",
			config:        protected(makeSandboxConfig("held", atev1.SandboxClassGvisor, true)),
			wantResolved:  true,
			wantProtected: true,
		},
		{
			name:          "unresolved pool protects nothing",
			config:        makeSandboxConfig("other-class", atev1.SandboxClassMicroVM, false),
			ref:           "other-class",
			wantResolved:  false,
			wantProtected: false,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			wp := makeWorkerPool("pool", "default", 1, "worker:v1")
			if tc.ref != "" {
				wp.Spec.SandboxClasses[0].ConfigRef = &atev1.SandboxConfigReference{Name: tc.ref}
			}
			// protectedAtWrite records whether config carried the finalizer
			// at the moment the pool's status was written.
			var wroteStatus, protectedAtWrite bool
			c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(wp, tc.config).WithStatusSubresource(wp).
				WithInterceptorFuncs(interceptor.Funcs{
					SubResourceUpdate: func(ctx context.Context, c client.Client, sub string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
						sc := &atev1.SandboxConfig{}
						if err := c.Get(ctx, types.NamespacedName{Name: tc.config.Name}, sc); err != nil {
							return err
						}
						wroteStatus = true
						protectedAtWrite = controllerutil.ContainsFinalizer(sc, atev1.SandboxConfigWorkerPoolProtectionFinalizer)
						return c.SubResource(sub).Update(ctx, obj, opts...)
					},
				}).Build()
			r := &WorkerPoolReconciler{Client: c, APIReader: c, Recorder: events.NewFakeRecorder(1)}
			if _, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(wp)}); err != nil {
				t.Fatalf("Reconcile: %v", err)
			}

			sc := &atev1.SandboxConfig{}
			if err := c.Get(t.Context(), types.NamespacedName{Name: tc.config.Name}, sc); err != nil {
				t.Fatalf("get SandboxConfig: %v", err)
			}
			if got := controllerutil.ContainsFinalizer(sc, atev1.SandboxConfigWorkerPoolProtectionFinalizer); got != tc.wantProtected {
				t.Errorf("config protected = %v, want %v", got, tc.wantProtected)
			}
			got := &atev1.WorkerPool{}
			if err := c.Get(t.Context(), client.ObjectKeyFromObject(wp), got); err != nil {
				t.Fatalf("get WorkerPool: %v", err)
			}
			if resolved := statusConfig(got, atev1.SandboxClassGvisor) == tc.config.Name; resolved != tc.wantResolved {
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

// TestReconcileConfigDeletedBeforeClaim verifies that a config deleted after
// the cached read the pool resolved from, but before the status write that
// claims it, never becomes the config in use: the protection reconciler runs
// while the claim is being written, as it can on a real cluster, and cannot
// see the pool yet. The read that confirms the switch finds the config gone
// or being deleted, so the class keeps the config it already uses, and the
// next reconcile, which the SandboxConfig watch triggers, settles on the
// older default.
func TestReconcileConfigDeletedBeforeClaim(t *testing.T) {
	t.Parallel()
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatalf("add scheme: %v", err)
	}
	if err := atev1.AddToScheme(scheme); err != nil {
		t.Fatalf("add scheme: %v", err)
	}
	t0 := metav1.NewTime(time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC))
	// newDefault is the newest gvisor default, so the pool resolves to it
	// first. oldDefault is the one it must fall back to.
	config := func(name string, created metav1.Time, finalizers ...string) *atev1.SandboxConfig {
		sc := makeSandboxConfig(name, atev1.SandboxClassGvisor, true)
		sc.CreationTimestamp = created
		sc.Finalizers = finalizers
		return sc
	}
	using := func(name string) []atev1.WorkerPoolSandboxClassStatus {
		return []atev1.WorkerPoolSandboxClassStatus{{
			Name:      atev1.SandboxClassGvisor,
			ConfigRef: &atev1.SandboxConfigReference{Name: name},
		}}
	}

	tests := []struct {
		name string
		// newDefault is the config the pool resolves to from the cache.
		newDefault *atev1.SandboxConfig
		// pinned sets the pool's configRef to newDefault.
		pinned bool
		// status is the pool's status before the reconcile.
		status []atev1.WorkerPoolSandboxClassStatus
		// deletedBefore marks newDefault for deletion before the reconcile.
		deletedBefore bool
		// deleteDuringWrite deletes newDefault just before the first status
		// write. release then runs the protection reconciler on it.
		deleteDuringWrite bool
		release           bool
		// wantFirst and want are the configs status must name after the
		// first and the second reconcile.
		wantFirst string
		want      string
	}{
		{
			name:       "not deleted: switched",
			newDefault: config("new-default", metav1.NewTime(t0.Add(time.Minute))),
			wantFirst:  "new-default",
			want:       "new-default",
		},
		{
			name:              "deleted and released: never named, falls back on the next reconcile",
			newDefault:        config("new-default", metav1.NewTime(t0.Add(time.Minute))),
			deleteDuringWrite: true,
			release:           true,
			wantFirst:         "",
			want:              "old-default",
		},
		{
			name:              "deleted, released, held by another finalizer: never named, falls back on the next reconcile",
			newDefault:        config("new-default", metav1.NewTime(t0.Add(time.Minute)), testHoldFinalizer),
			deleteDuringWrite: true,
			release:           true,
			wantFirst:         "",
			want:              "old-default",
		},
		{
			name:              "deleted, not yet released: never named, falls back on the next reconcile",
			newDefault:        config("new-default", metav1.NewTime(t0.Add(time.Minute))),
			deleteDuringWrite: true,
			wantFirst:         "",
			want:              "old-default",
		},
		{
			name:              "deleted and released while switching away from a config: keeps it",
			newDefault:        config("new-default", metav1.NewTime(t0.Add(time.Minute))),
			status:            using("old-default"),
			deleteDuringWrite: true,
			release:           true,
			wantFirst:         "old-default",
			want:              "old-default",
		},
		{
			name:              "deleted, not yet released, while switching away from a config: keeps it",
			newDefault:        config("new-default", metav1.NewTime(t0.Add(time.Minute))),
			status:            using("old-default"),
			deleteDuringWrite: true,
			wantFirst:         "old-default",
			want:              "old-default",
		},
		{
			name:          "pinned, named in status but released: dropped",
			newDefault:    config("new-default", metav1.NewTime(t0.Add(time.Minute)), testHoldFinalizer),
			pinned:        true,
			status:        using("new-default"),
			deletedBefore: true,
			wantFirst:     "",
			want:          "",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			wp := makeWorkerPool("pool", "default", 1, "worker:v1")
			wp.Status.SandboxClasses = tc.status
			if tc.pinned {
				wp.Spec.SandboxClasses[0].ConfigRef = &atev1.SandboxConfigReference{Name: tc.newDefault.Name}
			}
			newDefault := tc.newDefault.DeepCopy()
			if tc.deletedBefore {
				newDefault.DeletionTimestamp = &t0
			}
			deleted := false
			c := fake.NewClientBuilder().WithScheme(scheme).
				WithObjects(wp, newDefault, config("old-default", t0)).
				WithStatusSubresource(wp).
				WithIndex(&atev1.WorkerPool{}, workerPoolSandboxConfigIndex, func(obj client.Object) []string {
					return sandboxConfigsInUse(obj.(*atev1.WorkerPool))
				}).
				WithInterceptorFuncs(interceptor.Funcs{
					SubResourceUpdate: func(ctx context.Context, c client.Client, sub string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
						if tc.deleteDuringWrite && !deleted {
							deleted = true
							if err := c.Delete(ctx, newDefault.DeepCopy()); err != nil {
								return err
							}
							if tc.release {
								p := &SandboxConfigProtectionReconciler{Client: c, APIReader: c}
								if _, err := p.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(newDefault)}); err != nil {
									return err
								}
							}
						}
						return c.SubResource(sub).Update(ctx, obj, opts...)
					},
				}).Build()
			r := &WorkerPoolReconciler{Client: c, APIReader: c, Recorder: events.NewFakeRecorder(10)}
			for i, want := range []string{tc.wantFirst, tc.want} {
				if _, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(wp)}); err != nil {
					t.Fatalf("Reconcile %d: %v", i+1, err)
				}
				if tc.deleteDuringWrite && !deleted {
					t.Fatalf("status was never written, so nothing was deleted")
				}
				got := &atev1.WorkerPool{}
				if err := c.Get(t.Context(), client.ObjectKeyFromObject(wp), got); err != nil {
					t.Fatalf("get WorkerPool: %v", err)
				}
				if name := statusConfig(got, atev1.SandboxClassGvisor); name != want {
					t.Errorf("after reconcile %d, status names %q, want %q", i+1, name, want)
				}
				for _, class := range got.Status.SandboxClasses {
					if class.PendingConfigRef != nil {
						t.Errorf("after reconcile %d, class %q still claims %q", i+1, class.Name, class.PendingConfigRef.Name)
					}
				}
			}
		})
	}
}

// TestSandboxConfigReleasedWhenUnused verifies that a SandboxConfig no pool
// uses deletes without waiting: one no pool ever used carries no protection
// finalizer at all, and one a pool switched away from is released. The pool
// resolves to another config first, which shows both controllers have been
// running while the released config existed.
func TestSandboxConfigReleasedWhenUnused(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		// configs are created before the pool.
		configs []string
		pool    string
		// ref is the pool's first configRef.
		ref string
		// switchTo, if set, is the configRef the pool moves to.
		switchTo string
		// release is the config that must delete at once.
		release string
	}{
		{
			name:    "never used",
			configs: []string{"unused-sc", "unused-sc-other"},
			pool:    "test-unused-sc",
			ref:     "unused-sc-other",
			release: "unused-sc",
		},
		{
			name:     "pool switched away",
			configs:  []string{"switch-from", "switch-to"},
			pool:     "test-switch-ref",
			ref:      "switch-from",
			switchTo: "switch-to",
			release:  "switch-from",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			for _, name := range tc.configs {
				createSandboxConfig(t, makeSandboxConfig(name, atev1.SandboxClassGvisor, false))
			}
			wp := makeWorkerPool(tc.pool, "default", 1, "worker:v1")
			wp.Spec.SandboxClasses[0].ConfigRef = &atev1.SandboxConfigReference{Name: tc.ref}
			createWorkerPool(t, wp)
			waitResolvedAndDeployed(t, wp, tc.ref)

			if tc.switchTo != "" {
				updateWorkerPoolSpec(t, t.Context(), wp, "switch configRef", func(wp *atev1.WorkerPool) {
					wp.Spec.SandboxClasses[0].ConfigRef.Name = tc.switchTo
				})
				waitResolved(t, wp, tc.switchTo)
				waitProtectionFinalizer(t, tc.switchTo, true)
			}
			if tc.release != tc.ref {
				// Never used, so never protected.
				got := &atev1.SandboxConfig{}
				if err := k8sClient.Get(t.Context(), types.NamespacedName{Name: tc.release}, got); err != nil {
					t.Fatalf("get SandboxConfig: %v", err)
				}
				if controllerutil.ContainsFinalizer(got, atev1.SandboxConfigWorkerPoolProtectionFinalizer) {
					t.Fatalf("unused SandboxConfig %s carries the protection finalizer", tc.release)
				}
			}
			deleteAndWait(t, &atev1.SandboxConfig{ObjectMeta: metav1.ObjectMeta{Name: tc.release}})
		})
	}
}

// TestWorkerPoolConfigRef covers a pool with an explicit configRef. It
// resolves only when the named SandboxConfig exists, is of the pool's class,
// and is not being deleted. It then protects the config and gets its
// Deployment. Otherwise it reports why, records no config in status, gets no
// Deployment, and emits a Warning event.
func TestWorkerPoolConfigRef(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		// config is created before the pool. nil creates none.
		config *atev1.SandboxConfig
		// deleting marks config for deletion before the pool is created.
		deleting bool
		ref      string
		// wantReason is the Resolved=False reason to expect, or
		// "" when the pool must resolve.
		wantReason string
		// late is created once the pool reports wantReason. The pool must
		// then resolve to it.
		late *atev1.SandboxConfig
	}{
		{
			name:   "existing config of the same class",
			config: makeSandboxConfig("ref-ok", atev1.SandboxClassGvisor, false),
			ref:    "ref-ok",
		},
		{
			name:       "missing config, created later",
			ref:        "ref-late",
			wantReason: atev1.WorkerPoolReasonSandboxConfigNotFound,
			late:       makeSandboxConfig("ref-late", atev1.SandboxClassGvisor, false),
		},
		{
			name:       "config of another class",
			config:     makeSandboxConfig("ref-vm", atev1.SandboxClassMicroVM, false),
			ref:        "ref-vm",
			wantReason: atev1.WorkerPoolReasonSandboxClassMismatch,
		},
		{
			name:       "config being deleted",
			config:     makeSandboxConfig("ref-dying", atev1.SandboxClassGvisor, false),
			deleting:   true,
			ref:        "ref-dying",
			wantReason: atev1.WorkerPoolReasonSandboxConfigDeleting,
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
			wp := makeWorkerPool("test-configref-"+tc.ref, "default", 1, "worker:v1")
			wp.Spec.SandboxClasses[0].ConfigRef = &atev1.SandboxConfigReference{Name: tc.ref}
			createWorkerPool(t, wp)

			if tc.wantReason == "" {
				waitResolvedAndDeployed(t, wp, tc.ref)
				return
			}
			current := waitUnresolved(t, wp, tc.wantReason)
			if got := statusConfig(current, atev1.SandboxClassGvisor); got != "" {
				t.Errorf("status.sandboxClasses names %q, want no config for a pool that never resolved", got)
			}
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
func waitResolvedAndDeployed(t *testing.T, wp *atev1.WorkerPool, config string) {
	t.Helper()
	waitResolved(t, wp, config)
	waitProtectionFinalizer(t, config, true)
	eventually(t, func(ctx context.Context) (bool, error) {
		_, err := getDeployment(ctx, wp)
		return err == nil, nil
	})
}

// waitWarningEvent waits for a Warning event with reason about wp.
func waitWarningEvent(t *testing.T, wp *atev1.WorkerPool, reason string) {
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

// TestWorkerPoolUnresolvedKeepsConfigInUse verifies that a pool whose sandbox
// class stops resolving keeps the config it uses: it reports why with a
// Warning event, keeps the config in status and protected, and the controller
// keeps updating its Deployment.
func TestWorkerPoolUnresolvedKeepsConfigInUse(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		pool string
		// config is created with testHoldFinalizer and named by the pool's
		// configRef. nil leaves the pool on the class default.
		config *atev1.SandboxConfig
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
			wantReason: atev1.WorkerPoolReasonSandboxConfigNotFound,
		},
		{
			name:       "config in use marked for deletion",
			pool:       "test-ref-deleting-in-use",
			config:     makeSandboxConfig("dying-in-use", atev1.SandboxClassGvisor, false),
			replicas:   3,
			wantReason: atev1.WorkerPoolReasonSandboxConfigDeleting,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx := t.Context()
			wp := makeWorkerPool(tc.pool, "default", 1, "worker:v1")
			inUse := testGvisorDefault
			if tc.config != nil {
				controllerutil.AddFinalizer(tc.config, testHoldFinalizer)
				createSandboxConfig(t, tc.config)
				t.Cleanup(func() { removeFinalizer(t, tc.config.Name, testHoldFinalizer) })
				inUse = tc.config.Name
				wp.Spec.SandboxClasses[0].ConfigRef = &atev1.SandboxConfigReference{Name: inUse}
			}
			createWorkerPool(t, wp)
			waitResolvedAndDeployed(t, wp, inUse)

			if tc.missingRef != "" {
				updateWorkerPoolSpec(t, ctx, wp, "point WorkerPool at a missing SandboxConfig and scale it", func(wp *atev1.WorkerPool) {
					wp.Spec.Replicas = tc.replicas
					wp.Spec.SandboxClasses[0].ConfigRef = &atev1.SandboxConfigReference{Name: tc.missingRef}
				})
			} else if err := k8sClient.Delete(ctx, tc.config); err != nil {
				t.Fatalf("delete SandboxConfig: %v", err)
			}
			current := waitUnresolved(t, wp, tc.wantReason)
			if got := statusConfig(current, atev1.SandboxClassGvisor); got != inUse {
				t.Errorf("status.sandboxClasses names %q, want the config in use %q", got, inUse)
			}
			waitWarningEvent(t, wp, tc.wantReason)
			waitProtectionFinalizer(t, inUse, true)

			if tc.missingRef == "" {
				updateWorkerPoolSpec(t, ctx, wp, "scale WorkerPool", func(wp *atev1.WorkerPool) {
					wp.Spec.Replicas = tc.replicas
				})
			}
			waitDeploymentReplicas(t, wp, tc.replicas)
		})
	}
}

// waitDeploymentReplicas waits for wp's Deployment to ask for replicas pods.
func waitDeploymentReplicas(t *testing.T, wp *atev1.WorkerPool, replicas int32) {
	t.Helper()
	eventually(t, func(ctx context.Context) (bool, error) {
		dep, err := getDeployment(ctx, wp)
		if err != nil {
			return false, nil
		}
		return dep.Spec.Replicas != nil && *dep.Spec.Replicas == replicas, nil
	})
}

// TestSandboxConfigInUseProtection verifies that a SandboxConfig shared by
// several pools survives a delete until all of them are gone, and that the
// pools themselves delete without waiting.
//
// A sentinel pool uses a sentinel config that is deleted at the same time as
// the shared one. The pool deletions reach the protection reconciler through
// the same WorkerPool watch, in order, and it reconciles one config at a
// time, so once the sentinel is gone the shared config has been reconciled
// for every user deleted before the sentinel pool and found still in use.
func TestSandboxConfigInUseProtection(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		config string
		// users are the pools sharing config, deleted in order.
		users        []string
		sentinel     string
		sentinelPool string
	}{
		{
			name:         "two pools",
			config:       "shared-sc",
			users:        []string{"test-shared-a", "test-shared-b"},
			sentinel:     "shared-sentinel",
			sentinelPool: "test-shared-sentinel",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx := t.Context()
			sc := makeSandboxConfig(tc.config, atev1.SandboxClassGvisor, false)
			createSandboxConfig(t, sc)
			sentinel := makeSandboxConfig(tc.sentinel, atev1.SandboxClassGvisor, false)
			createSandboxConfig(t, sentinel)
			newPool := func(name, config string) *atev1.WorkerPool {
				wp := makeWorkerPool(name, "default", 1, "worker:v1")
				wp.Spec.SandboxClasses[0].ConfigRef = &atev1.SandboxConfigReference{Name: config}
				createWorkerPool(t, wp)
				waitResolved(t, wp, config)
				return wp
			}
			var users []*atev1.WorkerPool
			for _, name := range tc.users {
				users = append(users, newPool(name, sc.Name))
			}
			sentinelPool := newPool(tc.sentinelPool, sentinel.Name)
			waitProtectionFinalizer(t, sc.Name, true)
			waitProtectionFinalizer(t, sentinel.Name, true)

			for _, c := range []*atev1.SandboxConfig{sc, sentinel} {
				if err := k8sClient.Delete(ctx, c); err != nil {
					t.Fatalf("delete SandboxConfig %s: %v", c.Name, err)
				}
			}
			last := users[len(users)-1]
			for _, wp := range users[:len(users)-1] {
				deleteAndWait(t, wp)
			}
			deleteAndWait(t, sentinelPool)
			waitGone(t, sentinel)
			// The shared config has been reconciled for every other user's
			// deletion. It must stay for the last one.
			got := &atev1.SandboxConfig{}
			if err := k8sClient.Get(ctx, types.NamespacedName{Name: sc.Name}, got); err != nil {
				t.Fatalf("SandboxConfig still used by %s: get: %v", last.Name, err)
			}
			if got.DeletionTimestamp.IsZero() {
				t.Fatalf("SandboxConfig has no deletionTimestamp after delete")
			}

			deleteAndWait(t, last)
			waitGone(t, sc)
		})
	}
}

// TestWorkerPoolDefaultNewestWins verifies that a pool without a configRef
// follows the newest class default, and keeps the config it uses, while
// reporting why, once no default is left. Each step changes the class
// defaults and checks the pool's resolution afterwards. It is not parallel
// because it owns the microvm class default.
func TestWorkerPoolDefaultNewestWins(t *testing.T) {
	mvdefA := makeSandboxConfig("mvdef-a", atev1.SandboxClassMicroVM, true)
	mvdefB := makeSandboxConfig("mvdef-b", atev1.SandboxClassMicroVM, true)
	// The steps create the configs, but they outlive each step.
	deleteOnCleanup(t, mvdefA)
	deleteOnCleanup(t, mvdefB)
	create := func(t *testing.T, sc *atev1.SandboxConfig) {
		t.Helper()
		if err := k8sClient.Create(t.Context(), sc); err != nil {
			t.Fatalf("create SandboxConfig %s: %v", sc.Name, err)
		}
	}
	wp := makeWorkerPool("test-default-newest", "default", 1, "worker:v1")
	wp.Spec.SandboxClasses[0].Name = atev1.SandboxClassMicroVM
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
			wantReason: atev1.WorkerPoolReasonDefaultConfigNotFound,
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
			name: "the only default being deleted is kept, held by its finalizer",
			act: func(t *testing.T) {
				if err := k8sClient.Delete(t.Context(), mvdefB); err != nil {
					t.Fatalf("delete SandboxConfig mvdef-b: %v", err)
				}
			},
			wantConfig: "mvdef-b",
			wantReason: atev1.WorkerPoolReasonSandboxConfigDeleting,
		},
		{
			name: "no default left: the config in use is kept",
			act: func(t *testing.T) {
				err := retry.RetryOnConflict(retry.DefaultBackoff, func() error {
					sc := &atev1.SandboxConfig{}
					if err := k8sClient.Get(t.Context(), types.NamespacedName{Name: "mvdef-b"}, sc); err != nil {
						return err
					}
					delete(sc.Annotations, atev1.SandboxConfigClassDefaultAnnotation)
					return k8sClient.Update(t.Context(), sc)
				})
				if err != nil {
					t.Fatalf("remove class default annotation from mvdef-b: %v", err)
				}
			},
			wantConfig: "mvdef-b",
			wantReason: atev1.WorkerPoolReasonDefaultConfigNotFound,
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
			if got := statusConfig(current, atev1.SandboxClassMicroVM); got != step.wantConfig {
				t.Errorf("status.sandboxClasses names %q, want %q", got, step.wantConfig)
			}
			waitWarningEvent(t, wp, step.wantReason)
			if step.wantConfig == "" {
				assertNoDeployment(t, wp)
			}
		}) {
			return
		}
	}

	// Once the pool is gone, nothing holds mvdef-b.
	deleteAndWait(t, wp)
	waitGone(t, mvdefB)
}

// TestWorkerPoolMayUseSandboxConfig covers the ways a SandboxConfig
// event can concern a WorkerPool.
func TestWorkerPoolMayUseSandboxConfig(t *testing.T) {
	t.Parallel()
	sc := makeSandboxConfig("sc", atev1.SandboxClassGvisor, false)
	gvisor := atev1.WorkerPoolSandboxClass{Name: atev1.SandboxClassGvisor}
	microvm := atev1.WorkerPoolSandboxClass{Name: atev1.SandboxClassMicroVM}
	withRef := func(e atev1.WorkerPoolSandboxClass, name string) atev1.WorkerPoolSandboxClass {
		e.ConfigRef = &atev1.SandboxConfigReference{Name: name}
		return e
	}
	using := func(name string) atev1.WorkerPoolSandboxClassStatus {
		return atev1.WorkerPoolSandboxClassStatus{Name: atev1.SandboxClassMicroVM, ConfigRef: &atev1.SandboxConfigReference{Name: name}}
	}

	tests := []struct {
		name   string
		spec   []atev1.WorkerPoolSandboxClass
		status []atev1.WorkerPoolSandboxClassStatus
		want   bool
	}{
		{
			name: "runs the config's class",
			spec: []atev1.WorkerPoolSandboxClass{gvisor},
			want: true,
		},
		{
			name: "runs the config's class with another ref",
			spec: []atev1.WorkerPoolSandboxClass{withRef(gvisor, "other")},
			want: true,
		},
		{
			name: "spec names the config",
			spec: []atev1.WorkerPoolSandboxClass{withRef(microvm, "sc")},
			want: true,
		},
		{
			name:   "status names the config",
			spec:   []atev1.WorkerPoolSandboxClass{microvm},
			status: []atev1.WorkerPoolSandboxClassStatus{using("sc")},
			want:   true,
		},
		{
			name: "status claims the config",
			spec: []atev1.WorkerPoolSandboxClass{microvm},
			status: []atev1.WorkerPoolSandboxClassStatus{{
				Name:             atev1.SandboxClassMicroVM,
				ConfigRef:        &atev1.SandboxConfigReference{Name: "other"},
				PendingConfigRef: &atev1.SandboxConfigReference{Name: "sc"},
			}},
			want: true,
		},
		{
			name:   "another class, another config",
			spec:   []atev1.WorkerPoolSandboxClass{withRef(microvm, "other")},
			status: []atev1.WorkerPoolSandboxClassStatus{using("other")},
			want:   false,
		},
		{
			name:   "another class, no config in use",
			spec:   []atev1.WorkerPoolSandboxClass{withRef(microvm, "other")},
			status: []atev1.WorkerPoolSandboxClassStatus{{Name: atev1.SandboxClassMicroVM}},
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
	if err := atev1.AddToScheme(scheme); err != nil {
		t.Fatalf("add scheme: %v", err)
	}
	gvisor := []atev1.WorkerPoolSandboxClass{{Name: atev1.SandboxClassGvisor}}
	microvm := []atev1.WorkerPoolSandboxClass{{Name: atev1.SandboxClassMicroVM}}
	microvmRef := []atev1.WorkerPoolSandboxClass{{Name: atev1.SandboxClassMicroVM, ConfigRef: &atev1.SandboxConfigReference{Name: "sc"}}}
	usingSC := []atev1.WorkerPoolSandboxClassStatus{{Name: atev1.SandboxClassMicroVM, ConfigRef: &atev1.SandboxConfigReference{Name: "sc"}}}
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
			obj:  makeSandboxConfig("sc", atev1.SandboxClassGvisor, false),
			want: []reconcile.Request{req("a", "gv"), req("b", "vm-ref"), req("b", "vm-status")},
		},
		{
			name: "microvm config: every microvm pool",
			obj:  makeSandboxConfig("other", atev1.SandboxClassMicroVM, false),
			want: []reconcile.Request{req("b", "vm"), req("b", "vm-ref"), req("b", "vm-status")},
		},
		{
			name: "not a SandboxConfig",
			obj:  makeWorkerPoolClasses("a", "gv", gvisor, nil),
		},
		{
			name:    "list fails: nothing",
			obj:     makeSandboxConfig("sc", atev1.SandboxClassGvisor, false),
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

// TestRecordSandboxConfigEvents verifies the events a reconcile emits per
// class: a Normal event each time the config in use changes, naming the old
// and new config, and a Warning event for a failure, reported once and again
// only when its reason or message changes or the class resolved in between.
// A class whose config and failure are as before emits nothing.
func TestRecordSandboxConfigEvents(t *testing.T) {
	t.Parallel()
	failed := metav1.Condition{
		Type:    atev1.WorkerPoolSandboxClassConditionResolved,
		Status:  metav1.ConditionFalse,
		Reason:  atev1.WorkerPoolReasonSandboxConfigNotFound,
		Message: `SandboxConfig "x" not found`,
	}
	resolved := func(config string) metav1.Condition {
		return metav1.Condition{
			Type:    atev1.WorkerPoolSandboxClassConditionResolved,
			Status:  metav1.ConditionTrue,
			Reason:  atev1.WorkerPoolReasonResolved,
			Message: fmt.Sprintf("resolved to SandboxConfig %q", config),
		}
	}
	// class builds a gvisor status entry using config ("" for none) with
	// cond.
	class := func(config string, cond metav1.Condition) []atev1.WorkerPoolSandboxClassStatus {
		status := atev1.WorkerPoolSandboxClassStatus{Name: atev1.SandboxClassGvisor, Conditions: []metav1.Condition{cond}}
		if config != "" {
			status.ConfigRef = &atev1.SandboxConfigReference{Name: config}
		}
		return []atev1.WorkerPoolSandboxClassStatus{status}
	}
	with := func(reason, message string) metav1.Condition {
		c := failed
		c.Reason = reason
		c.Message = message
		return c
	}
	warning := "Warning " + failed.Reason + " " + failed.Message

	tests := []struct {
		name string
		// prev is the pool's status before the reconcile.
		prev    []atev1.WorkerPoolSandboxClassStatus
		classes []atev1.WorkerPoolSandboxClassStatus
		want    []string
	}{
		{
			name:    "first resolve",
			classes: class("x", resolved("x")),
			want:    []string{`Normal SandboxConfigSwitched sandbox class "gvisor" uses SandboxConfig "x"`},
		},
		{
			name:    "first resolve after a failure",
			prev:    class("", failed),
			classes: class("x", resolved("x")),
			want:    []string{`Normal SandboxConfigSwitched sandbox class "gvisor" uses SandboxConfig "x"`},
		},
		{
			name:    "switch",
			prev:    class("x", resolved("x")),
			classes: class("y", resolved("y")),
			want:    []string{`Normal SandboxConfigSwitched sandbox class "gvisor" switched from SandboxConfig "x" to "y"`},
		},
		{
			name:    "same config: nothing",
			prev:    class("x", resolved("x")),
			classes: class("x", resolved("x")),
		},
		{
			name:    "resolves again to the kept config: nothing",
			prev:    class("x", failed),
			classes: class("x", resolved("x")),
		},
		{
			name:    "first failure",
			classes: class("", failed),
			want:    []string{warning},
		},
		{
			name:    "failure keeping the config: warning only",
			prev:    class("x", resolved("x")),
			classes: class("x", failed),
			want:    []string{warning},
		},
		{
			name:    "same failure already reported",
			prev:    class("", failed),
			classes: class("", failed),
		},
		{
			name:    "another reason",
			prev:    class("", with(atev1.WorkerPoolReasonSandboxClassMismatch, failed.Message)),
			classes: class("", failed),
			want:    []string{warning},
		},
		{
			name:    "another message",
			prev:    class("", with(failed.Reason, `SandboxConfig "y" not found`)),
			classes: class("", failed),
			want:    []string{warning},
		},
		{
			name:    "resolved in between",
			prev:    class("", resolved("x")),
			classes: class("", failed),
			want:    []string{warning},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			wp := makeWorkerPool("pool", "default", 1, "worker:v1")
			rec := events.NewFakeRecorder(2 * len(tc.classes))
			r := &WorkerPoolReconciler{Recorder: rec}
			r.recordSandboxConfigEvents(wp, tc.prev, tc.classes)
			var got []string
			for len(rec.Events) > 0 {
				got = append(got, <-rec.Events)
			}
			if !slices.Equal(got, tc.want) {
				t.Errorf("events = %q, want %q", got, tc.want)
			}
		})
	}
}

// TestReconcileReportsSwitchOnceWritten verifies that a switch's
// SandboxConfigSwitched event follows the status write that records it: a
// reconcile whose write is rejected emits nothing, and the switch is
// reported once when a later reconcile writes it.
func TestReconcileReportsSwitchOnceWritten(t *testing.T) {
	t.Parallel()
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatalf("add client-go scheme: %v", err)
	}
	if err := atev1.AddToScheme(scheme); err != nil {
		t.Fatalf("add scheme: %v", err)
	}
	tests := []struct {
		name string
		// rejected is how many status writes moving configRef to "y" are
		// rejected with a Conflict before one goes through.
		rejected int
	}{
		{name: "written at once"},
		{name: "written on the second try", rejected: 1},
		{name: "written on the third try", rejected: 2},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			wp := makeWorkerPoolClasses("default", "pool",
				[]atev1.WorkerPoolSandboxClass{{Name: atev1.SandboxClassGvisor, ConfigRef: &atev1.SandboxConfigReference{Name: "y"}}},
				[]atev1.WorkerPoolSandboxClassStatus{{Name: atev1.SandboxClassGvisor, ConfigRef: &atev1.SandboxConfigReference{Name: "x"}}})
			objs := []client.Object{wp,
				makeSandboxConfig("x", atev1.SandboxClassGvisor, false),
				makeSandboxConfig("y", atev1.SandboxClassGvisor, false),
			}
			rejected := 0
			c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).WithStatusSubresource(wp).
				WithInterceptorFuncs(interceptor.Funcs{
					SubResourceUpdate: func(ctx context.Context, c client.Client, sub string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
						if pool, ok := obj.(*atev1.WorkerPool); ok && rejected < tc.rejected && statusConfig(pool, atev1.SandboxClassGvisor) == "y" {
							rejected++
							return k8errors.NewConflict(atev1.GroupVersion.WithResource("workerpools").GroupResource(), pool.Name, errors.New("stale"))
						}
						return c.SubResource(sub).Update(ctx, obj, opts...)
					},
				}).Build()
			rec := events.NewFakeRecorder(10)
			r := &WorkerPoolReconciler{Client: c, APIReader: c, Recorder: rec}
			req := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(wp)}

			for i := 0; i < tc.rejected; i++ {
				if _, err := r.Reconcile(t.Context(), req); !k8errors.IsConflict(err) {
					t.Fatalf("Reconcile %d error = %v, want Conflict", i+1, err)
				}
				if len(rec.Events) != 0 {
					t.Fatalf("Reconcile %d emitted %q before status was written", i+1, <-rec.Events)
				}
			}
			if _, err := r.Reconcile(t.Context(), req); err != nil {
				t.Fatalf("Reconcile: %v", err)
			}
			var got []string
			for len(rec.Events) > 0 {
				got = append(got, <-rec.Events)
			}
			want := []string{`Normal SandboxConfigSwitched sandbox class "gvisor" switched from SandboxConfig "x" to "y"`}
			if !slices.Equal(got, want) {
				t.Errorf("events = %q, want %q", got, want)
			}
			current := &atev1.WorkerPool{}
			if err := c.Get(t.Context(), client.ObjectKeyFromObject(wp), current); err != nil {
				t.Fatalf("get WorkerPool: %v", err)
			}
			if name := statusConfig(current, atev1.SandboxClassGvisor); name != "y" {
				t.Errorf("status names %q, want %q", name, "y")
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
	configs := []atev1.SandboxConfig{*makeSandboxConfig("def", atev1.SandboxClassGvisor, true)}
	ref := func(name string) *atev1.SandboxConfigReference {
		if name == "" {
			return nil
		}
		return &atev1.SandboxConfigReference{Name: name}
	}
	cond := func(status metav1.ConditionStatus, reason, message string, gen int64, at metav1.Time) metav1.Condition {
		return metav1.Condition{
			Type:               atev1.WorkerPoolSandboxClassConditionResolved,
			Status:             status,
			Reason:             reason,
			Message:            message,
			ObservedGeneration: gen,
			LastTransitionTime: at,
		}
	}
	resolvedDef := func(gen int64, at metav1.Time) metav1.Condition {
		return cond(metav1.ConditionTrue, atev1.WorkerPoolReasonResolved, `resolved to SandboxConfig "def"`, gen, at)
	}
	notFound := func(message string, gen int64, at metav1.Time) metav1.Condition {
		return cond(metav1.ConditionFalse, atev1.WorkerPoolReasonSandboxConfigNotFound, message, gen, at)
	}
	// now stands for a lastTransitionTime set during the call.
	var now metav1.Time

	tests := []struct {
		name string
		// specRef is the pool's configRef. "" follows the class default.
		specRef string
		prev    []atev1.WorkerPoolSandboxClassStatus
		want    []atev1.WorkerPoolSandboxClassStatus
		// wantResolved is the second return value.
		wantResolved bool
	}{
		{
			name:         "first resolve",
			want:         []atev1.WorkerPoolSandboxClassStatus{{Name: atev1.SandboxClassGvisor, ConfigRef: ref("def"), Conditions: []metav1.Condition{resolvedDef(generation, now)}}},
			wantResolved: true,
		},
		{
			name:         "still resolved keeps lastTransitionTime",
			prev:         []atev1.WorkerPoolSandboxClassStatus{{Name: atev1.SandboxClassGvisor, ConfigRef: ref("def"), Conditions: []metav1.Condition{resolvedDef(generation-1, earlier)}}},
			want:         []atev1.WorkerPoolSandboxClassStatus{{Name: atev1.SandboxClassGvisor, ConfigRef: ref("def"), Conditions: []metav1.Condition{resolvedDef(generation, earlier)}}},
			wantResolved: true,
		},
		{
			name:    "never resolved records no config",
			specRef: "missing",
			want:    []atev1.WorkerPoolSandboxClassStatus{{Name: atev1.SandboxClassGvisor, Conditions: []metav1.Condition{notFound(`SandboxConfig "missing" not found`, generation, now)}}},
		},
		{
			name:    "stops resolving keeps the config and moves lastTransitionTime",
			specRef: "missing",
			prev:    []atev1.WorkerPoolSandboxClassStatus{{Name: atev1.SandboxClassGvisor, ConfigRef: ref("def"), Conditions: []metav1.Condition{resolvedDef(generation-1, earlier)}}},
			want:    []atev1.WorkerPoolSandboxClassStatus{{Name: atev1.SandboxClassGvisor, ConfigRef: ref("def"), Conditions: []metav1.Condition{notFound(`SandboxConfig "missing" not found; keeping "def"`, generation, now)}}},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			wp := makeWorkerPool("pool", "default", 1, "worker:v1")
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

// TestClaimSandboxConfigs verifies the status written to claim the configs a
// pool switches to: each class's status stays as it was, a switching class
// names its new config in pendingConfigRef, and nothing is written when no
// class switches.
func TestClaimSandboxConfigs(t *testing.T) {
	t.Parallel()
	ref := func(name string) *atev1.SandboxConfigReference {
		if name == "" {
			return nil
		}
		return &atev1.SandboxConfigReference{Name: name}
	}
	resolved := metav1.Condition{
		Type:   atev1.WorkerPoolSandboxClassConditionResolved,
		Status: metav1.ConditionTrue,
		Reason: atev1.WorkerPoolReasonResolved,
	}
	// status builds the pool's single gvisor status entry.
	status := func(configRef, pending string, conds ...metav1.Condition) []atev1.WorkerPoolSandboxClassStatus {
		return []atev1.WorkerPoolSandboxClassStatus{{
			Name:             atev1.SandboxClassGvisor,
			ConfigRef:        ref(configRef),
			PendingConfigRef: ref(pending),
			Conditions:       conds,
		}}
	}

	tests := []struct {
		name string
		prev []atev1.WorkerPoolSandboxClassStatus
		// classes is the status resolveSandboxClasses built.
		classes []atev1.WorkerPoolSandboxClassStatus
		want    []atev1.WorkerPoolSandboxClassStatus
	}{
		{
			name:    "same config: no claim",
			prev:    status("x", "", resolved),
			classes: status("x", ""),
			want:    nil,
		},
		{
			name:    "unresolved and nothing kept: no claim",
			classes: status("", ""),
			want:    nil,
		},
		{
			name:    "new pool: claims, no config in use",
			classes: status("x", ""),
			want:    status("", "x"),
		},
		{
			name:    "switch: claims and keeps the config in use",
			prev:    status("y", "", resolved),
			classes: status("x", ""),
			want:    status("y", "x", resolved),
		},
		{
			name:    "earlier claim for the same config: claimed again",
			prev:    status("y", "x", resolved),
			classes: status("x", ""),
			want:    status("y", "x", resolved),
		},
		{
			name:    "earlier claim for another config: replaced",
			prev:    status("y", "x", resolved),
			classes: status("z", ""),
			want:    status("y", "z", resolved),
		},
		{
			name:    "earlier claim, back to the config in use: no claim",
			prev:    status("y", "x", resolved),
			classes: status("y", ""),
			want:    nil,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			wp := makeWorkerPool("pool", "default", 1, "worker:v1")
			wp.Status.SandboxClasses = tc.prev
			if got := claimSandboxConfigs(wp, tc.classes); !equality.Semantic.DeepEqual(got, tc.want) {
				t.Errorf("claimSandboxConfigs = %+v, want %+v", got, tc.want)
			}
		})
	}
}

// TestConfirmSandboxConfigSwitch verifies how the config a class switches to,
// as read from the API server, settles the class: a live config of the class
// stands, and anything else keeps the config in use with a False Resolved
// condition.
func TestConfirmSandboxConfigSwitch(t *testing.T) {
	t.Parallel()
	const generation = 3
	now := metav1.Now()
	resolvedX := metav1.Condition{
		Type:               atev1.WorkerPoolSandboxClassConditionResolved,
		Status:             metav1.ConditionTrue,
		Reason:             atev1.WorkerPoolReasonResolved,
		Message:            `resolved to SandboxConfig "x"`,
		ObservedGeneration: generation,
	}
	// class is the entry resolveSandboxClasses built: switching to x.
	class := atev1.WorkerPoolSandboxClassStatus{
		Name:       atev1.SandboxClassGvisor,
		ConfigRef:  &atev1.SandboxConfigReference{Name: "x"},
		Conditions: []metav1.Condition{resolvedX},
	}
	onY := []atev1.WorkerPoolSandboxClassStatus{{
		Name:             atev1.SandboxClassGvisor,
		ConfigRef:        &atev1.SandboxConfigReference{Name: "y"},
		PendingConfigRef: &atev1.SandboxConfigReference{Name: "x"},
	}}
	x := makeSandboxConfig("x", atev1.SandboxClassGvisor, false)
	deletingX := x.DeepCopy()
	deletingX.DeletionTimestamp = &now
	configs := []atev1.SandboxConfig{*makeSandboxConfig("y", atev1.SandboxClassGvisor, false), *x}

	tests := []struct {
		name string
		prev []atev1.WorkerPoolSandboxClassStatus
		// live is x as read from the API server. nil means not found.
		live *atev1.SandboxConfig
		// wantConfig is the config the class must end up using.
		wantConfig  string
		wantReason  string
		wantMessage string
	}{
		{
			name:        "live: switches",
			prev:        onY,
			live:        x,
			wantConfig:  "x",
			wantReason:  atev1.WorkerPoolReasonResolved,
			wantMessage: `resolved to SandboxConfig "x"`,
		},
		{
			name:        "not found: keeps the config in use",
			prev:        onY,
			wantConfig:  "y",
			wantReason:  atev1.WorkerPoolReasonSandboxConfigNotFound,
			wantMessage: `SandboxConfig "x" not found; keeping "y"`,
		},
		{
			name:        "being deleted: keeps the config in use",
			prev:        onY,
			live:        deletingX,
			wantConfig:  "y",
			wantReason:  atev1.WorkerPoolReasonSandboxConfigDeleting,
			wantMessage: `SandboxConfig "x" is being deleted; keeping "y"`,
		},
		{
			name:        "now of another class: keeps the config in use",
			prev:        onY,
			live:        makeSandboxConfig("x", atev1.SandboxClassMicroVM, false),
			wantConfig:  "y",
			wantReason:  atev1.WorkerPoolReasonSandboxClassMismatch,
			wantMessage: `SandboxConfig "x" is for sandbox class "microvm", not "gvisor"; keeping "y"`,
		},
		{
			name:        "being deleted, new pool: no config",
			live:        deletingX,
			wantReason:  atev1.WorkerPoolReasonSandboxConfigDeleting,
			wantMessage: `SandboxConfig "x" is being deleted`,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			wp := makeWorkerPool("pool", "default", 1, "worker:v1")
			wp.Generation = generation
			wp.Status.SandboxClasses = tc.prev
			got := confirmSandboxConfigSwitch(wp, *class.DeepCopy(), tc.live, configs)
			if name := ptr.Deref(got.ConfigRef, atev1.SandboxConfigReference{}).Name; name != tc.wantConfig {
				t.Errorf("configRef = %q, want %q", name, tc.wantConfig)
			}
			if got.PendingConfigRef != nil {
				t.Errorf("pendingConfigRef = %q, want none", got.PendingConfigRef.Name)
			}
			cond := meta.FindStatusCondition(got.Conditions, atev1.WorkerPoolSandboxClassConditionResolved)
			if cond == nil {
				t.Fatalf("no Resolved condition in %+v", got.Conditions)
			}
			if cond.Reason != tc.wantReason || cond.Message != tc.wantMessage || cond.ObservedGeneration != generation {
				t.Errorf("condition = %s %q (generation %d), want %s %q (generation %d)",
					cond.Reason, cond.Message, cond.ObservedGeneration, tc.wantReason, tc.wantMessage, generation)
			}
		})
	}
}

// TestReconcileConfirmsSwitch verifies a switch against a cache that lags
// the API server: the cache shows the new config live, while the API server
// has it gone or being deleted. The pool's status must claim the new config
// before it is read from the API server, and the switch only stands if that
// read finds it live. Otherwise the pool keeps the config it uses.
func TestReconcileConfirmsSwitch(t *testing.T) {
	t.Parallel()
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatalf("add client-go scheme: %v", err)
	}
	if err := atev1.AddToScheme(scheme); err != nil {
		t.Fatalf("add scheme: %v", err)
	}
	now := metav1.Now()
	protected := func(sc *atev1.SandboxConfig) *atev1.SandboxConfig {
		controllerutil.AddFinalizer(sc, atev1.SandboxConfigWorkerPoolProtectionFinalizer)
		return sc
	}
	deleting := func(sc *atev1.SandboxConfig) *atev1.SandboxConfig {
		sc.DeletionTimestamp = &now
		return sc
	}
	x := func() *atev1.SandboxConfig {
		return protected(makeSandboxConfig("x", atev1.SandboxClassGvisor, false))
	}

	tests := []struct {
		name string
		// inUse is the config the pool's status uses before the reconcile.
		// "" for a new pool.
		inUse string
		// liveX is x on the API server. nil means it is gone.
		liveX *atev1.SandboxConfig
		// wantRead is whether x must be read from the API server.
		wantRead       bool
		wantConfig     string
		wantReason     string
		wantDeployment bool
	}{
		{
			name:           "live: switches",
			inUse:          "y",
			liveX:          x(),
			wantRead:       true,
			wantConfig:     "x",
			wantReason:     atev1.WorkerPoolReasonResolved,
			wantDeployment: true,
		},
		{
			name:           "being deleted: keeps the config in use",
			inUse:          "y",
			liveX:          deleting(x()),
			wantRead:       true,
			wantConfig:     "y",
			wantReason:     atev1.WorkerPoolReasonSandboxConfigDeleting,
			wantDeployment: true,
		},
		{
			name:           "gone: keeps the config in use",
			inUse:          "y",
			wantRead:       true,
			wantConfig:     "y",
			wantReason:     atev1.WorkerPoolReasonSandboxConfigNotFound,
			wantDeployment: true,
		},
		{
			name:           "live, new pool: switches and gets a Deployment",
			liveX:          x(),
			wantRead:       true,
			wantConfig:     "x",
			wantReason:     atev1.WorkerPoolReasonResolved,
			wantDeployment: true,
		},
		{
			name:       "being deleted, new pool: no config, no Deployment",
			liveX:      deleting(x()),
			wantRead:   true,
			wantReason: atev1.WorkerPoolReasonSandboxConfigDeleting,
		},
		{
			name:           "already in use: not read again",
			inUse:          "x",
			liveX:          deleting(x()),
			wantConfig:     "x",
			wantReason:     atev1.WorkerPoolReasonResolved,
			wantDeployment: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			wp := makeWorkerPool("pool", "default", 1, "worker:v1")
			wp.Spec.SandboxClasses[0].ConfigRef = &atev1.SandboxConfigReference{Name: "x"}
			objs := []client.Object{wp, x(), protected(makeSandboxConfig("y", atev1.SandboxClassGvisor, false))}
			if tc.inUse != "" {
				wp.Status.SandboxClasses = []atev1.WorkerPoolSandboxClassStatus{{
					Name:      atev1.SandboxClassGvisor,
					ConfigRef: &atev1.SandboxConfigReference{Name: tc.inUse},
				}}
				objs = append(objs, &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: wp.Name, Namespace: wp.Namespace}})
			}
			c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).WithStatusSubresource(wp).Build()

			var liveObjs []client.Object
			if tc.liveX != nil {
				liveObjs = append(liveObjs, tc.liveX)
			}
			// claimedAtRead records whether the pool's status claimed x
			// when x was read from the API server.
			var read, claimedAtRead bool
			live := fake.NewClientBuilder().WithScheme(scheme).WithObjects(liveObjs...).
				WithInterceptorFuncs(interceptor.Funcs{
					Get: func(ctx context.Context, lc client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
						read = true
						pool := &atev1.WorkerPool{}
						if err := c.Get(ctx, client.ObjectKeyFromObject(wp), pool); err != nil {
							return err
						}
						claimedAtRead = slices.Contains(sandboxConfigsInUse(pool), key.Name)
						return lc.Get(ctx, key, obj, opts...)
					},
				}).Build()

			r := &WorkerPoolReconciler{Client: c, APIReader: live, Recorder: events.NewFakeRecorder(1)}
			if _, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(wp)}); err != nil {
				t.Fatalf("Reconcile: %v", err)
			}

			if read != tc.wantRead {
				t.Errorf("read x from the API server = %v, want %v", read, tc.wantRead)
			}
			if read && !claimedAtRead {
				t.Errorf("x was read from the API server before the pool's status claimed it")
			}
			got := &atev1.WorkerPool{}
			if err := c.Get(t.Context(), client.ObjectKeyFromObject(wp), got); err != nil {
				t.Fatalf("get WorkerPool: %v", err)
			}
			if name := statusConfig(got, atev1.SandboxClassGvisor); name != tc.wantConfig {
				t.Errorf("status names %q, want %q", name, tc.wantConfig)
			}
			class := sandboxClassStatus(got, atev1.SandboxClassGvisor)
			if class == nil {
				t.Fatalf("no gvisor status entry in %+v", got.Status)
			}
			if class.PendingConfigRef != nil {
				t.Errorf("pendingConfigRef = %q, want none", class.PendingConfigRef.Name)
			}
			if cond := meta.FindStatusCondition(class.Conditions, atev1.WorkerPoolSandboxClassConditionResolved); cond == nil || cond.Reason != tc.wantReason {
				t.Errorf("Resolved condition = %+v, want reason %s", cond, tc.wantReason)
			}
			err := c.Get(t.Context(), client.ObjectKeyFromObject(wp), &appsv1.Deployment{})
			if hasDeployment := err == nil; hasDeployment != tc.wantDeployment {
				t.Errorf("has Deployment = %v (get error %v), want %v", hasDeployment, err, tc.wantDeployment)
			}
		})
	}
}

// TestSwitchInterleavings runs a pool's switch from config y to config x with
// a deletion and a protection reconcile interleaved at each point of the
// switch, as they can be on a real cluster: before the claim is written,
// after it, after x is read from the API server, and after the final status
// write. Throughout, the config status names in configRef must exist, and
// the deleted config must outlive its last claim: the protection reconciler
// may release it only while no pool's status names it.
func TestSwitchInterleavings(t *testing.T) {
	t.Parallel()
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatalf("add client-go scheme: %v", err)
	}
	if err := atev1.AddToScheme(scheme); err != nil {
		t.Fatalf("add scheme: %v", err)
	}
	// The points in a switch where the deletion and the protection reconcile
	// run.
	const (
		beforeClaim = "before claim"
		afterClaim  = "after claim"
		afterRead   = "after read"
		afterFinal  = "after final write"
	)

	tests := []struct {
		name string
		// deleted is the config deleted at the point at.
		deleted string
		at      string
		// unprotected starts x without the protection finalizer, as a config
		// no pool has used yet.
		unprotected bool
		// wantHeldAtPoint is whether the deleted config still exists after
		// the protection reconcile at the point.
		wantHeldAtPoint bool
		// wantFirst is the config status names after the reconcile, and
		// wantSettled after the protection and pool reconciles that the
		// deletion triggers have run.
		wantFirst   string
		wantSettled string
		// wantGone is whether the deleted config is gone once settled.
		wantGone bool
	}{
		{
			name:        "x deleted and released before the claim: switch rejected",
			deleted:     "x",
			at:          beforeClaim,
			wantFirst:   "y",
			wantSettled: "y",
			wantGone:    true,
		},
		{
			name:        "unused x deleted and released before the claim: switch rejected",
			deleted:     "x",
			at:          beforeClaim,
			unprotected: true,
			wantFirst:   "y",
			wantSettled: "y",
			wantGone:    true,
		},
		{
			name:            "x deleted after the claim: held by the claim, switch rejected, then released",
			deleted:         "x",
			at:              afterClaim,
			wantHeldAtPoint: true,
			wantFirst:       "y",
			wantSettled:     "y",
			wantGone:        true,
		},
		{
			name:            "x deleted after the read: held by the claim, switched, kept while in use",
			deleted:         "x",
			at:              afterRead,
			wantHeldAtPoint: true,
			wantFirst:       "x",
			wantSettled:     "x",
		},
		{
			name:            "x deleted after the final write: held, kept while in use",
			deleted:         "x",
			at:              afterFinal,
			wantHeldAtPoint: true,
			wantFirst:       "x",
			wantSettled:     "x",
		},
		{
			name:            "y deleted before the claim: held while named, released after the switch",
			deleted:         "y",
			at:              beforeClaim,
			wantHeldAtPoint: true,
			wantFirst:       "x",
			wantSettled:     "x",
			wantGone:        true,
		},
		{
			name:            "y deleted after the claim: held while named, released after the switch",
			deleted:         "y",
			at:              afterClaim,
			wantHeldAtPoint: true,
			wantFirst:       "x",
			wantSettled:     "x",
			wantGone:        true,
		},
		{
			name:            "y deleted after the read: held while named, released after the switch",
			deleted:         "y",
			at:              afterRead,
			wantHeldAtPoint: true,
			wantFirst:       "x",
			wantSettled:     "x",
			wantGone:        true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			wp := makeWorkerPool("pool", "default", 1, "worker:v1")
			wp.Spec.SandboxClasses[0].ConfigRef = &atev1.SandboxConfigReference{Name: "x"}
			wp.Status.SandboxClasses = []atev1.WorkerPoolSandboxClassStatus{{
				Name:      atev1.SandboxClassGvisor,
				ConfigRef: &atev1.SandboxConfigReference{Name: "y"},
			}}
			x := makeSandboxConfig("x", atev1.SandboxClassGvisor, false)
			if !tc.unprotected {
				controllerutil.AddFinalizer(x, atev1.SandboxConfigWorkerPoolProtectionFinalizer)
			}
			y := makeSandboxConfig("y", atev1.SandboxClassGvisor, false)
			controllerutil.AddFinalizer(y, atev1.SandboxConfigWorkerPoolProtectionFinalizer)
			// base is the cluster itself: the hooks and the protection
			// reconciler use it, so only the pool's reconcile is intercepted.
			base := fake.NewClientBuilder().WithScheme(scheme).WithObjects(wp, x, y).WithStatusSubresource(wp).
				WithIndex(&atev1.WorkerPool{}, workerPoolSandboxConfigIndex, func(obj client.Object) []string {
					return sandboxConfigsInUse(obj.(*atev1.WorkerPool))
				}).Build()
			protection := &SandboxConfigProtectionReconciler{Client: base, APIReader: base}
			protect := func(ctx context.Context) error {
				for _, name := range []string{"x", "y"} {
					if _, err := protection.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: name}}); err != nil {
						return fmt.Errorf("protection reconcile of %q: %w", name, err)
					}
				}
				return nil
			}
			exists := func(ctx context.Context, name string) (bool, error) {
				err := base.Get(ctx, types.NamespacedName{Name: name}, &atev1.SandboxConfig{})
				if k8errors.IsNotFound(err) {
					return false, nil
				}
				return err == nil, err
			}
			// checkConfigRef fails the test if status names a config in
			// configRef that does not exist.
			checkConfigRef := func(ctx context.Context, when string) error {
				pool := &atev1.WorkerPool{}
				if err := base.Get(ctx, client.ObjectKeyFromObject(wp), pool); err != nil {
					return err
				}
				for _, class := range pool.Status.SandboxClasses {
					if class.ConfigRef == nil {
						continue
					}
					ok, err := exists(ctx, class.ConfigRef.Name)
					if err != nil {
						return err
					}
					if !ok {
						t.Errorf("%s: status names %q, which no longer exists", when, class.ConfigRef.Name)
					}
				}
				return nil
			}

			armed, hit := true, false
			// interleave deletes the config and runs the protection
			// reconciler, if at is the point the test interleaves at.
			interleave := func(ctx context.Context, at string) error {
				if !armed || at != tc.at {
					return nil
				}
				hit = true
				sc := &atev1.SandboxConfig{}
				if err := base.Get(ctx, types.NamespacedName{Name: tc.deleted}, sc); err != nil {
					return err
				}
				if err := base.Delete(ctx, sc); err != nil {
					return err
				}
				if err := protect(ctx); err != nil {
					return err
				}
				held, err := exists(ctx, tc.deleted)
				if err != nil {
					return err
				}
				if held != tc.wantHeldAtPoint {
					t.Errorf("%s: %q exists after the protection reconcile = %v, want %v", at, tc.deleted, held, tc.wantHeldAtPoint)
				}
				return checkConfigRef(ctx, at)
			}
			statusWrites := 0
			c := interceptor.NewClient(base, interceptor.Funcs{
				SubResourceUpdate: func(ctx context.Context, c client.Client, sub string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
					statusWrites++
					claim := statusWrites == 1
					if claim {
						if err := interleave(ctx, beforeClaim); err != nil {
							return err
						}
					}
					if err := c.SubResource(sub).Update(ctx, obj, opts...); err != nil {
						return err
					}
					if claim {
						return interleave(ctx, afterClaim)
					}
					return interleave(ctx, afterFinal)
				},
				Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
					if err := c.Get(ctx, key, obj, opts...); err != nil {
						return err
					}
					if _, ok := obj.(*atev1.SandboxConfig); ok {
						return interleave(ctx, afterRead)
					}
					return nil
				},
			})

			r := &WorkerPoolReconciler{Client: c, APIReader: c, Recorder: events.NewFakeRecorder(10)}
			req := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(wp)}
			if _, err := r.Reconcile(t.Context(), req); err != nil {
				t.Fatalf("Reconcile: %v", err)
			}
			armed = false
			if !hit {
				t.Fatalf("the reconcile never reached %q", tc.at)
			}
			if statusWrites != 2 {
				t.Errorf("status writes = %d, want 2: the claim and the final write", statusWrites)
			}
			got := &atev1.WorkerPool{}
			if err := base.Get(t.Context(), req.NamespacedName, got); err != nil {
				t.Fatalf("get WorkerPool: %v", err)
			}
			if name := statusConfig(got, atev1.SandboxClassGvisor); name != tc.wantFirst {
				t.Errorf("after the reconcile, status names %q, want %q", name, tc.wantFirst)
			}
			if err := checkConfigRef(t.Context(), "after the reconcile"); err != nil {
				t.Fatal(err)
			}

			// Settle: the deletion and the status writes trigger the
			// protection reconciler and another pool reconcile.
			if err := protect(t.Context()); err != nil {
				t.Fatal(err)
			}
			if _, err := r.Reconcile(t.Context(), req); err != nil {
				t.Fatalf("second Reconcile: %v", err)
			}
			if err := protect(t.Context()); err != nil {
				t.Fatal(err)
			}
			if err := base.Get(t.Context(), req.NamespacedName, got); err != nil {
				t.Fatalf("get WorkerPool: %v", err)
			}
			if name := statusConfig(got, atev1.SandboxClassGvisor); name != tc.wantSettled {
				t.Errorf("once settled, status names %q, want %q", name, tc.wantSettled)
			}
			if err := checkConfigRef(t.Context(), "once settled"); err != nil {
				t.Fatal(err)
			}
			ok, err := exists(t.Context(), tc.deleted)
			if err != nil {
				t.Fatal(err)
			}
			if gone := !ok; gone != tc.wantGone {
				t.Errorf("once settled, %q gone = %v, want %v", tc.deleted, gone, tc.wantGone)
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
		classes []atev1.WorkerPoolSandboxClassStatus
		want    bool
	}{
		{
			name:    "every class has a config",
			classes: []atev1.WorkerPoolSandboxClassStatus{{Name: atev1.SandboxClassGvisor, ConfigRef: &atev1.SandboxConfigReference{Name: "def"}}},
			want:    true,
		},
		{
			name: "one class has no config",
			classes: []atev1.WorkerPoolSandboxClassStatus{
				{Name: atev1.SandboxClassGvisor, ConfigRef: &atev1.SandboxConfigReference{Name: "def"}},
				{Name: atev1.SandboxClassMicroVM},
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
	if err := atev1.AddToScheme(scheme); err != nil {
		t.Fatalf("add scheme: %v", err)
	}
	errInjected := errors.New("injected")
	failList := interceptor.Funcs{
		List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
			if _, ok := list.(*atev1.SandboxConfigList); ok {
				return errInjected
			}
			return c.List(ctx, list, opts...)
		},
	}
	failProtect := interceptor.Funcs{
		Patch: func(ctx context.Context, c client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
			if _, ok := obj.(*atev1.SandboxConfig); ok {
				return errInjected
			}
			return c.Patch(ctx, obj, patch, opts...)
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
			name:          "protection fails, Deployment exists",
			funcs:         failProtect,
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
			name:  "protection fails, no Deployment",
			funcs: failProtect,
		},
		{
			name:  "status write fails, no Deployment",
			funcs: failStatus,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			wp := makeWorkerPool("pool", "default", 3, "worker:v1")
			objs := []client.Object{wp, makeSandboxConfig("def", atev1.SandboxClassGvisor, true)}
			if tc.hasDeployment {
				objs = append(objs, &appsv1.Deployment{
					ObjectMeta: metav1.ObjectMeta{Name: wp.Name, Namespace: wp.Namespace},
					Spec:       appsv1.DeploymentSpec{Replicas: ptr.To[int32](1)},
				})
			}
			c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).WithStatusSubresource(wp).
				WithInterceptorFuncs(tc.funcs).Build()
			r := &WorkerPoolReconciler{Client: c, APIReader: c, Recorder: events.NewFakeRecorder(1)}
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
	if err := atev1.AddToScheme(scheme); err != nil {
		t.Fatalf("add scheme: %v", err)
	}
	classes := func(config string) []atev1.WorkerPoolSandboxClassStatus {
		return []atev1.WorkerPoolSandboxClassStatus{{
			Name:      atev1.SandboxClassGvisor,
			ConfigRef: &atev1.SandboxConfigReference{Name: config},
			Conditions: []metav1.Condition{{
				Type:               atev1.WorkerPoolSandboxClassConditionResolved,
				Status:             metav1.ConditionTrue,
				Reason:             atev1.WorkerPoolReasonResolved,
				Message:            "resolved to SandboxConfig " + config,
				ObservedGeneration: 2,
				LastTransitionTime: metav1.NewTime(time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)),
			}},
		}}
	}
	before := atev1.WorkerPoolStatus{
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
		classes   []atev1.WorkerPoolSandboxClassStatus
		want      atev1.WorkerPoolStatus
		wantWrite bool
	}{
		{
			name:    "nil Deployment keeps the replica fields",
			classes: classes("new"),
			want: atev1.WorkerPoolStatus{
				Replicas: 3, ReadyReplicas: 2, Selector: "app=old",
				SandboxClasses: classes("new"),
			},
			wantWrite: true,
		},
		{
			name:    "Deployment replaces the replica fields",
			dep:     dep,
			classes: classes("new"),
			want: atev1.WorkerPoolStatus{
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
			wp := makeWorkerPool("pool", "default", 1, "worker:v1")
			wp.Status = *before.DeepCopy()
			c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(wp).WithStatusSubresource(wp).Build()
			stored := &atev1.WorkerPool{}
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
			got := &atev1.WorkerPool{}
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

// TestAddProtectionFinalizer checks that addProtectionFinalizer patches only
// when the finalizer is missing, leaves other finalizers alone, never adds to
// a deleting config, and fails with a conflict when the config changed since
// it was read.
func TestAddProtectionFinalizer(t *testing.T) {
	t.Parallel()
	runProtectionFinalizerCases(t, []protectionFinalizerCase{
		{
			name:        "add to unprotected config",
			op:          addProtectionFinalizer,
			sc:          protectedSandboxConfig(false, false),
			wantPatches: 1,
			want:        true,
		},
		{
			name: "add to protected config: no patch",
			op:   addProtectionFinalizer,
			sc:   protectedSandboxConfig(true, false),
			want: true,
		},
		{
			name: "add to deleting config: no patch",
			op:   addProtectionFinalizer,
			sc:   protectedSandboxConfig(false, true),
			want: false,
		},
		{
			name:        "add to stale config: conflict",
			op:          addProtectionFinalizer,
			sc:          protectedSandboxConfig(false, false),
			stale:       true,
			wantPatches: 1,
			want:        false,
			wantErr:     k8errors.IsConflict,
		},
	})
}

// --- helpers ---

func makeSandboxConfig(name string, class atev1.SandboxClass, isDefault bool) *atev1.SandboxConfig {
	v := atev1.SandboxVersionConfig{Name: "v1"}
	sc := &atev1.SandboxConfig{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: atev1.SandboxConfigSpec{
			SandboxClass:   class,
			DefaultVersion: v.Name,
			Versions:       []atev1.SandboxVersionConfig{v},
		},
	}
	if isDefault {
		sc.Annotations = map[string]string{atev1.SandboxConfigClassDefaultAnnotation: "true"}
	}
	return sc
}

func createSandboxConfig(t *testing.T, sc *atev1.SandboxConfig) {
	t.Helper()
	if err := k8sClient.Create(t.Context(), sc); err != nil {
		t.Fatalf("create SandboxConfig %s: %v", sc.Name, err)
	}
	deleteOnCleanup(t, sc)
}

func createWorkerPool(t *testing.T, wp *atev1.WorkerPool) {
	t.Helper()
	if err := k8sClient.Create(t.Context(), wp); err != nil {
		t.Fatalf("create WorkerPool %s: %v", wp.Name, err)
	}
	deleteOnCleanup(t, wp)
}

// waitResolved waits for wp's sandbox class to report Resolved=True for its
// current generation and to use config.
func waitResolved(t *testing.T, wp *atev1.WorkerPool, config string) *atev1.WorkerPool {
	t.Helper()
	return waitCondition(t, wp, func(current *atev1.WorkerPool, cond *metav1.Condition) bool {
		return cond.Status == metav1.ConditionTrue &&
			statusConfig(current, current.Spec.DefaultSandboxClass()) == config
	})
}

// waitUnresolved waits for wp's sandbox class to report Resolved=False with
// reason for its current generation.
func waitUnresolved(t *testing.T, wp *atev1.WorkerPool, reason string) *atev1.WorkerPool {
	t.Helper()
	return waitCondition(t, wp, func(_ *atev1.WorkerPool, cond *metav1.Condition) bool {
		return cond.Status == metav1.ConditionFalse && cond.Reason == reason
	})
}

func waitCondition(t *testing.T, wp *atev1.WorkerPool, ok func(*atev1.WorkerPool, *metav1.Condition) bool) *atev1.WorkerPool {
	t.Helper()
	current := &atev1.WorkerPool{}
	eventually(t, func(ctx context.Context) (bool, error) {
		if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(wp), current); err != nil {
			return false, nil
		}
		status := sandboxClassStatus(current, current.Spec.DefaultSandboxClass())
		if status == nil {
			return false, nil
		}
		cond := meta.FindStatusCondition(status.Conditions, atev1.WorkerPoolSandboxClassConditionResolved)
		return cond != nil && cond.ObservedGeneration == current.Generation && ok(current, cond), nil
	})
	return current
}

// waitProtectionFinalizer waits for the SandboxConfig name to carry, or not
// carry, the protection finalizer.
func waitProtectionFinalizer(t *testing.T, name string, want bool) {
	t.Helper()
	eventually(t, func(ctx context.Context) (bool, error) {
		sc := &atev1.SandboxConfig{}
		if err := k8sClient.Get(ctx, types.NamespacedName{Name: name}, sc); err != nil {
			return false, nil
		}
		return controllerutil.ContainsFinalizer(sc, atev1.SandboxConfigWorkerPoolProtectionFinalizer) == want, nil
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
func assertNoDeployment(t *testing.T, wp *atev1.WorkerPool) {
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
		sc := &atev1.SandboxConfig{}
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
func makeWorkerPoolClasses(ns, name string, spec []atev1.WorkerPoolSandboxClass, status []atev1.WorkerPoolSandboxClassStatus) *atev1.WorkerPool {
	wp := makeWorkerPool(name, ns, 1, "worker:v1")
	wp.Spec.SandboxClasses = spec
	wp.Status.SandboxClasses = status
	return wp
}

// statusConfig returns the SandboxConfig wp's status uses for class, or "".
func statusConfig(wp *atev1.WorkerPool, class atev1.SandboxClass) string {
	if s := sandboxClassStatus(wp, class); s != nil && s.ConfigRef != nil {
		return s.ConfigRef.Name
	}
	return ""
}

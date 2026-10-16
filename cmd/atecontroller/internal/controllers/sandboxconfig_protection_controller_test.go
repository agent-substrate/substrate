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
	"context"
	"errors"
	"testing"

	k8errors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	atev1alpha1 "go.ate.dev/substrate/pkg/api/v1alpha1"
)

// TestSandboxConfigProtectionReconcile checks that the reconciler never adds
// the protection finalizer, that a deleting SandboxConfig loses it only when
// neither the cache nor a live read finds a WorkerPool naming it, that it
// keeps it when either list fails, and that other finalizers are untouched.
func TestSandboxConfigProtectionReconcile(t *testing.T) {
	t.Parallel()
	scheme := runtime.NewScheme()
	if err := atev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add scheme: %v", err)
	}
	now := metav1.Now()
	// using returns a pool whose status has one sandbox class per name, each
	// using the named config, or no config when the name is "".
	using := func(names ...string) *atev1alpha1.WorkerPool {
		wp := makeWorkerPool("pool", "default", 1, "worker:v1")
		for _, name := range names {
			s := atev1alpha1.WorkerPoolSandboxClassStatus{Name: atev1alpha1.SandboxClassGvisor}
			if name != "" {
				s.ConfigRef = &atev1alpha1.SandboxConfigReference{Name: name}
			}
			wp.Status.SandboxClasses = append(wp.Status.SandboxClasses, s)
		}
		return wp
	}
	// claiming returns a pool whose one sandbox class uses "other" and is
	// switching to the named config.
	claiming := func(name string) *atev1alpha1.WorkerPool {
		wp := using("other")
		wp.Status.SandboxClasses[0].PendingConfigRef = &atev1alpha1.SandboxConfigReference{Name: name}
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
	// newClient returns a client whose List fails with listErr, if set.
	newClient := func(listErr error, objs ...client.Object) client.Client {
		return fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).
			WithIndex(&atev1alpha1.WorkerPool{}, workerPoolSandboxConfigIndex, func(obj client.Object) []string {
				return sandboxConfigsInUse(obj.(*atev1alpha1.WorkerPool))
			}).
			WithInterceptorFuncs(interceptor.Funcs{
				List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
					if listErr != nil {
						return listErr
					}
					return c.List(ctx, list, opts...)
				},
			}).Build()
	}
	errList := errors.New("list failed")

	tests := []struct {
		name   string
		sc     *atev1alpha1.SandboxConfig
		cached []client.Object
		live   []client.Object
		// cachedErr and liveErr fail the cached and the live WorkerPool list.
		cachedErr error
		liveErr   error
		want      bool
		// wantErr is the error Reconcile must return, or nil.
		wantErr error
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
			name:   "deleting, used by a later sandbox class in cache: keep",
			sc:     config(true, true),
			cached: []client.Object{using("other", "sc")},
			want:   true,
		},
		{
			name: "deleting, used by a later sandbox class live: keep",
			sc:   config(true, true),
			live: []client.Object{using("other", "sc")},
			want: true,
		},
		{
			name:   "deleting, claimed in cache: keep",
			sc:     config(true, true),
			cached: []client.Object{claiming("sc")},
			want:   true,
		},
		{
			name: "deleting, unclaimed in cache but claimed live: keep",
			sc:   config(true, true),
			live: []client.Object{claiming("sc")},
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
			name:   "deleting, only a pool that has no config in use: remove",
			sc:     config(true, true),
			cached: []client.Object{using("")},
			live:   []client.Object{using("")},
			want:   false,
		},
		{
			name: "deleting without the finalizer: nothing to do",
			sc:   config(false, true),
			want: false,
		},
		{
			name:      "deleting, cached list fails: keep and return the error",
			sc:        config(true, true),
			cachedErr: errList,
			want:      true,
			wantErr:   errList,
		},
		{
			name:    "deleting, unused in cache, live list fails: keep and return the error",
			sc:      config(true, true),
			cached:  []client.Object{using("other")},
			liveErr: errList,
			want:    true,
			wantErr: errList,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			c := newClient(tc.cachedErr, append([]client.Object{tc.sc}, tc.cached...)...)
			r := &SandboxConfigProtectionReconciler{Client: c, APIReader: newClient(tc.liveErr, tc.live...)}
			_, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: types.NamespacedName{Name: tc.sc.Name}})
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("Reconcile error = %v, want %v", err, tc.wantErr)
			}
			got := &atev1alpha1.SandboxConfig{}
			if err := c.Get(t.Context(), types.NamespacedName{Name: tc.sc.Name}, got); err != nil {
				t.Fatalf("get SandboxConfig: %v", err)
			}
			if has := controllerutil.ContainsFinalizer(got, atev1alpha1.SandboxConfigWorkerPoolProtectionFinalizer); has != tc.want {
				t.Errorf("has protection finalizer = %v, want %v", has, tc.want)
			}
			if controllerutil.ContainsFinalizer(tc.sc, testHoldFinalizer) && !controllerutil.ContainsFinalizer(got, testHoldFinalizer) {
				t.Errorf("lost %s", testHoldFinalizer)
			}
		})
	}
}

// TestSandboxConfigProtectionReconcileGet checks Reconcile when the
// SandboxConfig cannot be read: a config that is gone needs nothing, and any
// other read error is returned so the request is retried.
func TestSandboxConfigProtectionReconcileGet(t *testing.T) {
	t.Parallel()
	scheme := runtime.NewScheme()
	if err := atev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add scheme: %v", err)
	}
	errGet := errors.New("get failed")

	tests := []struct {
		name string
		// getErr fails every Get, if set.
		getErr  error
		wantErr error
	}{
		{
			name: "not found: nothing to release",
		},
		{
			name:    "get fails: return the error",
			getErr:  errGet,
			wantErr: errGet,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			c := fake.NewClientBuilder().WithScheme(scheme).
				WithInterceptorFuncs(interceptor.Funcs{
					Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
						if tc.getErr != nil {
							return tc.getErr
						}
						return c.Get(ctx, key, obj, opts...)
					},
				}).Build()
			r := &SandboxConfigProtectionReconciler{Client: c, APIReader: c}
			res, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: types.NamespacedName{Name: "missing"}})
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("Reconcile error = %v, want %v", err, tc.wantErr)
			}
			if !res.IsZero() {
				t.Errorf("Reconcile result = %+v, want zero", res)
			}
		})
	}
}

// protectionFinalizerCase is one case of a test that applies
// addProtectionFinalizer or removeProtectionFinalizer to a SandboxConfig.
type protectionFinalizerCase struct {
	name string
	op   func(context.Context, client.Client, *atev1alpha1.SandboxConfig) error
	sc   *atev1alpha1.SandboxConfig
	// stale changes the stored config after it is read, so the patch
	// carries an outdated resourceVersion.
	stale       bool
	wantPatches int
	want        bool
	wantErr     func(error) bool
}

// protectedSandboxConfig builds a SandboxConfig for a protectionFinalizerCase.
// It always carries testHoldFinalizer, so it stays readable while deleting,
// and the protection finalizer if protected.
func protectedSandboxConfig(protected, deleting bool) *atev1alpha1.SandboxConfig {
	sc := makeSandboxConfig("sc", atev1alpha1.SandboxClassGvisor, false)
	controllerutil.AddFinalizer(sc, testHoldFinalizer)
	if protected {
		controllerutil.AddFinalizer(sc, atev1alpha1.SandboxConfigWorkerPoolProtectionFinalizer)
	}
	if deleting {
		now := metav1.Now()
		sc.DeletionTimestamp = &now
	}
	return sc
}

// runProtectionFinalizerCases applies each case's op to its config and
// checks the patches made, the error, the protection finalizer, and that
// testHoldFinalizer is still there.
func runProtectionFinalizerCases(t *testing.T, tests []protectionFinalizerCase) {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := atev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add scheme: %v", err)
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			patches := 0
			c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(tc.sc).
				WithInterceptorFuncs(interceptor.Funcs{
					Patch: func(ctx context.Context, c client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
						patches++
						return c.Patch(ctx, obj, patch, opts...)
					},
				}).Build()
			key := types.NamespacedName{Name: tc.sc.Name}
			read := &atev1alpha1.SandboxConfig{}
			if err := c.Get(t.Context(), key, read); err != nil {
				t.Fatalf("get SandboxConfig: %v", err)
			}
			if tc.stale {
				stored := read.DeepCopy()
				stored.Labels = map[string]string{"changed": "true"}
				if err := c.Update(t.Context(), stored); err != nil {
					t.Fatalf("update SandboxConfig: %v", err)
				}
			}

			err := tc.op(t.Context(), c, read)
			if tc.wantErr == nil && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if tc.wantErr != nil && !tc.wantErr(err) {
				t.Fatalf("error = %v, want a conflict", err)
			}
			if patches != tc.wantPatches {
				t.Errorf("patches = %d, want %d", patches, tc.wantPatches)
			}
			got := &atev1alpha1.SandboxConfig{}
			if err := c.Get(t.Context(), key, got); err != nil {
				t.Fatalf("get SandboxConfig: %v", err)
			}
			if has := controllerutil.ContainsFinalizer(got, atev1alpha1.SandboxConfigWorkerPoolProtectionFinalizer); has != tc.want {
				t.Errorf("has protection finalizer = %v, want %v", has, tc.want)
			}
			if !controllerutil.ContainsFinalizer(got, testHoldFinalizer) {
				t.Errorf("lost %s", testHoldFinalizer)
			}
		})
	}
}

// TestRemoveProtectionFinalizer checks that removeProtectionFinalizer patches
// only when the finalizer is present, leaves other finalizers alone, and fails
// with a conflict when the config changed since it was read.
func TestRemoveProtectionFinalizer(t *testing.T) {
	t.Parallel()
	runProtectionFinalizerCases(t, []protectionFinalizerCase{
		{
			name:        "remove from protected config",
			op:          removeProtectionFinalizer,
			sc:          protectedSandboxConfig(true, false),
			wantPatches: 1,
			want:        false,
		},
		{
			name:        "remove from deleting config",
			op:          removeProtectionFinalizer,
			sc:          protectedSandboxConfig(true, true),
			wantPatches: 1,
			want:        false,
		},
		{
			name: "remove from unprotected config: no patch",
			op:   removeProtectionFinalizer,
			sc:   protectedSandboxConfig(false, false),
			want: false,
		},
		{
			name:        "remove from stale config: conflict",
			op:          removeProtectionFinalizer,
			sc:          protectedSandboxConfig(true, false),
			stale:       true,
			wantPatches: 1,
			want:        true,
			wantErr:     k8errors.IsConflict,
		},
	})
}

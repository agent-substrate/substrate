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
	"testing"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	atev1alpha1 "github.com/agent-substrate/substrate/pkg/api/v1alpha1"
)

func TestGoldenSnapshotWarmupFor(t *testing.T) {
	probe := &atev1alpha1.ContainerReadyz{
		HTTPGet: &atev1alpha1.HTTPGetAction{Port: 80},
	}

	tests := []struct {
		name       string
		containers []atev1alpha1.Container
		wantZero   bool
	}{
		{
			name:       "no containers keeps default warmup",
			containers: nil,
			wantZero:   false,
		},
		{
			name: "all containers have readyz skips warmup",
			containers: []atev1alpha1.Container{
				{Name: "a", Readyz: probe},
				{Name: "b", Readyz: probe},
			},
			wantZero: true,
		},
		{
			name: "single container with readyz skips warmup",
			containers: []atev1alpha1.Container{
				{Name: "a", Readyz: probe},
			},
			wantZero: true,
		},
		{
			name: "mixed containers keep warmup",
			containers: []atev1alpha1.Container{
				{Name: "a", Readyz: probe},
				{Name: "b"},
			},
			wantZero: false,
		},
		{
			name: "no readyz anywhere keeps warmup",
			containers: []atev1alpha1.Container{
				{Name: "a"},
				{Name: "b"},
			},
			wantZero: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			at := &atev1alpha1.ActorTemplate{
				Spec: atev1alpha1.ActorTemplateSpec{Containers: tt.containers},
			}
			got := goldenSnapshotWarmupFor(at)
			if tt.wantZero && got != 0 {
				t.Errorf("goldenSnapshotWarmupFor = %v, want 0", got)
			}
			if !tt.wantZero && got != goldenSnapshotWarmup {
				t.Errorf("goldenSnapshotWarmupFor = %v, want %v", got, goldenSnapshotWarmup)
			}
		})
	}
}

type mockControlClient struct {
	ateapipb.ControlClient
	createAtespaceFn func(ctx context.Context, req *ateapipb.CreateAtespaceRequest, opts ...grpc.CallOption) (*ateapipb.Atespace, error)
	createActorFn    func(ctx context.Context, req *ateapipb.CreateActorRequest, opts ...grpc.CallOption) (*ateapipb.Actor, error)
	deleteActorFn    func(ctx context.Context, req *ateapipb.DeleteActorRequest, opts ...grpc.CallOption) (*ateapipb.Actor, error)
	suspendActorFn   func(ctx context.Context, req *ateapipb.SuspendActorRequest, opts ...grpc.CallOption) (*ateapipb.SuspendActorResponse, error)
}

func (m *mockControlClient) CreateAtespace(ctx context.Context, req *ateapipb.CreateAtespaceRequest, opts ...grpc.CallOption) (*ateapipb.Atespace, error) {
	if m.createAtespaceFn != nil {
		return m.createAtespaceFn(ctx, req, opts...)
	}
	return &ateapipb.Atespace{}, nil
}

func (m *mockControlClient) CreateActor(ctx context.Context, req *ateapipb.CreateActorRequest, opts ...grpc.CallOption) (*ateapipb.Actor, error) {
	if m.createActorFn != nil {
		return m.createActorFn(ctx, req, opts...)
	}
	return &ateapipb.Actor{}, nil
}

func (m *mockControlClient) DeleteActor(ctx context.Context, req *ateapipb.DeleteActorRequest, opts ...grpc.CallOption) (*ateapipb.Actor, error) {
	if m.deleteActorFn != nil {
		return m.deleteActorFn(ctx, req, opts...)
	}
	return &ateapipb.Actor{}, nil
}

func (m *mockControlClient) SuspendActor(ctx context.Context, req *ateapipb.SuspendActorRequest, opts ...grpc.CallOption) (*ateapipb.SuspendActorResponse, error) {
	if m.suspendActorFn != nil {
		return m.suspendActorFn(ctx, req, opts...)
	}
	return &ateapipb.SuspendActorResponse{}, nil
}

func TestActorTemplateReconciler_Reconcile_PhaseInitial(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := atev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("failed to add scheme: %v", err)
	}

	const templateUID = "test-uid-12345"
	const expectedActorName = templateUID

	t.Run("creates golden actor using template UID", func(t *testing.T) {
		template := &atev1alpha1.ActorTemplate{
			ObjectMeta: metav1.ObjectMeta{
				Name:       "my-template",
				Namespace:  "default",
				UID:        types.UID(templateUID),
				Finalizers: []string{goldenActorFinalizer},
			},
			Status: atev1alpha1.ActorTemplateStatus{
				Phase: atev1alpha1.PhaseInitial,
			},
		}

		fakeK8sClient := fake.NewClientBuilder().
			WithScheme(scheme).
			WithStatusSubresource(&atev1alpha1.ActorTemplate{}).
			WithObjects(template).
			Build()

		var createdActorName string
		fakeAteClient := &mockControlClient{
			createActorFn: func(ctx context.Context, req *ateapipb.CreateActorRequest, opts ...grpc.CallOption) (*ateapipb.Actor, error) {
				createdActorName = req.GetActor().GetMetadata().GetName()
				return &ateapipb.Actor{}, nil
			},
		}

		reconciler := &ActorTemplateReconciler{
			Client:    fakeK8sClient,
			Scheme:    scheme,
			AteClient: fakeAteClient,
		}

		ctx := context.Background()
		req := ctrl.Request{NamespacedName: types.NamespacedName{Name: "my-template", Namespace: "default"}}
		res, err := reconciler.Reconcile(ctx, req)
		if err != nil {
			t.Fatalf("Reconcile returned error: %v", err)
		}
		if !res.IsZero() {
			t.Errorf("unexpected requeue result: %v", res)
		}

		if createdActorName != expectedActorName {
			t.Errorf("created actor name = %q, want %q", createdActorName, expectedActorName)
		}

		reconciledTemplate := &atev1alpha1.ActorTemplate{}
		if err := fakeK8sClient.Get(ctx, req.NamespacedName, reconciledTemplate); err != nil {
			t.Fatalf("failed to get reconciled ActorTemplate: %v", err)
		}

		if reconciledTemplate.Status.GoldenActorID != expectedActorName {
			t.Errorf("status.GoldenActorID = %q, want %q", reconciledTemplate.Status.GoldenActorID, expectedActorName)
		}
		if reconciledTemplate.Status.Phase != atev1alpha1.PhaseResumeGoldenActor {
			t.Errorf("status.Phase = %q, want %q", reconciledTemplate.Status.Phase, atev1alpha1.PhaseResumeGoldenActor)
		}
	})

	t.Run("handles AlreadyExists error when golden actor was created on prior attempt", func(t *testing.T) {
		template := &atev1alpha1.ActorTemplate{
			ObjectMeta: metav1.ObjectMeta{
				Name:       "my-template-retry",
				Namespace:  "default",
				UID:        types.UID(templateUID),
				Finalizers: []string{goldenActorFinalizer},
			},
			Status: atev1alpha1.ActorTemplateStatus{
				Phase: atev1alpha1.PhaseInitial,
			},
		}

		fakeK8sClient := fake.NewClientBuilder().
			WithScheme(scheme).
			WithStatusSubresource(&atev1alpha1.ActorTemplate{}).
			WithObjects(template).
			Build()

		fakeAteClient := &mockControlClient{
			createActorFn: func(ctx context.Context, req *ateapipb.CreateActorRequest, opts ...grpc.CallOption) (*ateapipb.Actor, error) {
				return nil, status.Error(codes.AlreadyExists, "actor already exists in ateapi")
			},
		}

		reconciler := &ActorTemplateReconciler{
			Client:    fakeK8sClient,
			Scheme:    scheme,
			AteClient: fakeAteClient,
		}

		ctx := context.Background()
		req := ctrl.Request{NamespacedName: types.NamespacedName{Name: "my-template-retry", Namespace: "default"}}
		_, err := reconciler.Reconcile(ctx, req)
		if err != nil {
			t.Fatalf("Reconcile returned error on AlreadyExists retry: %v", err)
		}

		reconciledTemplate := &atev1alpha1.ActorTemplate{}
		if err := fakeK8sClient.Get(ctx, req.NamespacedName, reconciledTemplate); err != nil {
			t.Fatalf("failed to get reconciled ActorTemplate: %v", err)
		}

		if reconciledTemplate.Status.GoldenActorID != expectedActorName {
			t.Errorf("status.GoldenActorID = %q, want %q", reconciledTemplate.Status.GoldenActorID, expectedActorName)
		}
		if reconciledTemplate.Status.Phase != atev1alpha1.PhaseResumeGoldenActor {
			t.Errorf("status.Phase = %q, want %q", reconciledTemplate.Status.Phase, atev1alpha1.PhaseResumeGoldenActor)
		}
	})
}

func TestActorTemplateReconciler_Reconcile_InstallsFinalizer(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := atev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("failed to add scheme: %v", err)
	}

	template := &atev1alpha1.ActorTemplate{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "needs-finalizer",
			Namespace: "default",
			UID:       types.UID("uid-install"),
		},
		Status: atev1alpha1.ActorTemplateStatus{Phase: atev1alpha1.PhaseInitial},
	}

	fakeK8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&atev1alpha1.ActorTemplate{}).
		WithObjects(template).
		Build()

	var createActorCalled bool
	fakeAteClient := &mockControlClient{
		createActorFn: func(ctx context.Context, req *ateapipb.CreateActorRequest, opts ...grpc.CallOption) (*ateapipb.Actor, error) {
			createActorCalled = true
			return &ateapipb.Actor{}, nil
		},
	}

	reconciler := &ActorTemplateReconciler{Client: fakeK8sClient, Scheme: scheme, AteClient: fakeAteClient}
	ctx := context.Background()
	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: "needs-finalizer", Namespace: "default"}}

	if _, err := reconciler.Reconcile(ctx, req); err != nil {
		t.Fatalf("Reconcile returned error: %v", err)
	}

	// First reconcile only installs the finalizer and returns early — no golden
	// actor is created yet, and the phase is untouched.
	if createActorCalled {
		t.Errorf("CreateActor called on the finalizer-install reconcile; want deferred to the next pass")
	}
	got := &atev1alpha1.ActorTemplate{}
	if err := fakeK8sClient.Get(ctx, req.NamespacedName, got); err != nil {
		t.Fatalf("failed to get template: %v", err)
	}
	if len(got.Finalizers) != 1 || got.Finalizers[0] != goldenActorFinalizer {
		t.Errorf("finalizers = %v, want [%q]", got.Finalizers, goldenActorFinalizer)
	}
	if got.Status.Phase != atev1alpha1.PhaseInitial {
		t.Errorf("status.Phase = %q, want %q (unchanged)", got.Status.Phase, atev1alpha1.PhaseInitial)
	}
}

func TestActorTemplateReconciler_Reconcile_Deletion(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := atev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("failed to add scheme: %v", err)
	}

	newDeletingTemplate := func(name, goldenActorID string) *atev1alpha1.ActorTemplate {
		return &atev1alpha1.ActorTemplate{
			ObjectMeta: metav1.ObjectMeta{
				Name:       name,
				Namespace:  "default",
				UID:        types.UID("uid-" + name),
				Finalizers: []string{goldenActorFinalizer},
			},
			Status: atev1alpha1.ActorTemplateStatus{
				Phase:         atev1alpha1.PhaseReady,
				GoldenActorID: goldenActorID,
			},
		}
	}

	t.Run("deletes suspended golden actor and releases finalizer", func(t *testing.T) {
		template := newDeletingTemplate("del-suspended", "uid-12345")
		fakeK8sClient := fake.NewClientBuilder().
			WithScheme(scheme).
			WithStatusSubresource(&atev1alpha1.ActorTemplate{}).
			WithObjects(template).
			Build()

		var deletedName string
		var suspendCalled bool
		fakeAteClient := &mockControlClient{
			deleteActorFn: func(ctx context.Context, req *ateapipb.DeleteActorRequest, opts ...grpc.CallOption) (*ateapipb.Actor, error) {
				deletedName = req.GetActor().GetName()
				return &ateapipb.Actor{}, nil
			},
			suspendActorFn: func(ctx context.Context, req *ateapipb.SuspendActorRequest, opts ...grpc.CallOption) (*ateapipb.SuspendActorResponse, error) {
				suspendCalled = true
				return &ateapipb.SuspendActorResponse{}, nil
			},
		}

		reconciler := &ActorTemplateReconciler{Client: fakeK8sClient, Scheme: scheme, AteClient: fakeAteClient}
		ctx := context.Background()
		req := ctrl.Request{NamespacedName: types.NamespacedName{Name: "del-suspended", Namespace: "default"}}

		if err := fakeK8sClient.Delete(ctx, template); err != nil {
			t.Fatalf("failed to mark template for deletion: %v", err)
		}
		res, err := reconciler.Reconcile(ctx, req)
		if err != nil {
			t.Fatalf("Reconcile returned error: %v", err)
		}
		if !res.IsZero() {
			t.Errorf("unexpected requeue result: %v", res)
		}
		if deletedName != "uid-12345" {
			t.Errorf("DeleteActor called with name %q, want %q", deletedName, "uid-12345")
		}
		if suspendCalled {
			t.Errorf("SuspendActor should not be called for an already-deletable actor")
		}
		got := &atev1alpha1.ActorTemplate{}
		if err := fakeK8sClient.Get(ctx, req.NamespacedName, got); err == nil {
			t.Errorf("template still present after finalizer removal; want deleted (finalizers=%v)", got.Finalizers)
		}
	})

	t.Run("suspends running golden actor and requeues", func(t *testing.T) {
		template := newDeletingTemplate("del-running", "uid-running")
		fakeK8sClient := fake.NewClientBuilder().
			WithScheme(scheme).
			WithStatusSubresource(&atev1alpha1.ActorTemplate{}).
			WithObjects(template).
			Build()

		var suspendedName string
		fakeAteClient := &mockControlClient{
			deleteActorFn: func(ctx context.Context, req *ateapipb.DeleteActorRequest, opts ...grpc.CallOption) (*ateapipb.Actor, error) {
				return nil, status.Error(codes.FailedPrecondition, "actor is not in a deletable state")
			},
			suspendActorFn: func(ctx context.Context, req *ateapipb.SuspendActorRequest, opts ...grpc.CallOption) (*ateapipb.SuspendActorResponse, error) {
				suspendedName = req.GetActor().GetName()
				return &ateapipb.SuspendActorResponse{}, nil
			},
		}

		reconciler := &ActorTemplateReconciler{Client: fakeK8sClient, Scheme: scheme, AteClient: fakeAteClient}
		ctx := context.Background()
		req := ctrl.Request{NamespacedName: types.NamespacedName{Name: "del-running", Namespace: "default"}}

		if err := fakeK8sClient.Delete(ctx, template); err != nil {
			t.Fatalf("failed to mark template for deletion: %v", err)
		}
		res, err := reconciler.Reconcile(ctx, req)
		if err != nil {
			t.Fatalf("Reconcile returned error: %v", err)
		}
		if res.RequeueAfter != goldenActorDeleteRetry {
			t.Errorf("RequeueAfter = %v, want %v", res.RequeueAfter, goldenActorDeleteRetry)
		}
		if suspendedName != "uid-running" {
			t.Errorf("SuspendActor called with name %q, want %q", suspendedName, "uid-running")
		}
		// Finalizer must still be held so the retry pass can delete the actor.
		got := &atev1alpha1.ActorTemplate{}
		if err := fakeK8sClient.Get(ctx, req.NamespacedName, got); err != nil {
			t.Fatalf("template unexpectedly gone before actor was reclaimed: %v", err)
		}
		if !controllerContainsFinalizer(got, goldenActorFinalizer) {
			t.Errorf("finalizer released before golden actor was reclaimed; finalizers=%v", got.Finalizers)
		}
	})

	t.Run("treats already-gone golden actor as success", func(t *testing.T) {
		template := newDeletingTemplate("del-notfound", "uid-gone")
		fakeK8sClient := fake.NewClientBuilder().
			WithScheme(scheme).
			WithStatusSubresource(&atev1alpha1.ActorTemplate{}).
			WithObjects(template).
			Build()

		fakeAteClient := &mockControlClient{
			deleteActorFn: func(ctx context.Context, req *ateapipb.DeleteActorRequest, opts ...grpc.CallOption) (*ateapipb.Actor, error) {
				return nil, status.Error(codes.NotFound, "actor already gone")
			},
		}

		reconciler := &ActorTemplateReconciler{Client: fakeK8sClient, Scheme: scheme, AteClient: fakeAteClient}
		ctx := context.Background()
		req := ctrl.Request{NamespacedName: types.NamespacedName{Name: "del-notfound", Namespace: "default"}}

		if err := fakeK8sClient.Delete(ctx, template); err != nil {
			t.Fatalf("failed to mark template for deletion: %v", err)
		}
		if _, err := reconciler.Reconcile(ctx, req); err != nil {
			t.Fatalf("Reconcile returned error: %v", err)
		}
		got := &atev1alpha1.ActorTemplate{}
		if err := fakeK8sClient.Get(ctx, req.NamespacedName, got); err == nil {
			t.Errorf("template still present after idempotent delete; want deleted (finalizers=%v)", got.Finalizers)
		}
	})

	t.Run("releases finalizer when no golden actor was ever created", func(t *testing.T) {
		template := newDeletingTemplate("del-nogolden", "")
		fakeK8sClient := fake.NewClientBuilder().
			WithScheme(scheme).
			WithStatusSubresource(&atev1alpha1.ActorTemplate{}).
			WithObjects(template).
			Build()

		var deleteCalled bool
		fakeAteClient := &mockControlClient{
			deleteActorFn: func(ctx context.Context, req *ateapipb.DeleteActorRequest, opts ...grpc.CallOption) (*ateapipb.Actor, error) {
				deleteCalled = true
				return &ateapipb.Actor{}, nil
			},
		}

		reconciler := &ActorTemplateReconciler{Client: fakeK8sClient, Scheme: scheme, AteClient: fakeAteClient}
		ctx := context.Background()
		req := ctrl.Request{NamespacedName: types.NamespacedName{Name: "del-nogolden", Namespace: "default"}}

		if err := fakeK8sClient.Delete(ctx, template); err != nil {
			t.Fatalf("failed to mark template for deletion: %v", err)
		}
		if _, err := reconciler.Reconcile(ctx, req); err != nil {
			t.Fatalf("Reconcile returned error: %v", err)
		}
		if deleteCalled {
			t.Errorf("DeleteActor called for a template with no GoldenActorID")
		}
		got := &atev1alpha1.ActorTemplate{}
		if err := fakeK8sClient.Get(ctx, req.NamespacedName, got); err == nil {
			t.Errorf("template still present after finalizer removal; want deleted (finalizers=%v)", got.Finalizers)
		}
	})
}

// controllerContainsFinalizer reports whether obj carries the named finalizer.
func controllerContainsFinalizer(obj metav1.Object, finalizer string) bool {
	for _, f := range obj.GetFinalizers() {
		if f == finalizer {
			return true
		}
	}
	return false
}

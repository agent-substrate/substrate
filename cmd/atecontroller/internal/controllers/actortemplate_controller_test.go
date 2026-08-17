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
				Name:      "my-template",
				Namespace: "default",
				UID:       types.UID(templateUID),
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
				Name:      "my-template-retry",
				Namespace: "default",
				UID:       types.UID(templateUID),
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

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

package workerservice

import (
	"context"
	"testing"

	"github.com/google/go-cmp/cmp"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/testing/protocmp"

	"go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/cmd/ateapi/internal/ateletauth/ateletauthtest"
	"go.ate.dev/substrate/cmd/ateapi/internal/store"
	"go.ate.dev/substrate/cmd/ateapi/internal/store/storetest"
	"go.ate.dev/substrate/internal/apierror"
	"go.ate.dev/substrate/internal/resources"
)

// seedAllocatedWorker registers a Worker on testNode hosting one Actor that
// books 1 CPU and 1Gi.
func seedAllocatedWorker(t *testing.T, st store.Interface) {
	t.Helper()
	seedReportedWorker(t, st, testNode, &ateapi.WorkerResources{CpuMillicores: 4000, MemoryBytes: 8<<30, Actors: 4})
	err := st.BindActorToWorker(context.Background(), testWorkerName, &ateapi.ActorAssignment{
		Actor:     &ateapi.ObjectRef{Atespace: "demo", Name: "actor-a"},
		ActorUid:  "uid-a",
		Resources: resources.CPUMemory(1000, 1<<30),
	}, nil)
	if err != nil {
		t.Fatalf("BindActorToWorker: %v", err)
	}
}

func workerAllocated(t *testing.T, st store.Interface) *ateapi.WorkerResources {
	t.Helper()
	w, err := st.GetWorker(context.Background(), testWorkerName)
	if err != nil {
		t.Fatalf("GetWorker: %v", err)
	}
	return w.GetStatus().GetAllocated()
}

func TestAddWorkerAllocated(t *testing.T) {
	st, cleanup := storetest.SetupTestStore(t)
	defer cleanup()
	s := New(st, &fakeControl{}, testAteletSPIFFEID, nil, "", nil)
	seedAllocatedWorker(t, st)

	_, err := s.AddWorkerAllocated(ateletauthtest.ContextWith(ateletauthtest.CertOn(t, testNode)), &ateapi.AddWorkerAllocatedRequest{
		Worker:        &ateapi.ObjectRef{Name: testWorkerName},
		ResourceDelta: resources.CPUMemory(500, 1<<30),
	})
	if err != nil {
		t.Fatalf("AddWorkerAllocated() failed: %v", err)
	}
	want := &ateapi.WorkerResources{CpuMillicores: 1500, MemoryBytes: 2<<30, Actors: 1}
	if diff := cmp.Diff(want, workerAllocated(t, st), protocmp.Transform()); diff != "" {
		t.Errorf("allocated mismatch (-want +got):\n%s", diff)
	}
}

// Handing back more than is booked clamps at zero; the actor count is left
// alone.
func TestSubtractWorkerAllocated(t *testing.T) {
	st, cleanup := storetest.SetupTestStore(t)
	defer cleanup()
	s := New(st, &fakeControl{}, testAteletSPIFFEID, nil, "", nil)
	seedAllocatedWorker(t, st)

	_, err := s.SubtractWorkerAllocated(ateletauthtest.ContextWith(ateletauthtest.CertOn(t, testNode)), &ateapi.SubtractWorkerAllocatedRequest{
		Worker:        &ateapi.ObjectRef{Name: testWorkerName},
		ResourceDelta: resources.CPUMemory(2000, 512<<20),
	})
	if err != nil {
		t.Fatalf("SubtractWorkerAllocated() failed: %v", err)
	}
	want := &ateapi.WorkerResources{MemoryBytes: 512<<20, Actors: 1}
	if diff := cmp.Diff(want, workerAllocated(t, st), protocmp.Transform()); diff != "" {
		t.Errorf("allocated mismatch (-want +got):\n%s", diff)
	}
}

// Both RPCs share one path, so each error case is run against both.
func TestAdjustWorkerAllocated_Errors(t *testing.T) {
	st, cleanup := storetest.SetupTestStore(t)
	defer cleanup()
	s := New(st, &fakeControl{}, testAteletSPIFFEID, nil, "", nil)
	seedAllocatedWorker(t, st)
	authed := ateletauthtest.ContextWith(ateletauthtest.CertOn(t, testNode))
	worker := &ateapi.ObjectRef{Name: testWorkerName}
	delta := resources.CPUMemory(100, 0)

	rpcs := map[string]func(context.Context, *ateapi.ObjectRef, *ateapi.Resources) error{
		"add": func(ctx context.Context, w *ateapi.ObjectRef, d *ateapi.Resources) error {
			_, err := s.AddWorkerAllocated(ctx, &ateapi.AddWorkerAllocatedRequest{Worker: w, ResourceDelta: d})
			return err
		},
		"subtract": func(ctx context.Context, w *ateapi.ObjectRef, d *ateapi.Resources) error {
			_, err := s.SubtractWorkerAllocated(ctx, &ateapi.SubtractWorkerAllocatedRequest{Worker: w, ResourceDelta: d})
			return err
		},
	}
	tests := []struct {
		name   string
		ctx    context.Context
		worker *ateapi.ObjectRef
		delta  *ateapi.Resources
		want   codes.Code
	}{
		{"unauthenticated", ateletauthtest.ContextWith(nil), worker, delta, codes.Unauthenticated},
		{"no worker ref", authed, nil, delta, codes.InvalidArgument},
		{"worker ref with atespace", authed, &ateapi.ObjectRef{Atespace: "demo", Name: testWorkerName}, delta, codes.InvalidArgument},
		{"no delta", authed, worker, nil, codes.InvalidArgument},
		{"negative quantity", authed, worker, resources.CPUMemory(-100, 0), codes.InvalidArgument},
		{"absent worker", authed, &ateapi.ObjectRef{Name: "3b9f1e77-2c4d-4a80-91be-6d5c8f0a7e21"}, delta, codes.NotFound},
		{"worker on another node", ateletauthtest.ContextWith(ateletauthtest.CertOn(t, "some-other-node")), worker, delta, codes.NotFound},
	}
	for rpc, call := range rpcs {
		for _, tc := range tests {
			t.Run(rpc+"/"+tc.name, func(t *testing.T) {
				if got := apierror.Code(call(tc.ctx, tc.worker, tc.delta)); got != tc.want {
					t.Errorf("code = %v, want %v", got, tc.want)
				}
			})
		}
	}

	// None of the refused requests may have landed.
	want := &ateapi.WorkerResources{CpuMillicores: 1000, MemoryBytes: 1<<30, Actors: 1}
	if diff := cmp.Diff(want, workerAllocated(t, st), protocmp.Transform()); diff != "" {
		t.Errorf("allocated changed despite every request being refused (-want +got):\n%s", diff)
	}
}

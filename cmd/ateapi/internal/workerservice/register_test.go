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
	"google.golang.org/protobuf/testing/protocmp"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/ateletauth/ateletauthtest"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store/storetest"
	"github.com/agent-substrate/substrate/internal/apierror"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc/codes"
)

// testRuntime is a SandboxRuntime of the given class whose compatibility
// identity is a single architecture attribute, which is enough to tell two
// runtimes apart.
func testRuntime(class, arch string) *ateapipb.SandboxRuntime {
	return &ateapipb.SandboxRuntime{
		SandboxClass: class,
		CompatVersion: &ateapipb.VersionedSandboxCompat{
			SchemaVersion: "v1",
			Attributes:    []*ateapipb.AttributeEntry{{Key: "architecture", Value: arch}},
		},
	}
}

// testDefaultRuntime is what seedReportedWorker records as already reported,
// of the class it gives the Worker.
var testDefaultRuntime = testRuntime("gvisor", "amd64")

func setRequest(actors int32) *ateapipb.RegisterWorkerRequest {
	return &ateapipb.RegisterWorkerRequest{
		Worker:         &ateapipb.ObjectRef{Name: testWorkerName},
		Capacity:       &ateapipb.WorkerResources{Actors: actors},
		DefaultRuntime: testDefaultRuntime,
	}
}

// The point of the whole path: a Worker moves from what it reported before to
// what its ateom reports now.
func TestRegisterWorker(t *testing.T) {
	st, cleanup := storetest.SetupTestStore(t)
	defer cleanup()
	s := New(st, &fakeSuspender{}, testAteletSPIFFEID, nil)
	seedReportedWorker(t, st, testNode, &ateapipb.WorkerResources{Actors: 1, Resources: resources.CPUMemory(2000, 0)})

	got, err := s.RegisterWorker(ateletauthtest.ContextWith(ateletauthtest.CertOn(t, testNode)), setRequest(4094))
	if err != nil {
		t.Fatalf("RegisterWorker() failed: %v", err)
	}
	if want := int32(4094); got.GetWorker().GetStatus().GetCapacity().GetActors() != want {
		t.Errorf("capacity.actors = %d, want %d", got.GetWorker().GetStatus().GetCapacity().GetActors(), want)
	}
	// A report replaces what is recorded. The Worker reports everything it has,
	// so a dimension this one leaves out is one it no longer supplies -- keeping
	// the old value would advertise compute nothing claims to have.
	if got := got.GetWorker().GetStatus().GetCapacity().GetResources(); got != nil {
		t.Errorf("capacity resources = %v, want the report's own (none)", got)
	}
}

func TestRegisterWorker_RecordsRuntimes(t *testing.T) {
	st, cleanup := storetest.SetupTestStore(t)
	defer cleanup()
	s := New(st, &fakeSuspender{}, testAteletSPIFFEID, nil)
	seedReportedWorker(t, st, testNode, &ateapipb.WorkerResources{Actors: 1})
	authed := ateletauthtest.ContextWith(ateletauthtest.CertOn(t, testNode))

	wantDefault := testRuntime("gvisor", "arm64")
	wantRestorable := []*ateapipb.SandboxRuntime{testRuntime("gvisor", "arm64-v8"), testRuntime("gvisor", "arm64-v9")}
	req := &ateapipb.RegisterWorkerRequest{
		Worker:             &ateapipb.ObjectRef{Name: testWorkerName},
		Capacity:           &ateapipb.WorkerResources{Actors: 4094},
		DefaultRuntime:     wantDefault,
		RestorableRuntimes: wantRestorable,
	}
	got, err := s.RegisterWorker(authed, req)
	if err != nil {
		t.Fatalf("RegisterWorker() failed: %v", err)
	}
	if diff := cmp.Diff(wantDefault, got.GetWorker().GetStatus().GetDefaultRuntime(), protocmp.Transform()); diff != "" {
		t.Errorf("default_runtime mismatch (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff(wantRestorable, got.GetWorker().GetStatus().GetRestorableRuntimes(), protocmp.Transform()); diff != "" {
		t.Errorf("restorable_runtimes mismatch (-want +got):\n%s", diff)
	}

	// Repeating the identical capacity and runtimes must not bump version.
	v1 := got.GetWorker().GetMetadata().GetVersion()
	again, err := s.RegisterWorker(authed, req)
	if err != nil {
		t.Fatalf("RegisterWorker() repeat failed: %v", err)
	}
	if gotV := again.GetWorker().GetMetadata().GetVersion(); gotV != v1 {
		t.Errorf("version = %d after identical capacity+runtimes report, want %d unchanged", gotV, v1)
	}

	// Runtimes are replaced like capacity: a report that lists no restorable
	// runtimes is a Worker that can restore from none but its default, as when
	// a version is disabled, and the record must not keep advertising it.
	req.RestorableRuntimes = nil
	got, err = s.RegisterWorker(authed, req)
	if err != nil {
		t.Fatalf("RegisterWorker() without restorable runtimes failed: %v", err)
	}
	if got := got.GetWorker().GetStatus().GetRestorableRuntimes(); len(got) != 0 {
		t.Errorf("restorable_runtimes = %v after a report without any, want none", got)
	}
}

// A Worker's sandbox class is fixed by the pool that created it. A runtime of
// another class is one the Worker cannot run, so the report is refused rather
// than recorded for the scheduler to act on.
func TestRegisterWorker_RejectsRuntimeOfAnotherClass(t *testing.T) {
	st, cleanup := storetest.SetupTestStore(t)
	defer cleanup()
	s := New(st, &fakeSuspender{}, testAteletSPIFFEID, nil)
	seeded := seedReportedWorker(t, st, testNode, &ateapipb.WorkerResources{Actors: 1})
	authed := ateletauthtest.ContextWith(ateletauthtest.CertOn(t, testNode))

	for _, tc := range []struct {
		name string
		req  *ateapipb.RegisterWorkerRequest
	}{{
		name: "default runtime",
		req: &ateapipb.RegisterWorkerRequest{
			Worker:         &ateapipb.ObjectRef{Name: testWorkerName},
			Capacity:       &ateapipb.WorkerResources{Actors: 2},
			DefaultRuntime: testRuntime("microvm", "amd64"),
		},
	}, {
		name: "restorable runtime",
		req: &ateapipb.RegisterWorkerRequest{
			Worker:             &ateapipb.ObjectRef{Name: testWorkerName},
			Capacity:           &ateapipb.WorkerResources{Actors: 2},
			DefaultRuntime:     testDefaultRuntime,
			RestorableRuntimes: []*ateapipb.SandboxRuntime{testRuntime("gvisor", "arm64"), testRuntime("microvm", "amd64")},
		},
	}} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := s.RegisterWorker(authed, tc.req)
			if got := apierror.Code(err); got != codes.FailedPrecondition {
				t.Fatalf("code = %v (err %v), want %v", got, err, codes.FailedPrecondition)
			}
		})
	}

	after, err := st.GetWorker(context.Background(), testWorkerName)
	if err != nil {
		t.Fatalf("GetWorker: %v", err)
	}
	if diff := cmp.Diff(seeded.GetStatus(), after.GetStatus(), protocmp.Transform()); diff != "" {
		t.Errorf("status changed despite every report being refused (-want +got):\n%s", diff)
	}
}

// A Worker recorded without a sandbox class has nothing for a report to
// contradict, so any class is accepted.
func TestRegisterWorker_UnclassedWorkerAcceptsAnyClass(t *testing.T) {
	st, cleanup := storetest.SetupTestStore(t)
	defer cleanup()
	s := New(st, &fakeSuspender{}, testAteletSPIFFEID, nil)
	if _, err := st.CreateWorker(context.Background(), &ateapipb.Worker{
		Metadata:        &ateapipb.ResourceMetadata{Name: testWorkerName},
		WorkerNamespace: "ate-system",
		WorkerPool:      "pool-1",
		WorkerPod:       "worker-pod-1",
		WorkerPodUid:    testWorkerName,
		NodeName:        testNode,
		Ips:             []string{"10.1.2.3"},
		Status:          &ateapipb.WorkerStatus{State: ateapipb.WorkerState_WORKER_STATE_ACTIVE},
	}); err != nil {
		t.Fatalf("seeding worker: %v", err)
	}

	want := testRuntime("microvm", "amd64")
	got, err := s.RegisterWorker(ateletauthtest.ContextWith(ateletauthtest.CertOn(t, testNode)), &ateapipb.RegisterWorkerRequest{
		Worker:         &ateapipb.ObjectRef{Name: testWorkerName},
		Capacity:       &ateapipb.WorkerResources{Actors: 2},
		DefaultRuntime: want,
	})
	if err != nil {
		t.Fatalf("RegisterWorker() failed: %v", err)
	}
	if diff := cmp.Diff(want, got.GetWorker().GetStatus().GetDefaultRuntime(), protocmp.Transform()); diff != "" {
		t.Errorf("default_runtime mismatch (-want +got):\n%s", diff)
	}
}

// An atelet speaks for the Workers it herds and no others. A Worker on another
// node is reported as absent rather than forbidden, so a caller learns nothing
// about what runs elsewhere.
func TestRegisterWorker_OtherNodeIsNotFound(t *testing.T) {
	st, cleanup := storetest.SetupTestStore(t)
	defer cleanup()
	s := New(st, &fakeSuspender{}, testAteletSPIFFEID, nil)
	seedReportedWorker(t, st, testNode, &ateapipb.WorkerResources{Actors: 1})

	_, err := s.RegisterWorker(ateletauthtest.ContextWith(ateletauthtest.CertOn(t, "some-other-node")), setRequest(4094))
	if got := apierror.Code(err); got != codes.NotFound {
		t.Fatalf("code = %v (err %v), want NotFound", got, err)
	}

	// And the report must not have landed.
	after, err := st.GetWorker(context.Background(), testWorkerName)
	if err != nil {
		t.Fatalf("GetWorker: %v", err)
	}
	if got := after.GetStatus().GetCapacity().GetActors(); got != 1 {
		t.Errorf("capacity.actors = %d, want 1 unchanged", got)
	}
}

// Re-sending the same capacity is not an update. An ateom reports once, but it
// retries until accepted and reports again if it restarts, so a repeat must not
// churn the Worker's version.
func TestRegisterWorker_UnchangedDoesNotWrite(t *testing.T) {
	st, cleanup := storetest.SetupTestStore(t)
	defer cleanup()
	s := New(st, &fakeSuspender{}, testAteletSPIFFEID, nil)
	seeded := seedReportedWorker(t, st, testNode, &ateapipb.WorkerResources{Actors: 4094})

	for range 3 {
		if _, err := s.RegisterWorker(ateletauthtest.ContextWith(ateletauthtest.CertOn(t, testNode)), setRequest(4094)); err != nil {
			t.Fatalf("RegisterWorker() failed: %v", err)
		}
	}
	after, err := st.GetWorker(context.Background(), testWorkerName)
	if err != nil {
		t.Fatalf("GetWorker: %v", err)
	}
	if got, want := after.GetMetadata().GetVersion(), seeded.GetMetadata().GetVersion(); got != want {
		t.Errorf("version = %d after three identical reports, want %d unchanged", got, want)
	}
}

func TestRegisterWorker_Errors(t *testing.T) {
	st, cleanup := storetest.SetupTestStore(t)
	defer cleanup()
	s := New(st, &fakeSuspender{}, testAteletSPIFFEID, nil)
	seedReportedWorker(t, st, testNode, &ateapipb.WorkerResources{Actors: 1})
	authed := ateletauthtest.ContextWith(ateletauthtest.CertOn(t, testNode))

	tests := []struct {
		name string
		ctx  context.Context
		req  *ateapipb.RegisterWorkerRequest
		want codes.Code
	}{
		{"unauthenticated", ateletauthtest.ContextWith(nil), setRequest(2), codes.Unauthenticated},
		{"no worker ref", authed, &ateapipb.RegisterWorkerRequest{
			Capacity:       &ateapipb.WorkerResources{Actors: 2},
			DefaultRuntime: testDefaultRuntime,
		}, codes.InvalidArgument},
		{"no capacity", authed, &ateapipb.RegisterWorkerRequest{
			Worker:         &ateapipb.ObjectRef{Name: testWorkerName},
			DefaultRuntime: testDefaultRuntime,
		}, codes.InvalidArgument},
		{"no default runtime", authed, &ateapipb.RegisterWorkerRequest{
			Worker:   &ateapipb.ObjectRef{Name: testWorkerName},
			Capacity: &ateapipb.WorkerResources{Actors: 2},
		}, codes.InvalidArgument},
		{"absent worker", authed, &ateapipb.RegisterWorkerRequest{
			Worker:         &ateapipb.ObjectRef{Name: "3b9f1e77-2c4d-4a80-91be-6d5c8f0a7e21"},
			Capacity:       &ateapipb.WorkerResources{Actors: 2},
			DefaultRuntime: testDefaultRuntime,
		}, codes.NotFound},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := s.RegisterWorker(tc.ctx, tc.req)
			if got := apierror.Code(err); got != tc.want {
				t.Errorf("code = %v (err %v), want %v", got, err, tc.want)
			}
		})
	}
}

// A report goes straight to the store, so nothing else checks it. A negative
// ceiling is the case that matters: placement asks whether allocated is below
// capacity, so the Worker would take no Actor ever again.
func TestRegisterWorker_RejectsNonsense(t *testing.T) {
	st, cleanup := storetest.SetupTestStore(t)
	defer cleanup()
	s := New(st, &fakeSuspender{}, testAteletSPIFFEID, nil)
	seeded := seedReportedWorker(t, st, testNode, &ateapipb.WorkerResources{Actors: 4094})
	authed := ateletauthtest.ContextWith(ateletauthtest.CertOn(t, testNode))

	for _, tc := range []struct {
		name     string
		capacity *ateapipb.WorkerResources
	}{
		{"negative ceiling", &ateapipb.WorkerResources{Actors: -1}},
		{"int32 underflow", &ateapipb.WorkerResources{Actors: -2147483648}},
		{"negative quantity", &ateapipb.WorkerResources{Resources: resources.CPUMemory(-1, 0)}},
		{"unparseable quantity", &ateapipb.WorkerResources{
			Resources: &ateapipb.Resources{Limits: []*ateapipb.Limits{{Name: "cpu", Quantity: "lots"}}},
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := s.RegisterWorker(authed, &ateapipb.RegisterWorkerRequest{
				Worker:         &ateapipb.ObjectRef{Name: testWorkerName},
				Capacity:       tc.capacity,
				DefaultRuntime: testDefaultRuntime,
			})
			if got := apierror.Code(err); got != codes.InvalidArgument {
				t.Fatalf("code = %v (err %v), want %v", got, err, codes.InvalidArgument)
			}
		})
	}

	after, err := st.GetWorker(context.Background(), testWorkerName)
	if err != nil {
		t.Fatalf("GetWorker: %v", err)
	}
	if diff := cmp.Diff(seeded.GetStatus().GetCapacity(), after.GetStatus().GetCapacity(), protocmp.Transform()); diff != "" {
		t.Errorf("capacity changed despite every report being refused (-want +got):\n%s", diff)
	}
}

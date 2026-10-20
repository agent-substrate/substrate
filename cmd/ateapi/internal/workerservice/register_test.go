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
	"fmt"
	"testing"

	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/testing/protocmp"

	"go.ate.dev/substrate/cmd/ateapi/internal/ateletauth/ateletauthtest"
	"go.ate.dev/substrate/cmd/ateapi/internal/store/storetest"
	"go.ate.dev/substrate/internal/apierror"
	"go.ate.dev/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc/codes"
)

// testCompat is a compatibility version whose identity is a single
// architecture attribute, which is enough to tell two runtimes apart.
func testCompat(arch string) *ateapipb.VersionedSandboxCompat {
	return &ateapipb.VersionedSandboxCompat{
		SchemaVersion: "v1",
		Attributes:    []*ateapipb.AttributeEntry{{Key: "architecture", Value: arch}},
	}
}

// testClass is one reported sandbox class: def as its default runtime and
// restorable as the others.
func testClass(name string, def *ateapipb.VersionedSandboxCompat, restorable ...*ateapipb.VersionedSandboxCompat) *ateapipb.WorkerSandboxClass {
	c := &ateapipb.WorkerSandboxClass{SandboxClass: name, DefaultRuntime: &ateapipb.WorkerSandboxRuntime{Version: def}}
	for _, r := range restorable {
		c.RestorableRuntimes = append(c.RestorableRuntimes, &ateapipb.WorkerSandboxRuntime{Version: r})
	}
	return c
}

// testSandboxClasses is what seedReportedWorker records as already reported:
// the class it gives the Worker.
var testSandboxClasses = []*ateapipb.WorkerSandboxClass{testClass("gvisor", testCompat("amd64"))}

func setRequest(actors int32) *ateapipb.RegisterWorkerRequest {
	return &ateapipb.RegisterWorkerRequest{
		Worker:         &ateapipb.ObjectRef{Name: testWorkerName},
		Capacity:       &ateapipb.WorkerResources{Actors: actors},
		SandboxClasses: testSandboxClasses,
	}
}

// The point of the whole path: a Worker moves from what it reported before to
// what its worker reports now.
func TestRegisterWorker(t *testing.T) {
	st, cleanup := storetest.SetupTestStore(t)
	defer cleanup()
	s := New(st, &fakeControl{}, testAteletSPIFFEID, nil, "", nil)
	seedReportedWorker(t, st, testNode, &ateapipb.WorkerResources{Actors: 1, CpuMillicores: 2000, MemoryBytes: 0})

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
	if c := got.GetWorker().GetStatus().GetCapacity(); c.GetCpuMillicores() != 0 || c.GetMemoryBytes() != 0 {
		t.Errorf("capacity = %v, want the report's own (no cpu or memory)", c)
	}
}

// A report's classes are checked against the class the Worker recorded at its
// first registration and recorded only when they differ from what is stored.
// Capacity is held at what was seeded, so a write here is one the classes
// alone caused.
func TestRegisterWorker_SandboxClasses(t *testing.T) {
	st, cleanup := storetest.SetupTestStore(t)
	defer cleanup()
	s := New(st, &fakeControl{}, testAteletSPIFFEID, nil, "", nil)
	authed := ateletauthtest.ContextWith(ateletauthtest.CertOn(t, testNode))

	amd64, arm64 := testCompat("amd64"), testCompat("arm64")
	v8, v9 := testCompat("arm64-v8"), testCompat("arm64-v9")
	// Every Worker here has already reported gvisor with amd64 as its default
	// and v8 and v9 restorable, in that order, at a capacity of one actor.
	seeded := func() *ateapipb.WorkerStatus {
		return &ateapipb.WorkerStatus{
			State:          ateapipb.WorkerState_WORKER_STATE_ACTIVE,
			Capacity:       &ateapipb.WorkerResources{Actors: 1},
			SandboxClasses: []*ateapipb.WorkerSandboxClass{testClass("gvisor", amd64, v8, v9)},
		}
	}

	tests := []struct {
		name string
		// unreported seeds a Worker that has not registered yet, with no
		// sandbox classes, instead of seeded().
		unreported bool
		classes    []*ateapipb.WorkerSandboxClass
		wantCode   codes.Code
		// wantWrite is whether the report changes the record.
		wantWrite bool
	}{{
		name:    "identical report writes nothing",
		classes: []*ateapipb.WorkerSandboxClass{testClass("gvisor", amd64, v8, v9)},
	}, {
		name:      "changed default runtime writes",
		classes:   []*ateapipb.WorkerSandboxClass{testClass("gvisor", arm64, v8, v9)},
		wantWrite: true,
	}, {
		name:      "changed restorable runtime writes",
		classes:   []*ateapipb.WorkerSandboxClass{testClass("gvisor", amd64, v8, arm64)},
		wantWrite: true,
	}, {
		// A report is compared as sent: the list is atomic, so a new order
		// is a new report.
		name:      "reordered restorable runtimes write",
		classes:   []*ateapipb.WorkerSandboxClass{testClass("gvisor", amd64, v9, v8)},
		wantWrite: true,
	}, {
		// Runtimes are replaced like capacity: a report that lists no
		// restorable runtimes is a Worker that can restore from none but its
		// default, as when a version is disabled, and the record must not
		// keep advertising them.
		name:      "dropped restorable runtimes write",
		classes:   []*ateapipb.WorkerSandboxClass{testClass("gvisor", amd64)},
		wantWrite: true,
	}, {
		// Once a Worker has registered a sandbox class, an entry of another
		// class contradicts its recorded state and is refused.
		name:     "another class is refused",
		classes:  []*ateapipb.WorkerSandboxClass{testClass("microvm", amd64, v8, v9)},
		wantCode: codes.FailedPrecondition,
	}, {
		// A pool declares one class, so does a Worker: the list's size is an
		// API rule, checked before the recorded class is.
		name:     "a second class is refused",
		classes:  []*ateapipb.WorkerSandboxClass{testClass("gvisor", amd64, v8, v9), testClass("microvm", amd64)},
		wantCode: codes.InvalidArgument,
	}, {
		// A Worker that has not registered yet accepts any class on its first
		// report.
		name:       "unreported Worker takes a class of any name",
		unreported: true,
		classes:    []*ateapipb.WorkerSandboxClass{testClass("microvm", amd64)},
		wantWrite:  true,
	}, {
		name:     "a report of no classes is refused",
		wantCode: codes.InvalidArgument,
	}}
	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			name := fmt.Sprintf("1a7e4c83-6d20-4f95-b3c8-%012d", i)
			initialStatus := seeded()
			if tt.unreported {
				initialStatus = &ateapipb.WorkerStatus{State: ateapipb.WorkerState_WORKER_STATE_ACTIVE}
			}
			before := seedWorker(t, st, name, testNode, initialStatus)

			got, err := s.RegisterWorker(authed, &ateapipb.RegisterWorkerRequest{
				Worker:         &ateapipb.ObjectRef{Name: name},
				Capacity:       &ateapipb.WorkerResources{Actors: 1},
				SandboxClasses: tt.classes,
			})
			if code := apierror.Code(err); code != tt.wantCode {
				t.Fatalf("code = %v (err %v), want %v", code, err, tt.wantCode)
			}
			after, err := st.GetWorker(context.Background(), name)
			if err != nil {
				t.Fatalf("GetWorker: %v", err)
			}
			if wrote := after.GetMetadata().GetVersion() != before.GetMetadata().GetVersion(); wrote != tt.wantWrite {
				t.Fatalf("version %d -> %d, want a write: %v", before.GetMetadata().GetVersion(), after.GetMetadata().GetVersion(), tt.wantWrite)
			}
			if !tt.wantWrite {
				if diff := cmp.Diff(before.GetStatus(), after.GetStatus(), protocmp.Transform()); diff != "" {
					t.Errorf("status changed without a write (-want +got):\n%s", diff)
				}
				return
			}
			if diff := cmp.Diff(tt.classes, after.GetStatus().GetSandboxClasses(), protocmp.Transform()); diff != "" {
				t.Errorf("sandbox_classes mismatch (-want +got):\n%s", diff)
			}
			if diff := cmp.Diff(after, got.GetWorker(), protocmp.Transform()); diff != "" {
				t.Errorf("response is not the stored Worker (-want +got):\n%s", diff)
			}
		})
	}
}

// An atelet speaks for the Workers it herds and no others. A Worker on another
// node is reported as absent rather than forbidden, so a caller learns nothing
// about what runs elsewhere.
func TestRegisterWorker_OtherNodeIsNotFound(t *testing.T) {
	st, cleanup := storetest.SetupTestStore(t)
	defer cleanup()
	s := New(st, &fakeControl{}, testAteletSPIFFEID, nil, "", nil)
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

// Re-sending the same capacity is not an update. A worker reports once, but it
// retries until accepted and reports again if it restarts, so a repeat must not
// churn the Worker's version.
func TestRegisterWorker_UnchangedDoesNotWrite(t *testing.T) {
	st, cleanup := storetest.SetupTestStore(t)
	defer cleanup()
	s := New(st, &fakeControl{}, testAteletSPIFFEID, nil, "", nil)
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

// Attribute order is the producer's business, not the record's: the same
// attributes in another order are the same runtime, so they are stored sorted
// by key and a reordered repeat writes nothing.
func TestRegisterWorker_SortsAttributes(t *testing.T) {
	st, cleanup := storetest.SetupTestStore(t)
	defer cleanup()
	s := New(st, &fakeControl{}, testAteletSPIFFEID, nil, "", nil)
	seedReportedWorker(t, st, testNode, &ateapipb.WorkerResources{Actors: 1})
	authed := ateletauthtest.ContextWith(ateletauthtest.CertOn(t, testNode))

	compatWith := func(keys ...string) *ateapipb.VersionedSandboxCompat {
		c := &ateapipb.VersionedSandboxCompat{SchemaVersion: "v1"}
		for _, k := range keys {
			c.Attributes = append(c.Attributes, &ateapipb.AttributeEntry{Key: k, Value: "v"})
		}
		return c
	}
	register := func(def, restorable *ateapipb.VersionedSandboxCompat) *ateapipb.Worker {
		t.Helper()
		got, err := s.RegisterWorker(authed, &ateapipb.RegisterWorkerRequest{
			Worker:         &ateapipb.ObjectRef{Name: testWorkerName},
			Capacity:       &ateapipb.WorkerResources{Actors: 1},
			SandboxClasses: []*ateapipb.WorkerSandboxClass{testClass("gvisor", def, restorable)},
		})
		if err != nil {
			t.Fatalf("RegisterWorker() failed: %v", err)
		}
		return got.GetWorker()
	}

	first := register(compatWith("cpu_features", "architecture"), compatWith("sandbox_bundle_hash", "architecture"))
	want := []*ateapipb.WorkerSandboxClass{testClass("gvisor", compatWith("architecture", "cpu_features"), compatWith("architecture", "sandbox_bundle_hash"))}
	if diff := cmp.Diff(want, first.GetStatus().GetSandboxClasses(), protocmp.Transform()); diff != "" {
		t.Errorf("sandbox_classes not stored sorted by key (-want +got):\n%s", diff)
	}

	again := register(compatWith("architecture", "cpu_features"), compatWith("architecture", "sandbox_bundle_hash"))
	if got, want := again.GetMetadata().GetVersion(), first.GetMetadata().GetVersion(); got != want {
		t.Errorf("version = %d after a reordered repeat, want %d unchanged", got, want)
	}
}

func TestRegisterWorker_Errors(t *testing.T) {
	st, cleanup := storetest.SetupTestStore(t)
	defer cleanup()
	s := New(st, &fakeControl{}, testAteletSPIFFEID, nil, "", nil)
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
			SandboxClasses: testSandboxClasses,
		}, codes.InvalidArgument},
		{"no capacity", authed, &ateapipb.RegisterWorkerRequest{
			Worker:         &ateapipb.ObjectRef{Name: testWorkerName},
			SandboxClasses: testSandboxClasses,
		}, codes.InvalidArgument},
		{"no sandbox classes", authed, &ateapipb.RegisterWorkerRequest{
			Worker:   &ateapipb.ObjectRef{Name: testWorkerName},
			Capacity: &ateapipb.WorkerResources{Actors: 2},
		}, codes.InvalidArgument},
		{"class without a default runtime", authed, &ateapipb.RegisterWorkerRequest{
			Worker:         &ateapipb.ObjectRef{Name: testWorkerName},
			Capacity:       &ateapipb.WorkerResources{Actors: 2},
			SandboxClasses: []*ateapipb.WorkerSandboxClass{{SandboxClass: "gvisor"}},
		}, codes.InvalidArgument},
		{"class that is not a sandbox class", authed, &ateapipb.RegisterWorkerRequest{
			Worker:         &ateapipb.ObjectRef{Name: testWorkerName},
			Capacity:       &ateapipb.WorkerResources{Actors: 2},
			SandboxClasses: []*ateapipb.WorkerSandboxClass{testClass("plain", testCompat("amd64"))},
		}, codes.InvalidArgument},
		{"absent worker", authed, &ateapipb.RegisterWorkerRequest{
			Worker:         &ateapipb.ObjectRef{Name: "3b9f1e77-2c4d-4a80-91be-6d5c8f0a7e21"},
			Capacity:       &ateapipb.WorkerResources{Actors: 2},
			SandboxClasses: testSandboxClasses,
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
	s := New(st, &fakeControl{}, testAteletSPIFFEID, nil, "", nil)
	seeded := seedReportedWorker(t, st, testNode, &ateapipb.WorkerResources{Actors: 4094})
	authed := ateletauthtest.ContextWith(ateletauthtest.CertOn(t, testNode))

	for _, tc := range []struct {
		name     string
		capacity *ateapipb.WorkerResources
	}{
		{"negative ceiling", &ateapipb.WorkerResources{Actors: -1}},
		{"int32 underflow", &ateapipb.WorkerResources{Actors: -2147483648}},
		{"negative quantity", &ateapipb.WorkerResources{CpuMillicores: -1, MemoryBytes: 0}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := s.RegisterWorker(authed, &ateapipb.RegisterWorkerRequest{
				Worker:         &ateapipb.ObjectRef{Name: testWorkerName},
				Capacity:       tc.capacity,
				SandboxClasses: testSandboxClasses,
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

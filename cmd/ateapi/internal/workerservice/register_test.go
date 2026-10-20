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

func registerRequest(epoch int64, actors int32) *ateapipb.RegisterWorkerRequest {
	return &ateapipb.RegisterWorkerRequest{
		Worker:   &ateapipb.ObjectRef{Name: testWorkerName},
		Epoch:    epoch,
		Ips:      testIPs,
		Capacity: &ateapipb.WorkerResources{Actors: actors},
	}
}

func runtimesRequest(epoch int64) *ateapipb.SetWorkerSandboxRuntimesRequest {
	return &ateapipb.SetWorkerSandboxRuntimesRequest{
		Worker:         &ateapipb.ObjectRef{Name: testWorkerName},
		Epoch:          epoch,
		SandboxClasses: testSandboxClasses,
	}
}

func mustGetWorker(t *testing.T, s *Server, name string) *ateapipb.Worker {
	t.Helper()
	w, err := s.store.GetWorker(context.Background(), name)
	if err != nil {
		t.Fatalf("GetWorker: %v", err)
	}
	return w
}

// Each acquire hands out the next epoch, and none of them becomes active: a
// worker process that never registers changes nothing the control plane acts
// on.
func TestAcquireWorkerEpoch(t *testing.T) {
	st, cleanup := storetest.SetupTestStore(t)
	defer cleanup()
	s := New(st, &fakeControl{}, testAteletSPIFFEID, nil, "", nil)
	seedReportedWorker(t, st, testNode, &ateapipb.WorkerResources{Actors: 1})
	authed := ateletauthtest.ContextWith(ateletauthtest.CertOn(t, testNode))

	for want := int64(testEpoch + 1); want <= testEpoch+2; want++ {
		got, err := s.AcquireWorkerEpoch(authed, &ateapipb.AcquireWorkerEpochRequest{Worker: &ateapipb.ObjectRef{Name: testWorkerName}})
		if err != nil {
			t.Fatalf("AcquireWorkerEpoch() failed: %v", err)
		}
		if got.GetEpoch() != want {
			t.Errorf("epoch = %d, want %d", got.GetEpoch(), want)
		}
	}
	after := mustGetWorker(t, s, testWorkerName)
	if got, want := after.GetStatus().GetIssuedEpoch(), int64(testEpoch+2); got != want {
		t.Errorf("issued_epoch = %d, want %d", got, want)
	}
	if got := after.GetStatus().GetRegisteredEpoch(); got != testEpoch {
		t.Errorf("registered_epoch = %d, want %d unchanged", got, testEpoch)
	}
}

func TestAcquireWorkerEpoch_NewWorker(t *testing.T) {
	st, cleanup := storetest.SetupTestStore(t)
	defer cleanup()
	s := New(st, &fakeControl{}, testAteletSPIFFEID, nil, "", nil)
	seedWorker(t, st, testWorkerName, testNode, &ateapipb.WorkerStatus{State: ateapipb.WorkerState_WORKER_STATE_UNAVAILABLE})
	authed := ateletauthtest.ContextWith(ateletauthtest.CertOn(t, testNode))

	got, err := s.AcquireWorkerEpoch(authed, &ateapipb.AcquireWorkerEpochRequest{Worker: &ateapipb.ObjectRef{Name: testWorkerName}})
	if err != nil {
		t.Fatalf("AcquireWorkerEpoch() failed: %v", err)
	}
	if got.GetEpoch() != 1 {
		t.Errorf("epoch = %d, want 1", got.GetEpoch())
	}
}

func TestAcquireWorkerEpoch_Errors(t *testing.T) {
	st, cleanup := storetest.SetupTestStore(t)
	defer cleanup()
	s := New(st, &fakeControl{}, testAteletSPIFFEID, nil, "", nil)
	seedReportedWorker(t, st, testNode, &ateapipb.WorkerResources{Actors: 1})
	authed := ateletauthtest.ContextWith(ateletauthtest.CertOn(t, testNode))
	ref := &ateapipb.ObjectRef{Name: testWorkerName}

	tests := []struct {
		name string
		ctx  context.Context
		req  *ateapipb.AcquireWorkerEpochRequest
		want codes.Code
	}{
		{"unauthenticated", ateletauthtest.ContextWith(nil), &ateapipb.AcquireWorkerEpochRequest{Worker: ref}, codes.Unauthenticated},
		{"no worker ref", authed, &ateapipb.AcquireWorkerEpochRequest{}, codes.InvalidArgument},
		{"other node", ateletauthtest.ContextWith(ateletauthtest.CertOn(t, "some-other-node")), &ateapipb.AcquireWorkerEpochRequest{Worker: ref}, codes.NotFound},
		{"absent worker", authed, &ateapipb.AcquireWorkerEpochRequest{Worker: &ateapipb.ObjectRef{Name: "3b9f1e77-2c4d-4a80-91be-6d5c8f0a7e21"}}, codes.NotFound},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := s.AcquireWorkerEpoch(tc.ctx, tc.req)
			if got := apierror.Code(err); got != tc.want {
				t.Errorf("code = %v (err %v), want %v", got, err, tc.want)
			}
		})
	}
	if got := mustGetWorker(t, s, testWorkerName).GetStatus().GetIssuedEpoch(); got != testEpoch {
		t.Errorf("issued_epoch = %d after refused acquires, want %d unchanged", got, testEpoch)
	}
}

// The point of the whole path: a new worker process acquires an epoch and
// registers with it, which activates the epoch and replaces what the previous
// process reported.
func TestRegisterWorker(t *testing.T) {
	st, cleanup := storetest.SetupTestStore(t)
	defer cleanup()
	s := New(st, &fakeControl{}, testAteletSPIFFEID, nil, "", nil)
	seedReportedWorker(t, st, testNode, &ateapipb.WorkerResources{Actors: 1, CpuMillicores: 2000, MemoryBytes: 0})
	authed := ateletauthtest.ContextWith(ateletauthtest.CertOn(t, testNode))

	acquired, err := s.AcquireWorkerEpoch(authed, &ateapipb.AcquireWorkerEpochRequest{Worker: &ateapipb.ObjectRef{Name: testWorkerName}})
	if err != nil {
		t.Fatalf("AcquireWorkerEpoch() failed: %v", err)
	}
	req := registerRequest(acquired.GetEpoch(), 4094)
	req.Ips = []string{"10.9.8.7", "fd00::7"}
	got, err := s.RegisterWorker(authed, req)
	if err != nil {
		t.Fatalf("RegisterWorker() failed: %v", err)
	}
	status := got.GetWorker().GetStatus()
	if status.GetRegisteredEpoch() != acquired.GetEpoch() {
		t.Errorf("registered_epoch = %d, want %d", status.GetRegisteredEpoch(), acquired.GetEpoch())
	}
	if diff := cmp.Diff(req.GetIps(), status.GetIps()); diff != "" {
		t.Errorf("ips mismatch (-want +got):\n%s", diff)
	}
	if want := int32(4094); status.GetCapacity().GetActors() != want {
		t.Errorf("capacity.actors = %d, want %d", status.GetCapacity().GetActors(), want)
	}
	// A report replaces what is recorded. The Worker reports everything it has,
	// so a dimension this one leaves out is one it no longer supplies -- keeping
	// the old value would advertise compute nothing claims to have.
	if c := status.GetCapacity(); c.GetCpuMillicores() != 0 || c.GetMemoryBytes() != 0 {
		t.Errorf("capacity = %v, want the report's own (no cpu or memory)", c)
	}
	// The sandbox classes were the previous process's; this one reports its own.
	if got := status.GetSandboxClasses(); got != nil {
		t.Errorf("sandbox_classes = %v, want cleared until the new process reports", got)
	}
	if diff := cmp.Diff(mustGetWorker(t, s, testWorkerName), got.GetWorker(), protocmp.Transform()); diff != "" {
		t.Errorf("response is not the stored Worker (-want +got):\n%s", diff)
	}
}

// Only an epoch that was handed out, and is not older than the registered one,
// registers. Every Worker here starts with testEpoch registered and two more
// acquired.
func TestRegisterWorker_Epochs(t *testing.T) {
	st, cleanup := storetest.SetupTestStore(t)
	defer cleanup()
	s := New(st, &fakeControl{}, testAteletSPIFFEID, nil, "", nil)
	authed := ateletauthtest.ContextWith(ateletauthtest.CertOn(t, testNode))
	seeded := func() *ateapipb.WorkerStatus {
		return &ateapipb.WorkerStatus{
			State:           ateapipb.WorkerState_WORKER_STATE_ACTIVE,
			IssuedEpoch:     testEpoch + 2,
			RegisteredEpoch: testEpoch,
			Ips:             testIPs,
			Capacity:        &ateapipb.WorkerResources{Actors: 1},
			SandboxClasses:  testSandboxClasses,
		}
	}

	tests := []struct {
		name     string
		epoch    int64
		ips      []string
		wantCode codes.Code
		// wantWrite is whether the registration changes the record.
		wantWrite bool
		// wantRegistered is the registered_epoch afterwards.
		wantRegistered int64
		// wantRuntimes is whether the seeded runtimes survive.
		wantRuntimes bool
	}{{
		name:           "retry of the registration writes nothing",
		epoch:          testEpoch,
		ips:            testIPs,
		wantRegistered: testEpoch,
		wantRuntimes:   true,
	}, {
		// A worker process registers once: the registered epoch's IPs and
		// capacity do not change.
		name:           "registered epoch with other IPs is refused",
		epoch:          testEpoch,
		ips:            []string{"10.9.8.7"},
		wantCode:       codes.FailedPrecondition,
		wantRegistered: testEpoch,
		wantRuntimes:   true,
	}, {
		// A later acquire may be activated before an earlier one: only the
		// highest registered epoch counts, and acquires never registered stay
		// inactive.
		name:           "highest acquired epoch activates",
		epoch:          testEpoch + 2,
		ips:            testIPs,
		wantWrite:      true,
		wantRegistered: testEpoch + 2,
	}, {
		name:           "older epoch is refused",
		epoch:          testEpoch - 1,
		ips:            testIPs,
		wantCode:       codes.FailedPrecondition,
		wantRegistered: testEpoch,
		// Not written, so the runtimes stay.
		wantRuntimes: true,
	}, {
		name:           "epoch never handed out is refused",
		epoch:          testEpoch + 3,
		ips:            testIPs,
		wantCode:       codes.FailedPrecondition,
		wantRegistered: testEpoch,
		wantRuntimes:   true,
	}}
	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			name := fmt.Sprintf("1a7e4c83-6d20-4f95-b3c8-%012d", i)
			before := seedWorker(t, st, name, testNode, seeded())

			_, err := s.RegisterWorker(authed, &ateapipb.RegisterWorkerRequest{
				Worker:   &ateapipb.ObjectRef{Name: name},
				Epoch:    tt.epoch,
				Ips:      tt.ips,
				Capacity: &ateapipb.WorkerResources{Actors: 1},
			})
			if code := apierror.Code(err); code != tt.wantCode {
				t.Fatalf("code = %v (err %v), want %v", code, err, tt.wantCode)
			}
			after := mustGetWorker(t, s, name)
			if wrote := after.GetMetadata().GetVersion() != before.GetMetadata().GetVersion(); wrote != tt.wantWrite {
				t.Errorf("version %d -> %d, want a write: %v", before.GetMetadata().GetVersion(), after.GetMetadata().GetVersion(), tt.wantWrite)
			}
			if got := after.GetStatus().GetRegisteredEpoch(); got != tt.wantRegistered {
				t.Errorf("registered_epoch = %d, want %d", got, tt.wantRegistered)
			}
			if got := len(after.GetStatus().GetSandboxClasses()) > 0; got != tt.wantRuntimes {
				t.Errorf("sandbox_classes kept = %v, want %v", got, tt.wantRuntimes)
			}
			if tt.wantCode == codes.OK {
				if diff := cmp.Diff(tt.ips, after.GetStatus().GetIps()); diff != "" {
					t.Errorf("ips mismatch (-want +got):\n%s", diff)
				}
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

	_, err := s.RegisterWorker(ateletauthtest.ContextWith(ateletauthtest.CertOn(t, "some-other-node")), registerRequest(testEpoch, 4094))
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

func TestRegisterWorker_Errors(t *testing.T) {
	st, cleanup := storetest.SetupTestStore(t)
	defer cleanup()
	s := New(st, &fakeControl{}, testAteletSPIFFEID, nil, "", nil)
	seedReportedWorker(t, st, testNode, &ateapipb.WorkerResources{Actors: 1})
	authed := ateletauthtest.ContextWith(ateletauthtest.CertOn(t, testNode))
	without := func(mutate func(*ateapipb.RegisterWorkerRequest)) *ateapipb.RegisterWorkerRequest {
		req := registerRequest(testEpoch, 2)
		mutate(req)
		return req
	}

	tests := []struct {
		name string
		ctx  context.Context
		req  *ateapipb.RegisterWorkerRequest
		want codes.Code
	}{
		{"unauthenticated", ateletauthtest.ContextWith(nil), registerRequest(testEpoch, 2), codes.Unauthenticated},
		{"no worker ref", authed, without(func(r *ateapipb.RegisterWorkerRequest) { r.Worker = nil }), codes.InvalidArgument},
		{"no epoch", authed, without(func(r *ateapipb.RegisterWorkerRequest) { r.Epoch = 0 }), codes.InvalidArgument},
		{"no ips", authed, without(func(r *ateapipb.RegisterWorkerRequest) { r.Ips = nil }), codes.InvalidArgument},
		{"no capacity", authed, without(func(r *ateapipb.RegisterWorkerRequest) { r.Capacity = nil }), codes.InvalidArgument},
		{"absent worker", authed, without(func(r *ateapipb.RegisterWorkerRequest) {
			r.Worker = &ateapipb.ObjectRef{Name: "3b9f1e77-2c4d-4a80-91be-6d5c8f0a7e21"}
		}), codes.NotFound},
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
			req := registerRequest(testEpoch, 0)
			req.Capacity = tc.capacity
			_, err := s.RegisterWorker(authed, req)
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

// A report's classes are checked against the class the registered worker
// process reported first and recorded only when they differ from what is
// stored.
func TestSetWorkerSandboxRuntimes(t *testing.T) {
	st, cleanup := storetest.SetupTestStore(t)
	defer cleanup()
	s := New(st, &fakeControl{}, testAteletSPIFFEID, nil, "", nil)
	authed := ateletauthtest.ContextWith(ateletauthtest.CertOn(t, testNode))

	amd64, arm64 := testCompat("amd64"), testCompat("arm64")
	v8, v9 := testCompat("arm64-v8"), testCompat("arm64-v9")
	// Every Worker here has registered testEpoch and reported gvisor with
	// amd64 as its default and v8 and v9 restorable, in that order.
	seeded := func() *ateapipb.WorkerStatus {
		return &ateapipb.WorkerStatus{
			State:           ateapipb.WorkerState_WORKER_STATE_ACTIVE,
			IssuedEpoch:     testEpoch + 1,
			RegisteredEpoch: testEpoch,
			Capacity:        &ateapipb.WorkerResources{Actors: 1},
			SandboxClasses:  []*ateapipb.WorkerSandboxClass{testClass("gvisor", amd64, v8, v9)},
		}
	}

	tests := []struct {
		name string
		// unreported seeds a Worker whose worker process has registered
		// testEpoch but reported no sandbox classes yet, instead of seeded().
		unreported bool
		// epoch defaults to testEpoch.
		epoch    int64
		classes  []*ateapipb.WorkerSandboxClass
		wantCode codes.Code
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
		// A report from a worker process that has since been replaced, or
		// one that has not registered yet, describes no running process.
		name:     "older epoch is refused",
		epoch:    testEpoch - 1,
		classes:  []*ateapipb.WorkerSandboxClass{testClass("gvisor", arm64)},
		wantCode: codes.FailedPrecondition,
	}, {
		name:     "acquired but unregistered epoch is refused",
		epoch:    testEpoch + 1,
		classes:  []*ateapipb.WorkerSandboxClass{testClass("gvisor", arm64)},
		wantCode: codes.FailedPrecondition,
	}, {
		// Once the worker process has reported a sandbox class, an entry of
		// another class contradicts its recorded state and is refused.
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
		// A worker process that has not reported yet sets the class on its
		// first report.
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
				initialStatus.SandboxClasses = nil
			}
			before := seedWorker(t, st, name, testNode, initialStatus)
			epoch := tt.epoch
			if epoch == 0 {
				epoch = testEpoch
			}

			got, err := s.SetWorkerSandboxRuntimes(authed, &ateapipb.SetWorkerSandboxRuntimesRequest{
				Worker:         &ateapipb.ObjectRef{Name: name},
				Epoch:          epoch,
				SandboxClasses: tt.classes,
			})
			if code := apierror.Code(err); code != tt.wantCode {
				t.Fatalf("code = %v (err %v), want %v", code, err, tt.wantCode)
			}
			after := mustGetWorker(t, s, name)
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

// Attribute order is the producer's business, not the record's: the same
// attributes in another order are the same runtime, so they are stored sorted
// by key and a reordered repeat writes nothing.
func TestSetWorkerSandboxRuntimes_SortsAttributes(t *testing.T) {
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
	report := func(def, restorable *ateapipb.VersionedSandboxCompat) *ateapipb.Worker {
		t.Helper()
		req := runtimesRequest(testEpoch)
		req.SandboxClasses = []*ateapipb.WorkerSandboxClass{testClass("gvisor", def, restorable)}
		got, err := s.SetWorkerSandboxRuntimes(authed, req)
		if err != nil {
			t.Fatalf("SetWorkerSandboxRuntimes() failed: %v", err)
		}
		return got.GetWorker()
	}

	first := report(compatWith("cpu_features", "architecture"), compatWith("sandbox_bundle_hash", "architecture"))
	want := []*ateapipb.WorkerSandboxClass{testClass("gvisor", compatWith("architecture", "cpu_features"), compatWith("architecture", "sandbox_bundle_hash"))}
	if diff := cmp.Diff(want, first.GetStatus().GetSandboxClasses(), protocmp.Transform()); diff != "" {
		t.Errorf("sandbox_classes not stored sorted by key (-want +got):\n%s", diff)
	}

	again := report(compatWith("architecture", "cpu_features"), compatWith("architecture", "sandbox_bundle_hash"))
	if got, want := again.GetMetadata().GetVersion(), first.GetMetadata().GetVersion(); got != want {
		t.Errorf("version = %d after a reordered repeat, want %d unchanged", got, want)
	}
}

func TestSetWorkerSandboxRuntimes_Errors(t *testing.T) {
	st, cleanup := storetest.SetupTestStore(t)
	defer cleanup()
	s := New(st, &fakeControl{}, testAteletSPIFFEID, nil, "", nil)
	seedReportedWorker(t, st, testNode, &ateapipb.WorkerResources{Actors: 1})
	authed := ateletauthtest.ContextWith(ateletauthtest.CertOn(t, testNode))
	without := func(mutate func(*ateapipb.SetWorkerSandboxRuntimesRequest)) *ateapipb.SetWorkerSandboxRuntimesRequest {
		req := runtimesRequest(testEpoch)
		mutate(req)
		return req
	}

	tests := []struct {
		name string
		ctx  context.Context
		req  *ateapipb.SetWorkerSandboxRuntimesRequest
		want codes.Code
	}{
		{"unauthenticated", ateletauthtest.ContextWith(nil), runtimesRequest(testEpoch), codes.Unauthenticated},
		{"no worker ref", authed, without(func(r *ateapipb.SetWorkerSandboxRuntimesRequest) { r.Worker = nil }), codes.InvalidArgument},
		{"no epoch", authed, without(func(r *ateapipb.SetWorkerSandboxRuntimesRequest) { r.Epoch = 0 }), codes.InvalidArgument},
		{"no sandbox classes", authed, without(func(r *ateapipb.SetWorkerSandboxRuntimesRequest) { r.SandboxClasses = nil }), codes.InvalidArgument},
		{"class without a default runtime", authed, without(func(r *ateapipb.SetWorkerSandboxRuntimesRequest) {
			r.SandboxClasses = []*ateapipb.WorkerSandboxClass{{SandboxClass: "gvisor"}}
		}), codes.InvalidArgument},
		{"class that is not a sandbox class", authed, without(func(r *ateapipb.SetWorkerSandboxRuntimesRequest) {
			r.SandboxClasses = []*ateapipb.WorkerSandboxClass{testClass("plain", testCompat("amd64"))}
		}), codes.InvalidArgument},
		{"other node", ateletauthtest.ContextWith(ateletauthtest.CertOn(t, "some-other-node")), runtimesRequest(testEpoch), codes.NotFound},
		{"absent worker", authed, without(func(r *ateapipb.SetWorkerSandboxRuntimesRequest) {
			r.Worker = &ateapipb.ObjectRef{Name: "3b9f1e77-2c4d-4a80-91be-6d5c8f0a7e21"}
		}), codes.NotFound},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := s.SetWorkerSandboxRuntimes(tc.ctx, tc.req)
			if got := apierror.Code(err); got != tc.want {
				t.Errorf("code = %v (err %v), want %v", got, err, tc.want)
			}
		})
	}
}

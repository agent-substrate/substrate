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

	"go.ate.dev/substrate/cmd/ateapi/internal/store"
	"go.ate.dev/substrate/internal/installdefaults"
	"go.ate.dev/substrate/internal/resources"
	"go.ate.dev/substrate/pkg/proto/ateapipb"
)

// The Worker every test in this package calls about, and the node its atelet
// runs on.
const (
	testWorkerName = "8f1c2d34-5e6a-4b7c-9d8e-0f1a2b3c4d5e"
	testNode       = "node-1"
)

// testAteletSPIFFEID is the atelet identity a Server accepts, and the one the
// certificates from ateletauthtest carry.
var testAteletSPIFFEID = installdefaults.SPIFFEID(installdefaults.SystemNamespace, installdefaults.AteletServiceAccount)

// fakeControl stands in for the Control service. What it records is the
// point of most of the tests: whether the request reached the operation at
// all.
type fakeControl struct {
	calls []*ateapipb.SuspendActorRequest
	resp  *ateapipb.SuspendActorResponse
	err   error

	reports    []appliedReport
	reportErr  error
	crashCalls []crashCall
	crashResp  *ateapipb.Actor
}

func (f *fakeControl) SuspendActor(_ context.Context, req *ateapipb.SuspendActorRequest) (*ateapipb.SuspendActorResponse, error) {
	f.calls = append(f.calls, req)
	return f.resp, f.err
}

func (f *fakeControl) ApplyActorSnapshotState(_ context.Context, actorRef resources.ActorRef, actorUID, node string, state *ateapipb.ActorSnapshotState) error {
	f.reports = append(f.reports, appliedReport{actorRef: actorRef, actorUID: actorUID, node: node, state: state})
	return f.reportErr
}

// appliedReport is one ApplyActorSnapshotState call.
type appliedReport struct {
	actorRef resources.ActorRef
	actorUID string
	node     string
	state    *ateapipb.ActorSnapshotState
}

// crashCall is one CrashActorReportedByWorker call.
type crashCall struct {
	ActorRef                      resources.ActorRef
	ActorUID, WorkerName, Message string
}

func (f *fakeControl) CrashActorReportedByWorker(_ context.Context, actorRef resources.ActorRef, actorUID, workerName, message string) (*ateapipb.Actor, error) {
	f.crashCalls = append(f.crashCalls, crashCall{actorRef, actorUID, workerName, message})
	return f.crashResp, f.err
}

// seedReportedWorker registers a Worker on nodeName that has already reported
// capacity and its default runtime: identity from the pod, the way the syncer
// writes it, plus the result of an earlier registration. A Worker that has
// never registered carries none, so what a fresh registration replaces is what
// this seeds.
func seedReportedWorker(t *testing.T, st store.Interface, nodeName string, capacity *ateapipb.WorkerResources) *ateapipb.Worker {
	t.Helper()
	return seedWorker(t, st, testWorkerName, nodeName, &ateapipb.WorkerStatus{
		State: ateapipb.WorkerState_WORKER_STATE_ACTIVE, Capacity: capacity, DefaultRuntime: testDefaultRuntime,
	})
}

// seedWorker registers a Worker named name on nodeName, with status already
// recorded. name doubles as the pod UID, so it must be UUID-shaped.
func seedWorker(t *testing.T, st store.Interface, name, nodeName string, status *ateapipb.WorkerStatus) *ateapipb.Worker {
	t.Helper()
	created, err := st.CreateWorker(context.Background(), &ateapipb.Worker{
		Metadata:        &ateapipb.ResourceMetadata{Name: name},
		WorkerNamespace: "ate-system",
		WorkerPool:      "pool-1",
		WorkerPod:       "worker-pod-1",
		WorkerPodUid:    name,
		NodeName:        nodeName,
		Ips:             []string{"10.1.2.3"},
		Status:          status,
	})
	if err != nil {
		t.Fatalf("seeding worker %s: %v", name, err)
	}
	return created
}

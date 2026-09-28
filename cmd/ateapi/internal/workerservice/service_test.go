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

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/internal/installdefaults"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
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

// fakeSuspender stands in for the Control service. What it records is the
// point of most of the suspend tests: whether the request reached the suspend
// at all.
type fakeSuspender struct {
	calls []*ateapipb.SuspendActorRequest
	resp  *ateapipb.SuspendActorResponse
	err   error
}

func (f *fakeSuspender) SuspendActor(_ context.Context, req *ateapipb.SuspendActorRequest) (*ateapipb.SuspendActorResponse, error) {
	f.calls = append(f.calls, req)
	return f.resp, f.err
}

// seedReportedWorker registers a Worker on nodeName that has already reported
// capacity: identity from the pod, the way the syncer writes it, plus the
// result of an earlier report. A Worker that has never reported carries none,
// so what a fresh report replaces is what this seeds.
func seedReportedWorker(t *testing.T, st store.Interface, nodeName string, capacity *ateapipb.WorkerResources) *ateapipb.Worker {
	t.Helper()
	created, err := st.CreateWorker(context.Background(), &ateapipb.Worker{
		Metadata:        &ateapipb.ResourceMetadata{Name: testWorkerName},
		WorkerNamespace: "ate-system",
		WorkerPool:      "pool-1",
		WorkerPod:       "worker-pod-1",
		WorkerPodUid:    testWorkerName,
		NodeName:        nodeName,
		Ip:              "10.1.2.3",
		SandboxClass:    "gvisor",
		Status:          &ateapipb.WorkerStatus{State: ateapipb.WorkerState_WORKER_STATE_ACTIVE, Capacity: capacity},
	})
	if err != nil {
		t.Fatalf("seeding worker: %v", err)
	}
	return created
}

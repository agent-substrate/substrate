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

package functionaltest

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/testing/protocmp"
	"k8s.io/apimachinery/pkg/util/wait"
)

// Workers are named after the pod UID they were derived from.
const testWorkerName = "5f2c1a90-7b34-4e6d-8a11-0c3e9d5b7f42"

// newTestWorker returns a Worker in the shape CreateWorker accepts: named after
// its pod UID, with the pod coordinates filled in and no status, which is
// output-only.
//
// The Worker stands on its own, with no pod behind it. That is enough for the
// CRUD API — only the atelet dialer resolves a Worker to a pod, and nothing in
// ate-api reconciles the two now that the syncer runs in ate-controller. Tests
// that need both use createWorkerPod.
func newTestWorker(ns string) *ateapipb.Worker {
	return &ateapipb.Worker{
		Metadata:        &ateapipb.ResourceMetadata{Name: testWorkerName},
		WorkerNamespace: ns,
		WorkerPool:      "pool1",
		WorkerPod:       "worker-api-1",
		WorkerPodUid:    testWorkerName,
		NodeName:        "node1",
		Ip:              "10.1.2.3",
		SandboxClass:    "gvisor",
		Capacity:        &ateapipb.WorkerCapacity{CpuMilli: 2000, MemoryBytes: 4 << 30},
	}
}

// registerWorker registers newTestWorker over the API and returns it as stored.
func registerWorker(t *testing.T, tc *testContext, ns string) *ateapipb.Worker {
	t.Helper()
	created, err := tc.client.CreateWorker(context.Background(), &ateapipb.CreateWorkerRequest{Worker: newTestWorker(ns)})
	if err != nil {
		t.Fatalf("CreateWorker failed: %v", err)
	}
	return created
}

func workerRef(name string) *ateapipb.ObjectRef {
	return &ateapipb.ObjectRef{Name: name}
}

// waitForWorkerState waits for a state committed by an RPC to reach the worker
// cache, which follows the store through a PostgreSQL watch and so lands
// shortly after the call returns rather than with it.
func waitForWorkerState(t *testing.T, tc *testContext, name string, want ateapipb.WorkerState) {
	t.Helper()
	err := wait.PollUntilContextTimeout(context.Background(), 10*time.Millisecond, 5*time.Second, true, func(context.Context) (bool, error) {
		worker, err := tc.workerCache.Worker(name)
		if err != nil {
			return false, nil
		}
		return worker.GetStatus().GetState() == want, nil
	})
	if err != nil {
		t.Fatalf("worker %s did not reach state %v in the worker cache: %v", name, want, err)
	}
}

func containsWorker(workers []*ateapipb.Worker, name string) bool {
	for _, w := range workers {
		if w.GetMetadata().GetName() == name {
			return true
		}
	}
	return false
}

// TestCreateAndGetWorker registers a Worker and reads it back, checking that
// the server assigns the metadata and the ACTIVE status a new Worker starts in.
func TestCreateAndGetWorker(t *testing.T) {
	ns := namespaceForTest("ns-worker-create-get")
	tc := setupTest(t, ns)
	defer tc.cleanup()

	created := registerWorker(t, tc, ns)

	want := newTestWorker(ns)
	want.Metadata.Version = 1
	want.Status = &ateapipb.WorkerStatus{State: ateapipb.WorkerState_WORKER_STATE_ACTIVE}
	if diff := cmp.Diff(want, created, protocmp.Transform(), ignoreServerMetadata); diff != "" {
		t.Errorf("CreateWorker response mismatch (-want +got):\n%s", diff)
	}
	if created.GetMetadata().GetUid() == "" {
		t.Errorf("CreateWorker returned no uid, which every later guard needs")
	}

	got, err := tc.client.GetWorker(context.Background(), &ateapipb.GetWorkerRequest{Worker: workerRef(testWorkerName)})
	if err != nil {
		t.Fatalf("GetWorker failed: %v", err)
	}
	if diff := cmp.Diff(created, got, protocmp.Transform()); diff != "" {
		t.Errorf("GetWorker returned something other than what CreateWorker stored (-created +got):\n%s", diff)
	}
}

// TestListWorkers tests that registered workers are listed.
// Workflow:
//  1. Creates a mock WorkerPool in Kubernetes.
//  2. Creates a mock worker Pod in Kubernetes belonging to that pool, and
//     registers the Worker the syncer would derive from it.
//  3. Calls ListWorkers RPC.
//  4. Verifies that the worker appears in the response.
func TestListWorkers(t *testing.T) {
	ns := namespaceForTest("ns-list-workers")
	tc := setupTest(t, ns)
	defer tc.cleanup()

	createWorkerPool(t, tc, ns, "pool1", map[string]string{"foo": "bar"})
	podUID := createWorkerPod(t, tc, ns, "worker-1", "node1", "pool1")

	listResp, err := tc.client.ListWorkers(context.Background(), &ateapipb.ListWorkersRequest{})
	if err != nil {
		t.Fatalf("ListWorkers failed: %v", err)
	}

	var filteredWorkers []*ateapipb.Worker
	for _, w := range listResp.GetWorkers() {
		if w.GetWorkerNamespace() == ns {
			filteredWorkers = append(filteredWorkers, w)
		}
	}

	want := []*ateapipb.Worker{
		{
			Metadata: &ateapipb.ResourceMetadata{
				Name:    podUID,
				Version: 1,
			},
			WorkerNamespace: ns,
			WorkerPool:      "pool1",
			WorkerPod:       "worker-1",
			WorkerPodUid:    podUID,
			NodeName:        "node1",
			Ip:              "127.0.0.1",
			SandboxClass:    "gvisor",
			Labels:          map[string]string{"foo": "bar"},
			Status: &ateapipb.WorkerStatus{
				State: ateapipb.WorkerState_WORKER_STATE_ACTIVE,
			},
		},
	}

	if diff := cmp.Diff(want, filteredWorkers, protocmp.Transform(), ignoreServerMetadata); diff != "" {
		t.Errorf("ListWorkers response mismatch (-want +got):\n%s", diff)
	}
}

// TestUpdateWorker changes the two mutable fields. An update replaces the whole
// Worker, so the request is the observed one with those fields changed: its
// metadata carries the uid and version guards, and everything else has to be
// sent back as-is to avoid being cleared.
func TestUpdateWorker(t *testing.T) {
	ns := namespaceForTest("ns-worker-update")
	tc := setupTest(t, ns)
	defer tc.cleanup()

	worker := registerWorker(t, tc, ns)
	worker.SandboxClass = "runc"
	worker.Labels = map[string]string{"tier": "batch"}

	updated, err := tc.client.UpdateWorker(context.Background(), &ateapipb.UpdateWorkerRequest{Worker: worker})
	if err != nil {
		t.Fatalf("UpdateWorker failed: %v", err)
	}

	want := newTestWorker(ns)
	want.Metadata.Version = 2
	want.SandboxClass = "runc"
	want.Labels = map[string]string{"tier": "batch"}
	want.Status = &ateapipb.WorkerStatus{State: ateapipb.WorkerState_WORKER_STATE_ACTIVE}
	if diff := cmp.Diff(want, updated, protocmp.Transform(), ignoreServerMetadata); diff != "" {
		t.Errorf("UpdateWorker response mismatch (-want +got):\n%s", diff)
	}
}

// TestDrainWorker marks a Worker as terminating, and checks the state reaches
// the worker cache the scheduler places actors from. Without that, a drained
// Worker would keep taking work.
func TestDrainWorker(t *testing.T) {
	ns := namespaceForTest("ns-worker-drain")
	tc := setupTest(t, ns)
	defer tc.cleanup()

	registerWorker(t, tc, ns)
	waitForWorkerAvailable(t, tc, testWorkerName)

	drained, err := tc.client.DrainWorker(context.Background(), &ateapipb.DrainWorkerRequest{Worker: workerRef(testWorkerName)})
	if err != nil {
		t.Fatalf("DrainWorker failed: %v", err)
	}

	want := newTestWorker(ns)
	want.Metadata.Version = 2
	want.Status = &ateapipb.WorkerStatus{State: ateapipb.WorkerState_WORKER_STATE_DRAINING}
	if diff := cmp.Diff(want, drained, protocmp.Transform(), ignoreServerMetadata); diff != "" {
		t.Errorf("DrainWorker response mismatch (-want +got):\n%s", diff)
	}

	waitForWorkerState(t, tc, testWorkerName, ateapipb.WorkerState_WORKER_STATE_DRAINING)
}

// TestDeleteWorker deregisters a Worker and checks it is gone. Deregistering is
// not silently idempotent, so a second attempt reports NOT_FOUND.
func TestDeleteWorker(t *testing.T) {
	ns := namespaceForTest("ns-worker-delete")
	tc := setupTest(t, ns)
	defer tc.cleanup()
	ctx := context.Background()

	created := registerWorker(t, tc, ns)

	deleted, err := tc.client.DeleteWorker(ctx, &ateapipb.DeleteWorkerRequest{Worker: workerRef(testWorkerName)})
	if err != nil {
		t.Fatalf("DeleteWorker failed: %v", err)
	}
	if diff := cmp.Diff(created, deleted, protocmp.Transform(), ignoreTimestamps); diff != "" {
		t.Errorf("DeleteWorker returned something other than the record it removed (-created +deleted):\n%s", diff)
	}

	listed, err := tc.client.ListWorkers(ctx, &ateapipb.ListWorkersRequest{})
	if err != nil {
		t.Fatalf("ListWorkers failed: %v", err)
	}
	if containsWorker(listed.GetWorkers(), testWorkerName) {
		t.Errorf("ListWorkers still returns worker %s after DeleteWorker", testWorkerName)
	}

	_, err = tc.client.DeleteWorker(ctx, &ateapipb.DeleteWorkerRequest{Worker: workerRef(testWorkerName)})
	assertGrpcError(t, err, codes.NotFound, fmt.Sprintf("Worker %s not found", testWorkerName))
}

func TestValidation_Worker(t *testing.T) {
	ns := namespaceForTest("ns-validation-worker")
	tc := setupTest(t, ns)
	defer tc.cleanup()

	t.Run("ListWorkers", func(t *testing.T) {
		_, err := tc.client.ListWorkers(context.Background(), &ateapipb.ListWorkersRequest{PageSize: -1})
		assertGrpcErrorRegex(t, err, codes.InvalidArgument, "page_size: Invalid value")
	})

	t.Run("ListWorkers invalid token", func(t *testing.T) {
		_, err := tc.client.ListWorkers(context.Background(), &ateapipb.ListWorkersRequest{PageToken: "%%%"})
		assertGrpcError(t, err, codes.InvalidArgument, "invalid page_token")
	})
}

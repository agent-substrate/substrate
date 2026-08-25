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
	"testing"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/testing/protocmp"
)

// TestListWorkers tests that workers mirrored to the store are listed.
// Workflow:
// 1. Creates a mock WorkerPool in Kubernetes.
// 2. Creates a mock worker Pod in Kubernetes belonging to that pool.
// 3. Waits for the background WorkerPoolSyncer to mirror it to the store.
// 4. Calls ListWorkers RPC.
// 5. Verifies that the worker appears in the response.
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

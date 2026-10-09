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
	"errors"
	"slices"
	"testing"

	"github.com/google/go-cmp/cmp"
	"google.golang.org/grpc/codes"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/ateletauth/ateletauthtest"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store/storetest"
	"github.com/agent-substrate/substrate/internal/apierror"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

// Workers besides testWorkerName, and the actors the tests bind to them.
const (
	sameNodeWorker  = "3b9f1e77-2c4d-4a80-91be-6d5c8f0a7e21"
	otherNodeWorker = "5c7e2a91-3d4b-4f60-8a1c-2e9b7d0f4a63"
	otherNode       = "node-2"

	actorA1 = "0a000000-0000-4000-8000-000000000001"
	actorA2 = "0a000000-0000-4000-8000-000000000002"
	actorA3 = "0a000000-0000-4000-8000-000000000003"
	actorC1 = "0c000000-0000-4000-8000-000000000001"
	actorB1 = "0b000000-0000-4000-8000-000000000001"
)

// fakeWorkerLister stands in for the worker cache. calls records whether a
// request got as far as reading it.
type fakeWorkerLister struct {
	workers []*ateapipb.Worker
	err     error
	calls   int
}

func (f *fakeWorkerLister) Workers() ([]*ateapipb.Worker, error) {
	f.calls++
	return f.workers, f.err
}

// seedWorkerOn stores a Worker named name on nodeName, as the syncer
// registers one, and returns it as stored.
func seedWorkerOn(t *testing.T, st store.Interface, name, nodeName string) *ateapipb.Worker {
	t.Helper()
	created, err := st.CreateWorker(context.Background(), &ateapipb.Worker{
		Metadata:        &ateapipb.ResourceMetadata{Name: name},
		WorkerNamespace: "ate-system",
		WorkerPool:      "pool-1",
		WorkerPod:       "worker-pod-" + name[:8],
		WorkerPodUid:    name,
		NodeName:        nodeName,
		Ips:             []string{"10.1.2.3"},
		SandboxClass:    "gvisor",
		Status:          &ateapipb.WorkerStatus{State: ateapipb.WorkerState_WORKER_STATE_ACTIVE},
	})
	if err != nil {
		t.Fatalf("seeding worker %s: %v", name, err)
	}
	return created
}

// bindActors assigns actors to a Worker the way placement does: in-process,
// through the store.
func bindActors(t *testing.T, st store.Interface, workerName string, actorUIDs ...string) {
	t.Helper()
	for _, uid := range actorUIDs {
		if err := st.BindActorToWorker(context.Background(), workerName, &ateapipb.ActorAssignment{
			Actor:    &ateapipb.ObjectRef{Atespace: "team-a", Name: "actor-" + uid[:8] + uid[len(uid)-1:]},
			ActorUid: uid,
		}, nil); err != nil {
			t.Fatalf("binding actor %s to worker %s: %v", uid, workerName, err)
		}
	}
}

func listNodeActorUIDs(t *testing.T, s *Server, node string) []string {
	t.Helper()
	resp, err := s.ListNodeActorUIDs(ateletauthtest.ContextWith(ateletauthtest.CertOn(t, node)), &ateapipb.ListNodeActorUIDsRequest{})
	if err != nil {
		t.Fatalf("ListNodeActorUIDs() failed: %v", err)
	}
	return slices.Sorted(slices.Values(resp.GetActorUids()))
}

// The point of the RPC: one call answers for every Worker on the caller's
// node, and for nothing on any other node.
func TestListNodeActorUIDs(t *testing.T) {
	st, cleanup := storetest.SetupTestStore(t)
	defer cleanup()
	workers := &fakeWorkerLister{workers: []*ateapipb.Worker{
		seedWorkerOn(t, st, testWorkerName, testNode),
		seedWorkerOn(t, st, sameNodeWorker, testNode),
		seedWorkerOn(t, st, otherNodeWorker, otherNode),
	}}
	bindActors(t, st, testWorkerName, actorA1, actorA2)
	bindActors(t, st, sameNodeWorker, actorC1)
	bindActors(t, st, otherNodeWorker, actorB1)
	s := New(st, &fakeSuspender{}, workers, testAteletSPIFFEID, nil)

	if diff := cmp.Diff([]string{actorA1, actorA2, actorC1}, listNodeActorUIDs(t, s, testNode)); diff != "" {
		t.Errorf("actors on %s mismatch (-want +got):\n%s", testNode, diff)
	}
	if diff := cmp.Diff([]string{actorB1}, listNodeActorUIDs(t, s, otherNode)); diff != "" {
		t.Errorf("actors on %s mismatch (-want +got):\n%s", otherNode, diff)
	}
}

// A node with nothing placed on it is an empty answer, not an error, and so is
// a Worker the cache still holds after the store deleted it: it has no
// assignments left.
func TestListNodeActorUIDs_NothingPlaced(t *testing.T) {
	st, cleanup := storetest.SetupTestStore(t)
	defer cleanup()
	idle := seedWorkerOn(t, st, testWorkerName, testNode)
	deleted := &ateapipb.Worker{
		Metadata: &ateapipb.ResourceMetadata{Name: sameNodeWorker},
		NodeName: testNode,
	}
	s := New(st, &fakeSuspender{}, &fakeWorkerLister{workers: []*ateapipb.Worker{idle, deleted}}, testAteletSPIFFEID, nil)

	if got := listNodeActorUIDs(t, s, testNode); len(got) != 0 {
		t.Errorf("ListNodeActorUIDs() = %v, want none", got)
	}
}

// A Worker's assignments are read a page at a time, and every page counts:
// a UID dropped here is a live actor's directory the sweep deletes.
func TestListNodeActorUIDs_ReadsEveryPage(t *testing.T) {
	old := nodeActorsPageSize
	nodeActorsPageSize = 1
	t.Cleanup(func() { nodeActorsPageSize = old })

	st, cleanup := storetest.SetupTestStore(t)
	defer cleanup()
	worker := seedWorkerOn(t, st, testWorkerName, testNode)
	bindActors(t, st, testWorkerName, actorA1, actorA2, actorA3)
	s := New(st, &fakeSuspender{}, &fakeWorkerLister{workers: []*ateapipb.Worker{worker}}, testAteletSPIFFEID, nil)

	if diff := cmp.Diff([]string{actorA1, actorA2, actorA3}, listNodeActorUIDs(t, s, testNode)); diff != "" {
		t.Errorf("actors mismatch (-want +got):\n%s", diff)
	}
}

func TestListNodeActorUIDs_Errors(t *testing.T) {
	tests := []struct {
		name string
		ctx  func(t *testing.T) context.Context
		// cacheErr is what the worker cache reports.
		cacheErr error
		want     codes.Code
		// readsCache is whether the request should get as far as the cache.
		readsCache bool
	}{{
		name: "unauthenticated",
		ctx:  func(*testing.T) context.Context { return ateletauthtest.ContextWith(nil) },
		want: codes.Unauthenticated,
	}, {
		name: "caller is not atelet",
		ctx: func(t *testing.T) context.Context {
			return ateletauthtest.ContextWith(ateletauthtest.Cert(t, "ns/ate-system/sa/actor-workload", ateletauthtest.PodIdentityOn(testNode)))
		},
		want: codes.PermissionDenied,
	}, {
		// The cache is resyncing. Answering from what it held would drop
		// every Worker it has yet to relist, so the caller is told to retry.
		name: "worker cache not ready",
		ctx: func(t *testing.T) context.Context {
			return ateletauthtest.ContextWith(ateletauthtest.CertOn(t, testNode))
		},
		cacheErr:   errors.New("worker cache not ready"),
		want:       codes.Unavailable,
		readsCache: true,
	}}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			st, cleanup := storetest.SetupTestStore(t)
			defer cleanup()
			worker := seedWorkerOn(t, st, testWorkerName, testNode)
			bindActors(t, st, testWorkerName, actorA1)
			workers := &fakeWorkerLister{workers: []*ateapipb.Worker{worker}, err: tc.cacheErr}
			s := New(st, &fakeSuspender{}, workers, testAteletSPIFFEID, nil)

			resp, err := s.ListNodeActorUIDs(tc.ctx(t), &ateapipb.ListNodeActorUIDsRequest{})
			if got := apierror.Code(err); got != tc.want {
				t.Fatalf("code = %v (err %v), want %v", got, err, tc.want)
			}
			if resp != nil {
				t.Errorf("a refused request still answered: %v", resp)
			}
			if got := workers.calls > 0; got != tc.readsCache {
				t.Errorf("read the worker cache = %v, want %v", got, tc.readsCache)
			}
		})
	}
}

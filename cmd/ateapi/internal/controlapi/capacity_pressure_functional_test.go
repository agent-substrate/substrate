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

package controlapi

import (
	"context"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

// TestResumeActor_NoFreeWorkers_EmitsCapacityPressurePerEligiblePool verifies
// the request-edge autoscaling signal end to end over the real gRPC stream: a
// resume that finds no free worker publishes one CapacityPressureEvent for
// every pool eligible for the actor (template workerSelector match), and none
// for pools outside the eligible set.
func TestResumeActor_NoFreeWorkers_EmitsCapacityPressurePerEligiblePool(t *testing.T) {
	ns := namespaceForTest("ns-capacity-pressure")
	tc := setupTest(t, ns)
	defer tc.cleanup()

	// Two pools match the template's selector; a third does not.
	createWorkerPool(t, tc, ns, "pool-a", map[string]string{"group": ns})
	createWorkerPool(t, tc, ns, "pool-b", map[string]string{"group": ns})
	createWorkerPool(t, tc, ns, "pool-other", map[string]string{"group": ns + "-other"})
	createTemplateWithSelector(t, tc, ns, "tmpl1", &metav1.LabelSelector{
		MatchLabels: map[string]string{"group": ns},
	})

	if _, err := tc.client.CreateActor(context.Background(), &ateapipb.CreateActorRequest{
		ActorRef:               &ateapipb.ActorRef{Atespace: testAtespace, Name: "id1"},
		ActorTemplateNamespace: ns,
		ActorTemplateName:      "tmpl1",
	}); err != nil {
		t.Fatalf("CreateActor failed: %v", err)
	}

	streamCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	stream, err := tc.client.WatchCapacityPressure(streamCtx, &ateapipb.WatchCapacityPressureRequest{})
	if err != nil {
		t.Fatalf("WatchCapacityPressure failed: %v", err)
	}

	// Opening the stream returns before the server handler necessarily runs;
	// wait until it has registered its hub subscription so the events below
	// cannot be published into a subscriber-less hub and dropped.
	if err := wait.PollUntilContextTimeout(streamCtx, 10*time.Millisecond, 5*time.Second, true, func(context.Context) (bool, error) {
		tc.service.pressure.mu.Lock()
		defer tc.service.pressure.mu.Unlock()
		return len(tc.service.pressure.subs) > 0, nil
	}); err != nil {
		t.Fatalf("capacity-pressure stream never subscribed to the hub: %v", err)
	}

	// No worker pods exist, so both eligible pools are out of free workers.
	_, err = tc.client.ResumeActor(context.Background(), &ateapipb.ResumeActorRequest{
		ActorRef: &ateapipb.ActorRef{Atespace: testAtespace, Name: "id1"},
	})
	assertGrpcError(t, err, codes.FailedPrecondition, "no free workers available")

	// Exactly the two eligible pools are signalled (order is unspecified).
	got := map[string]bool{}
	for i := 0; i < 2; i++ {
		ev, err := stream.Recv()
		if err != nil {
			t.Fatalf("Recv event %d: %v", i+1, err)
		}
		if ev.GetWorkerNamespace() != ns {
			t.Errorf("event %d namespace = %q, want %q", i+1, ev.GetWorkerNamespace(), ns)
		}
		got[ev.GetWorkerPool()] = true
	}
	if !got["pool-a"] || !got["pool-b"] {
		t.Errorf("capacity-pressure events for pools %v, want pool-a and pool-b", got)
	}

	// The non-eligible pool must not be signalled: no further event arrives.
	extra := make(chan *ateapipb.CapacityPressureEvent, 1)
	go func() {
		if ev, err := stream.Recv(); err == nil {
			extra <- ev
		}
	}()
	select {
	case ev := <-extra:
		t.Errorf("unexpected extra capacity-pressure event for %s/%s", ev.GetWorkerNamespace(), ev.GetWorkerPool())
	case <-time.After(300 * time.Millisecond):
	}
}

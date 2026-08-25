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
	"slices"
	"testing"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/internal/resources"
	atev1alpha1 "github.com/agent-substrate/substrate/pkg/api/v1alpha1"
	listersv1alpha1 "github.com/agent-substrate/substrate/pkg/client/listers/api/v1alpha1"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/cache"
)

// testWorkerUID derives a stable pod UID from a pod name, for Workers seeded
// straight into the store.
func testWorkerUID(podName string) string {
	return uuid.NewSHA1(uuid.NameSpaceDNS, []byte(podName)).String()
}

// newTestActorWorkflow builds an ActorWorkflow backed by the given store and a
// lister serving one minimal ActorTemplate. Dependencies the unit tests never
// reach (worker cache, atelet dialer, k8s clients) are nil, so a step that
// unexpectedly executes against them fails the test loudly.
func newTestActorWorkflow(t *testing.T, st store.Interface, tmplNamespace, tmplName string) *ActorWorkflow {
	t.Helper()
	indexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{})
	if err := indexer.Add(&atev1alpha1.ActorTemplate{
		ObjectMeta: metav1.ObjectMeta{Namespace: tmplNamespace, Name: tmplName},
	}); err != nil {
		t.Fatalf("add template to indexer: %v", err)
	}
	return NewActorWorkflow(st, nil, nil, listersv1alpha1.NewActorTemplateLister(indexer), nil, nil, nil, nil, "", nil)
}

// seedWorkflowActor stores an actor with the given state, bound to the given
// template (pass the same tmplNamespace/tmplName as newTestActorWorkflow).
// opts mutate the actor before it is stored.
func seedWorkflowActor(t *testing.T, ctx context.Context, st store.Interface, actorRef resources.ActorRef, tmplNamespace, tmplName string, actorState ateapipb.ActorState, opts ...func(*ateapipb.Actor)) {
	t.Helper()
	actor := &ateapipb.Actor{
		Metadata:               &ateapipb.ResourceMetadata{Name: actorRef.Name, Atespace: actorRef.Atespace},
		Status:                 &ateapipb.ActorStatus{State: actorState},
		ActorTemplateNamespace: tmplNamespace,
		ActorTemplateName:      tmplName,
	}
	for _, opt := range opts {
		opt(actor)
	}
	if _, err := st.CreateActor(ctx, actor); err != nil {
		t.Fatalf("seed actor: %v", err)
	}
}

// allActorStates enumerates every ActorState value, for exhaustive
// CheckPrerequisite table tests. It is derived from the generated enum map so
// states added to the proto are covered automatically.
var allActorStates = func() []ateapipb.ActorState {
	nums := make([]int32, 0, len(ateapipb.ActorState_name))
	for n := range ateapipb.ActorState_name {
		nums = append(nums, n)
	}
	slices.Sort(nums)
	states := make([]ateapipb.ActorState, 0, len(nums))
	for _, n := range nums {
		states = append(states, ateapipb.ActorState(n))
	}
	return states
}()

// assertPrerequisiteResult verifies a CheckPrerequisite outcome for one
// state: nil when allowed, FailedPrecondition otherwise.
func assertPrerequisiteResult(t *testing.T, st ateapipb.ActorState, err error, wantAllowed bool) {
	t.Helper()
	if wantAllowed {
		if err != nil {
			t.Errorf("state %v: CheckPrerequisite = %v, want nil", st, err)
		}
		return
	}
	if err == nil {
		t.Errorf("state %v: CheckPrerequisite = nil, want FailedPrecondition", st)
		return
	}
	if got := status.Code(err); got != codes.FailedPrecondition {
		t.Errorf("state %v: status.Code = %v, want %v", st, got, codes.FailedPrecondition)
	}
}

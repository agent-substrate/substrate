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
	"errors"
	"slices"
	"testing"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store/storetest"
	"github.com/agent-substrate/substrate/internal/objectstore/objectstoretest"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// testWorkerUID derives a stable pod UID from a pod name, for Workers seeded
// straight into the store.
func testWorkerUID(podName string) string {
	return uuid.NewSHA1(uuid.NameSpaceDNS, []byte(podName)).String()
}

// newTestActorWorkflow builds an ActorWorkflow backed by the given store,
// with one minimal ActorTemplate stored in tmplAtespace. Dependencies the
// unit tests never reach (worker cache, atelet dialer, k8s clients) are nil,
// so a step that unexpectedly executes against them fails the test loudly.
// External snapshots go to an in-memory object store, reachable from a test as
// w.objectStore.(*objectstoretest.Fake).
func newTestActorWorkflow(t *testing.T, st store.Interface, tmplAtespace, tmplName string) *ActorWorkflow {
	t.Helper()
	storetest.MustCreateAtespace(t, context.Background(), st, tmplAtespace)
	if _, err := st.CreateActorTemplate(context.Background(), &ateapipb.ActorTemplate{
		Metadata: &ateapipb.ResourceMetadata{Atespace: tmplAtespace, Name: tmplName},
		SnapshotsConfig: &ateapipb.SnapshotsConfig{
			StorageLocation: "gs://snapshots",
		},
		SandboxConfig: &ateapipb.SandboxConfig{
			SandboxClass: ateapipb.SandboxClass_SANDBOX_CLASS_GVISOR,
			ConfigName:   "gvisor",
		},
	}); err != nil && !errors.Is(err, store.ErrAlreadyExists) {
		t.Fatalf("create test ActorTemplate: %v", err)
	}
	return NewActorWorkflow(st, nil, nil, nil, nil, nil, "", nil, objectstoretest.New())
}

// newFinalizeWorkflow builds an ActorWorkflow over persistence with an
// in-memory object store, for the step-level tests that seed the store
// directly rather than going through newTestActorWorkflow.
func newFinalizeWorkflow(persistence store.Interface) (*ActorWorkflow, *objectstoretest.Fake) {
	objects := objectstoretest.New()
	return &ActorWorkflow{store: persistence, objectStore: objects}, objects
}

// mustActorSnapshotURI builds the URI of a snapshot the actor took under
// template's storage location, the way the suspend workflow does.
func mustActorSnapshotURI(t *testing.T, template *ateapipb.ActorTemplate, actor *ateapipb.Actor, name string) resources.SnapshotURI {
	t.Helper()
	atespace, uid := actor.GetMetadata().GetAtespace(), actor.GetMetadata().GetUid()
	uri, err := resources.NewActorSnapshotURI(template.GetSnapshotsConfig().GetStorageLocation(), atespace, uid, name)
	if err != nil {
		t.Fatalf("NewActorSnapshotURI(%s/%s/%s): %v", atespace, uid, name, err)
	}
	return uri
}

const (
	// testStorageLocation is the snapshots_config.storage_location the tests
	// build snapshot URIs under.
	testStorageLocation = "gs://bucket/root"

	// someActorUID stands in for the UID the store assigns an Actor, for tests
	// that need a well-formed snapshot URI but never exercise who owns it. Those
	// seed their Actor in a single call, before a real UID exists.
	someActorUID = "6b1f9d0c-4a2e-4d38-9c77-5e0a1b2c3d4e"
)

// someActorSnapshotURI builds the URI of a snapshot under someActorUID's
// prefix, at location.
func someActorSnapshotURI(t *testing.T, location, atespace, name string) string {
	t.Helper()
	uri, err := resources.NewActorSnapshotURI(location, atespace, someActorUID, name)
	if err != nil {
		t.Fatalf("NewActorSnapshotURI(%s/%s/%s): %v", atespace, someActorUID, name, err)
	}
	return uri.String()
}

// mustTagSnapshotURI builds the URI of the one snapshot a tag owns, the way the
// tag workflow does.
func mustTagSnapshotURI(t *testing.T, template *ateapipb.ActorTemplate, atespace, name string) resources.SnapshotURI {
	t.Helper()
	uri, err := resources.NewTagSnapshotURI(template.GetSnapshotsConfig().GetStorageLocation(), atespace, name)
	if err != nil {
		t.Fatalf("NewTagSnapshotURI(%s/%s): %v", atespace, name, err)
	}
	return uri
}

// mustUpdateActorStatus mutates a stored actor's status. Tests reach for it to
// record snapshot URIs: an actor's prefix is keyed on the UID the store
// assigns, so its URIs cannot be written until the row exists.
func mustUpdateActorStatus(t *testing.T, ctx context.Context, persistence store.Interface, actor *ateapipb.Actor, mutate func(*ateapipb.ActorStatus)) *ateapipb.Actor {
	t.Helper()
	actorRef := resources.ActorRefFromActor(actor)
	updated, err := persistence.UpdateActor(ctx, actorRef, store.PreconditionFrom(actor), func(toUpdate *ateapipb.Actor) error {
		mutate(toUpdate.Status)
		return nil
	})
	if err != nil {
		t.Fatalf("UpdateActor(%s): %v", actorRef, err)
	}
	return updated
}

// seedWorkflowActor stores an actor with the given state, bound to the given
// template (pass the same tmplAtespace/tmplName as newTestActorWorkflow).
// opts mutate the actor before it is stored.
func seedWorkflowActor(t *testing.T, ctx context.Context, st store.Interface, actorRef resources.ActorRef, tmplAtespace, tmplName string, actorState ateapipb.ActorState, opts ...func(*ateapipb.Actor)) {
	t.Helper()

	actor := &ateapipb.Actor{
		Metadata:      &ateapipb.ResourceMetadata{Name: actorRef.Name, Atespace: actorRef.Atespace},
		Status:        &ateapipb.ActorStatus{State: actorState},
		ActorTemplate: &ateapipb.ObjectRef{Atespace: tmplAtespace, Name: tmplName},
	}
	for _, opt := range opts {
		opt(actor)
	}
	storetest.MustCreateActor(t, ctx, st, actor)
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

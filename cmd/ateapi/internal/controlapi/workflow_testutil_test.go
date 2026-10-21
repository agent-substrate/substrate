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

	"github.com/google/uuid"
	"go.ate.dev/substrate/cmd/ateapi/internal/store"
	"go.ate.dev/substrate/cmd/ateapi/internal/store/storetest"
	"go.ate.dev/substrate/internal/apierror"
	"go.ate.dev/substrate/internal/ateattr"
	"go.ate.dev/substrate/internal/objectstore/objectstoretest"
	"go.ate.dev/substrate/internal/objectstoreplugin/objectstoreplugintest"
	"go.ate.dev/substrate/internal/resources"
	"go.ate.dev/substrate/pkg/proto/ateapipb"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
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
// External snapshots go to an in-memory object store through the control
// object-store plugin.
func newTestActorWorkflow(t *testing.T, st store.Interface, tmplAtespace, tmplName string) *ActorWorkflow {
	t.Helper()
	storetest.MustCreateAtespace(t, context.Background(), st, tmplAtespace)
	if _, err := st.CreateActorTemplate(context.Background(), &ateapipb.ActorTemplate{
		Metadata: &ateapipb.ResourceMetadata{Atespace: tmplAtespace, Name: tmplName},
		SnapshotConfig: &ateapipb.SnapshotConfig{
			StorageLocation: "gs://snapshots",
		},
	}); err != nil && !errors.Is(err, store.ErrAlreadyExists) {
		t.Fatalf("create test ActorTemplate: %v", err)
	}
	return NewActorWorkflow(st, nil, nil, nil, nil, "", nil, objectstoreplugintest.ControlClient(objectstoretest.New()))
}

// newFinalizeWorkflow builds an ActorWorkflow over persistence with an
// in-memory object store, for the step-level tests that seed the store
// directly rather than going through newTestActorWorkflow.
func newFinalizeWorkflow(persistence store.Interface) (*ActorWorkflow, *objectstoretest.Fake) {
	objects := objectstoretest.New()
	return &ActorWorkflow{store: persistence, objectStorePlugin: objectstoreplugintest.ControlClient(objects)}, objects
}

// newWireFinalizeWorkflow is newWireCaptureWorkflow with an in-memory object
// store behind the control object-store plugin, for the suspend tests that
// check which external snapshots a finalize collects.
func newWireFinalizeWorkflow(t *testing.T, persistence store.Interface) (*ActorWorkflow, *capturingAtelet, *objectstoretest.Fake) {
	t.Helper()
	w, atelet := newWireCaptureWorkflow(t, persistence)
	objects := objectstoretest.New()
	w.objectStorePlugin = objectstoreplugintest.ControlClient(objects)
	return w, atelet, objects
}

// mustActorSnapshotURI builds the URI of a snapshot the actor took under
// template's storage location, the way the suspend workflow does.
func mustActorSnapshotURI(t *testing.T, template *ateapipb.ActorTemplate, actor *ateapipb.Actor, name string) resources.SnapshotURI {
	t.Helper()
	atespace, uid := actor.GetMetadata().GetAtespace(), actor.GetMetadata().GetUid()
	uri, err := resources.NewActorSnapshotURI(template.GetSnapshotConfig().GetStorageLocation(), atespace, uid, name)
	if err != nil {
		t.Fatalf("NewActorSnapshotURI(%s/%s/%s): %v", atespace, uid, name, err)
	}
	return uri
}

const (
	// testStorageLocation is the snapshot_config.storage_location the tests
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
	uri, err := resources.NewTagSnapshotURI(template.GetSnapshotConfig().GetStorageLocation(), atespace, name)
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

// withPausedShape seeds the shape a pause leaves behind: a node-local
// snapshot at the latest generation with a PENDING durable copy, pinned to
// node. Pair it with ACTOR_STATE_SUSPENDED to seed a paused actor.
func withPausedShape(t *testing.T, node string) func(*ateapipb.Actor) {
	t.Helper()
	snap := newPausedSnapshot(t, 1, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "", "pause-snap", node)
	return func(a *ateapipb.Actor) {
		a.Status.AssignedNode = node
		a.Status.LastAssignedGeneration = 1
		a.Status.Snapshots = []*ateapipb.Snapshot{proto.CloneOf(snap)}
	}
}

// withUploadedPausedShape seeds a paused actor whose snapshot atelet has since
// uploaded: the paused shape, with its durable copy COMPLETED.
func withUploadedPausedShape(t *testing.T, node string) func(*ateapipb.Actor) {
	t.Helper()
	paused := withPausedShape(t, node)
	return func(a *ateapipb.Actor) {
		paused(a)
		a.Status.Snapshots[0].DurableSnapshot.Status = ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED
	}
}

// newInProgressSnapshot builds the snapshot a suspend of either mode marks at
// generation gen: a PENDING durable copy at a URI named uuid, and no locality
// yet.
func newInProgressSnapshot(t *testing.T, gen int32, fidelity ateapipb.SnapshotFidelity, uuid string) *ateapipb.Snapshot {
	t.Helper()
	return newDurableSnapshot(gen, ateapipb.SnapshotOwner_SNAPSHOT_OWNER_ACTOR, fidelity, "", uuid, someActorSnapshotURI(t, testStorageLocation, "team-a", uuid), ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_PENDING)
}

// newPausedSnapshot builds the snapshot a pause leaves on node locality at
// generation gen: a node-local checkpoint whose PENDING durable copy atelet
// uploads in the background. With no locality it is the in-progress mark.
func newPausedSnapshot(t *testing.T, gen int32, fidelity ateapipb.SnapshotFidelity, templateUID, uuid, locality string) *ateapipb.Snapshot {
	t.Helper()
	snap := newInProgressSnapshot(t, gen, fidelity, uuid)
	snap.ActorTemplateUid = templateUID
	snap.Locality = locality
	return snap
}

// withPauseInProgress seeds the shape a suspend of either mode has between
// marking the actor SUSPENDING and finalizing it: a snapshot at the latest
// generation with a PENDING durable copy and no locality yet. Pair it with
// ACTOR_STATE_SUSPENDING.
func withPauseInProgress(t *testing.T) func(*ateapipb.Actor) {
	snap := newInProgressSnapshot(t, 1, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "pause-snap")
	return func(a *ateapipb.Actor) {
		a.Status.LastAssignedGeneration = 1
		a.Status.Snapshots = []*ateapipb.Snapshot{proto.CloneOf(snap)}
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
	if got := apierror.Code(err); got != codes.FailedPrecondition {
		t.Errorf("state %v: status.Code = %v, want %v", st, got, codes.FailedPrecondition)
	}
}

// suspendTestTemplateRef names the template seedSuspendTemplate stores.
func suspendTestTemplateRef() *ateapipb.ObjectRef {
	return &ateapipb.ObjectRef{Atespace: "ns", Name: "tmpl1"}
}

// seedSuspendTemplate stores the ActorTemplate the suspend and pause workflow
// tests run their actors from, with the given snapshot config, and returns it
// as stored.
func seedSuspendTemplate(t *testing.T, ctx context.Context, st store.Interface, snapshotConfig *ateapipb.SnapshotConfig) *ateapipb.ActorTemplate {
	t.Helper()
	ref := suspendTestTemplateRef()
	storetest.MustCreateAtespace(t, ctx, st, ref.GetAtespace())
	if snapshotConfig.GetStorageLocation() == "" {
		if snapshotConfig == nil {
			snapshotConfig = &ateapipb.SnapshotConfig{}
		} else {
			snapshotConfig = proto.CloneOf(snapshotConfig)
		}
		snapshotConfig.StorageLocation = testStorageLocation
	}
	created, err := st.CreateActorTemplate(ctx, &ateapipb.ActorTemplate{
		Metadata:       &ateapipb.ResourceMetadata{Atespace: ref.GetAtespace(), Name: ref.GetName()},
		SnapshotConfig: snapshotConfig,
	})
	if err != nil {
		t.Fatalf("CreateActorTemplate: %v", err)
	}
	stored, err := st.GetActorTemplate(ctx, resources.ActorTemplateRefFromActorTemplate(created))
	if err != nil {
		t.Fatalf("GetActorTemplate: %v", err)
	}
	return stored
}

// seedWireWorker stores the Worker wireTestAssignment names and places
// assignment on it (none when nil), so a finalize step has a real assignment
// to release.
func seedWireWorker(t *testing.T, ctx context.Context, st store.Interface, assignment *ateapipb.ActorAssignment) {
	t.Helper()
	wa := wireTestAssignment()
	if _, err := st.CreateWorker(ctx, &ateapipb.Worker{
		Metadata:        &ateapipb.ResourceMetadata{Name: wa.GetWorker().GetName()},
		WorkerNamespace: wa.GetWorkerNamespace(),
		WorkerPool:      wa.GetWorkerPool(),
		WorkerPod:       wa.GetWorkerPod(),
		WorkerPodUid:    wa.GetWorkerPodUid(),
		NodeName:        wa.GetNodeName(),
		Status:          &ateapipb.WorkerStatus{},
	}); err != nil {
		t.Fatalf("CreateWorker: %v", err)
	}
	seedAssignment(t, st, wa.GetWorker().GetName(), assignment)
}

// assignmentOf is the worker assignment that places actor on a worker.
func assignmentOf(actor *ateapipb.Actor) *ateapipb.ActorAssignment {
	return &ateapipb.ActorAssignment{
		Actor:    &ateapipb.ObjectRef{Atespace: actor.GetMetadata().GetAtespace(), Name: actor.GetMetadata().GetName()},
		ActorUid: actor.GetMetadata().GetUid(),
	}
}

// newMarkOnlyWorkflow builds an ActorWorkflow whose atelet dial always fails,
// so a suspend or pause stops right after its mark step and leaves the actor
// as that step committed it.
func newMarkOnlyWorkflow(st store.Interface) *ActorWorkflow {
	return &ActorWorkflow{store: st, dialer: newDanglingDialer()}
}

// assertStoppedAtDial verifies a workflow built by newMarkOnlyWorkflow failed
// dialing atelet: it got past its mark step, and the failure is retryable.
func assertStoppedAtDial(t *testing.T, err error) {
	t.Helper()
	if !errors.Is(err, ErrNoAteletOnNode) {
		t.Fatalf("err = %v, want ErrNoAteletOnNode from the atelet dial", err)
	}
}

// assertLifecycleFidelity verifies the one lifecycle-op datapoint reader
// collected carries want as its snapshot fidelity label.
func assertLifecycleFidelity(t *testing.T, reader *sdkmetric.ManualReader, want string) {
	t.Helper()
	dp := singleHistogramDP(t, reader, lifecycleOpDurationMetric)
	if got, ok := attrString(dp, ateattr.SnapshotFidelityKey); !ok || got != want {
		t.Errorf("lifecycle-op snapshot fidelity = %q (present %t), want %q", got, ok, want)
	}
}

// updateDuringCheckpoint makes atelet's Checkpoint mutate the stored actor
// before it returns, standing in for a writer that lands between a workflow's
// atelet call and its finalize step.
func updateDuringCheckpoint(t *testing.T, atelet *capturingAtelet, st store.Interface, actorRef resources.ActorRef, mutate func(*ateapipb.ActorStatus)) {
	atelet.onCheckpoint = func(ctx context.Context) error {
		actor, err := st.GetActor(ctx, actorRef)
		if err != nil {
			t.Errorf("GetActor during Checkpoint: %v", err)
			return err
		}
		if _, err := st.UpdateActor(ctx, actorRef, store.PreconditionFrom(actor), func(toUpdate *ateapipb.Actor) error {
			mutate(toUpdate.Status)
			return nil
		}); err != nil {
			t.Errorf("UpdateActor during Checkpoint: %v", err)
			return err
		}
		return nil
	}
}

// mustReadActor reads the stored actor.
func mustReadActor(t *testing.T, ctx context.Context, st store.Interface, actorRef resources.ActorRef) *ateapipb.Actor {
	t.Helper()
	actor, err := st.GetActor(ctx, actorRef)
	if err != nil {
		t.Fatalf("GetActor(%s): %v", actorRef, err)
	}
	return actor
}

// workerDeletedDuringReleaseStore runs the DeleteWorker workflow's release of
// an actor right after a finalize step frees the actor's worker. That workflow
// takes no actor lease, so its crash can land between the step's read of the
// actor and its write.
type workerDeletedDuringReleaseStore struct {
	store.Interface
}

func (s workerDeletedDuringReleaseStore) ReleaseActorFromWorker(ctx context.Context, workerName, actorUID string) (*ateapipb.Worker, error) {
	worker, err := s.Interface.GetWorker(ctx, workerName)
	if err != nil {
		return nil, err
	}
	assignment, err := s.Interface.GetWorkerAssignment(ctx, workerName, actorUID)
	if err != nil {
		return nil, err
	}
	released, err := s.Interface.ReleaseActorFromWorker(ctx, workerName, actorUID)
	if err != nil {
		return nil, err
	}
	if err := NewWorkerWorkflow(s.Interface).releaseBoundActor(ctx, worker, assignment); err != nil {
		return nil, err
	}
	return released, nil
}

// assertConcurrentCrashKept verifies that a finalize step racing
// workerDeletedDuringReleaseStore failed as Aborted and left the actor CRASHED
// with the crash DeleteWorker recorded, rather than committing over it.
func assertConcurrentCrashKept(t *testing.T, ctx context.Context, st store.Interface, actorRef resources.ActorRef, finalizeErr error, wantCrash string) {
	t.Helper()
	if got := apierror.Code(finalizeErr); got != codes.Aborted {
		t.Errorf("finalize error = %v, want code %v", finalizeErr, codes.Aborted)
	}
	got, err := st.GetActor(ctx, actorRef)
	if err != nil {
		t.Fatalf("GetActor: %v", err)
	}
	if got.GetStatus().GetState() != ateapipb.ActorState_ACTOR_STATE_CRASHED {
		t.Errorf("state = %v, want %v", got.GetStatus().GetState(), ateapipb.ActorState_ACTOR_STATE_CRASHED)
	}
	if msg := got.GetStatus().GetCrash().GetMessage(); msg != wantCrash {
		t.Errorf("crash message = %q, want %q", msg, wantCrash)
	}
}

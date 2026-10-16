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
	"net"
	"sync"
	"testing"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/scheduling"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/internal/apierror"
	"github.com/agent-substrate/substrate/internal/ateattr"
	"github.com/agent-substrate/substrate/internal/installdefaults"
	"github.com/agent-substrate/substrate/internal/proto/ateletpb"
	"github.com/agent-substrate/substrate/internal/resources"
	atev1alpha1 "github.com/agent-substrate/substrate/pkg/api/v1alpha1"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/cache"
)

// fakeWorkflowStore implements actorWorkflowStore and scheduling.WorkerSource in-memory.
type fakeWorkflowStore struct {
	mu          sync.Mutex
	actors      map[resources.ActorRef]*ateapipb.Actor
	workers     map[string]*ateapipb.Worker
	assignments map[string]map[string]*ateapipb.ActorAssignment // workerName -> actorUID -> assignment
	templates   map[resources.ActorTemplateRef]*ateapipb.ActorTemplate
	version     int64
}

func newFakeWorkflowStore() *fakeWorkflowStore {
	return &fakeWorkflowStore{
		actors:      make(map[resources.ActorRef]*ateapipb.Actor),
		workers:     make(map[string]*ateapipb.Worker),
		assignments: make(map[string]map[string]*ateapipb.ActorAssignment),
		templates:   make(map[resources.ActorTemplateRef]*ateapipb.ActorTemplate),
	}
}

func (f *fakeWorkflowStore) Workers() ([]*ateapipb.Worker, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	res := make([]*ateapipb.Worker, 0, len(f.workers))
	for _, w := range f.workers {
		res = append(res, proto.Clone(w).(*ateapipb.Worker))
	}
	return res, nil
}

func (f *fakeWorkflowStore) GetActor(_ context.Context, ref resources.ActorRef) (*ateapipb.Actor, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	a, ok := f.actors[ref]
	if !ok {
		return nil, store.ErrNotFound
	}
	return proto.Clone(a).(*ateapipb.Actor), nil
}

func (f *fakeWorkflowStore) UpdateActor(_ context.Context, ref resources.ActorRef, precondition store.Precondition, mutate func(*ateapipb.Actor) error) (*ateapipb.Actor, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	a, ok := f.actors[ref]
	if !ok {
		return nil, store.ErrNotFound
	}
	if precondition.Version != 0 && a.GetMetadata().GetVersion() != precondition.Version {
		return nil, store.ErrVersionConflict
	}
	cloned := proto.Clone(a).(*ateapipb.Actor)
	if err := mutate(cloned); err != nil {
		return nil, err
	}
	f.version++
	if cloned.Metadata == nil {
		cloned.Metadata = &ateapipb.ResourceMetadata{}
	}
	cloned.Metadata.Version = f.version
	f.actors[ref] = cloned
	return proto.Clone(cloned).(*ateapipb.Actor), nil
}

func (f *fakeWorkflowStore) DeleteActor(_ context.Context, ref resources.ActorRef, _ store.DeletePreconditions) (*ateapipb.Actor, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	a, ok := f.actors[ref]
	if !ok {
		return nil, store.ErrNotFound
	}
	delete(f.actors, ref)
	return a, nil
}

func (f *fakeWorkflowStore) GetWorker(_ context.Context, name string) (*ateapipb.Worker, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	w, ok := f.workers[name]
	if !ok {
		return nil, store.ErrNotFound
	}
	return proto.Clone(w).(*ateapipb.Worker), nil
}

func (f *fakeWorkflowStore) UpdateWorker(_ context.Context, name string, precondition store.Precondition, mutate func(*ateapipb.Worker) error) (*ateapipb.Worker, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	w, ok := f.workers[name]
	if !ok {
		return nil, store.ErrNotFound
	}
	if precondition.Version != 0 && w.GetMetadata().GetVersion() != precondition.Version {
		return nil, store.ErrVersionConflict
	}
	cloned := proto.Clone(w).(*ateapipb.Worker)
	if err := mutate(cloned); err != nil {
		return nil, err
	}
	f.version++
	if cloned.Metadata == nil {
		cloned.Metadata = &ateapipb.ResourceMetadata{}
	}
	cloned.Metadata.Version = f.version
	f.workers[name] = cloned
	return proto.Clone(cloned).(*ateapipb.Worker), nil
}

func (f *fakeWorkflowStore) BindActorToWorker(_ context.Context, workerName string, assignment *ateapipb.ActorAssignment, admit func(*ateapipb.Worker) error) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	w, ok := f.workers[workerName]
	if !ok {
		return store.ErrNotFound
	}
	if admit != nil {
		if err := admit(w); err != nil {
			return err
		}
	}
	if f.assignments[workerName] == nil {
		f.assignments[workerName] = make(map[string]*ateapipb.ActorAssignment)
	}
	f.assignments[workerName][assignment.GetActorUid()] = proto.Clone(assignment).(*ateapipb.ActorAssignment)
	return nil
}

func (f *fakeWorkflowStore) ReleaseActorFromWorker(_ context.Context, workerName string, actorUID string) (*ateapipb.Worker, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if byWorker, ok := f.assignments[workerName]; ok {
		delete(byWorker, actorUID)
	}
	w, ok := f.workers[workerName]
	if !ok {
		return nil, store.ErrNotFound
	}
	return proto.Clone(w).(*ateapipb.Worker), nil
}

func (f *fakeWorkflowStore) GetWorkerAssignment(_ context.Context, workerName, actorUID string) (*ateapipb.ActorAssignment, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if byWorker, ok := f.assignments[workerName]; ok {
		if a, ok := byWorker[actorUID]; ok {
			return proto.Clone(a).(*ateapipb.ActorAssignment), nil
		}
	}
	return nil, store.ErrNotFound
}

func (f *fakeWorkflowStore) FindWorkerHostingActor(_ context.Context, actorUID string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for workerName, byWorker := range f.assignments {
		if _, ok := byWorker[actorUID]; ok {
			return workerName, nil
		}
	}
	return "", store.ErrNotFound
}

func (f *fakeWorkflowStore) ListWorkerAssignments(_ context.Context, workerName string, _ store.ListOptions) (store.ListResponse[*ateapipb.ActorAssignment], error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var res []*ateapipb.ActorAssignment
	if byWorker, ok := f.assignments[workerName]; ok {
		for _, a := range byWorker {
			res = append(res, proto.Clone(a).(*ateapipb.ActorAssignment))
		}
	}
	return store.ListResponse[*ateapipb.ActorAssignment]{Items: res}, nil
}

func (f *fakeWorkflowStore) CreateTag(_ context.Context, tag *ateapipb.Tag) (*ateapipb.Tag, error) {
	return tag, nil
}
func (f *fakeWorkflowStore) GetTag(_ context.Context, _ resources.TagRef) (*ateapipb.Tag, error) {
	return nil, store.ErrNotFound
}
func (f *fakeWorkflowStore) UpdateTag(_ context.Context, _ resources.TagRef, _ store.Precondition, _ func(*ateapipb.Tag) error) (*ateapipb.Tag, error) {
	return nil, store.ErrNotFound
}
func (f *fakeWorkflowStore) DeleteTag(_ context.Context, _ resources.TagRef, _ store.DeletePreconditions) (*ateapipb.Tag, error) {
	return nil, store.ErrNotFound
}

func (f *fakeWorkflowStore) GetActorTemplate(_ context.Context, ref resources.ActorTemplateRef) (*ateapipb.ActorTemplate, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	tmpl, ok := f.templates[ref]
	if !ok {
		return nil, store.ErrNotFound
	}
	return proto.Clone(tmpl).(*ateapipb.ActorTemplate), nil
}

func (f *fakeWorkflowStore) DeleteActorTemplate(_ context.Context, ref resources.ActorTemplateRef, _ store.DeletePreconditions) (*ateapipb.ActorTemplate, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	tmpl, ok := f.templates[ref]
	if !ok {
		return nil, store.ErrNotFound
	}
	delete(f.templates, ref)
	return tmpl, nil
}

func (f *fakeWorkflowStore) AcquireLease(ctx context.Context, _ string) (*store.Lease, error) {
	return store.NewLease(ctx, func() {}), nil
}

// configurableAtelet is a mock atelet whose RPC behaviors can be customized per-test.
type configurableAtelet struct {
	ateletpb.UnimplementedAteletServer
	mu            sync.Mutex
	restoreErr    error
	checkpointErr error
	terminateErr  error
	runErr        error
}

func (c *configurableAtelet) Restore(_ context.Context, _ *ateletpb.RestoreRequest) (*ateletpb.RestoreResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.restoreErr != nil {
		return nil, c.restoreErr
	}
	return &ateletpb.RestoreResponse{}, nil
}

func (c *configurableAtelet) Run(_ context.Context, _ *ateletpb.RunRequest) (*ateletpb.RunResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.runErr != nil {
		return nil, c.runErr
	}
	return &ateletpb.RunResponse{}, nil
}

func (c *configurableAtelet) Checkpoint(_ context.Context, _ *ateletpb.CheckpointRequest) (*ateletpb.CheckpointResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.checkpointErr != nil {
		return nil, c.checkpointErr
	}
	return &ateletpb.CheckpointResponse{}, nil
}

func (c *configurableAtelet) Terminate(_ context.Context, _ *ateletpb.TerminateRequest) (*ateletpb.TerminateResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.terminateErr != nil {
		return nil, c.terminateErr
	}
	return &ateletpb.TerminateResponse{}, nil
}

func buildTestWorkflowWithAtelet(t *testing.T, st *fakeWorkflowStore, ateletHandler *configurableAtelet, nodeName string) *ActorWorkflow {
	t.Helper()
	srv := grpc.NewServer()
	ateletpb.RegisterAteletServer(srv, ateletHandler)
	lis := bufconn.Listen(1 << 20)
	go func() {
		_ = srv.Serve(lis)
	}()
	conn, err := grpc.NewClient("passthrough://bufnet",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}))
	if err != nil {
		t.Fatalf("connecting to mock atelet: %v", err)
	}
	t.Cleanup(func() {
		_ = conn.Close()
		srv.Stop()
	})

	indexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{
		byNode: func(obj any) ([]string, error) {
			pod := obj.(*corev1.Pod)
			return []string{pod.Spec.NodeName}, nil
		},
	})
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: installdefaults.SystemNamespace, Name: "atelet-" + nodeName, UID: types.UID("atelet-uid-" + nodeName)},
		Spec:       corev1.PodSpec{NodeName: nodeName},
		Status:     corev1.PodStatus{PodIPs: []corev1.PodIP{{IP: "10.0.0.1"}}},
	}
	_ = indexer.Add(pod)

	dialer := NewAteletDialer(indexer, installdefaults.SystemNamespace, "", "")
	dialer.ateletConns.Add("atelet-uid-"+nodeName, &ateletConn{ip: "10.0.0.1", conn: conn})

	lister := sandboxConfigListerFor(t, []*atev1alpha1.SandboxConfig{{
		ObjectMeta: metav1.ObjectMeta{Name: "gvisor"},
		Spec: atev1alpha1.SandboxConfigSpec{
			SandboxClass:   atev1alpha1.SandboxClassGvisor,
			DefaultVersion: "v1",
			Versions: []atev1alpha1.SandboxVersionConfig{{
				Name:       "v1",
				PauseImage: "pause@sha256:abc",
				Assets:     testAssets(),
			}},
		},
	}})

	return &ActorWorkflow{
		store:               st,
		scheduler:           scheduling.New(st),
		dialer:              dialer,
		sandboxConfigLister: lister,
	}
}

func buildTestWorkflowWithMultiNodeAtelet(t *testing.T, st *fakeWorkflowStore, nodeHandlers map[string]*configurableAtelet) *ActorWorkflow {
	t.Helper()
	indexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{
		byNode: func(obj any) ([]string, error) {
			pod := obj.(*corev1.Pod)
			return []string{pod.Spec.NodeName}, nil
		},
	})
	dialer := NewAteletDialer(indexer, installdefaults.SystemNamespace, "", "")

	for nodeName, handler := range nodeHandlers {
		srv := grpc.NewServer()
		ateletpb.RegisterAteletServer(srv, handler)
		lis := bufconn.Listen(1 << 20)
		go func() {
			_ = srv.Serve(lis)
		}()
		conn, err := grpc.NewClient("passthrough://bufnet",
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
				return lis.DialContext(ctx)
			}))
		if err != nil {
			t.Fatalf("connecting to mock atelet for node %s: %v", nodeName, err)
		}
		t.Cleanup(func() {
			_ = conn.Close()
			srv.Stop()
		})

		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Namespace: installdefaults.SystemNamespace, Name: "atelet-" + nodeName, UID: types.UID("atelet-uid-" + nodeName)},
			Spec:       corev1.PodSpec{NodeName: nodeName},
			Status:     corev1.PodStatus{PodIPs: []corev1.PodIP{{IP: "10.0.0.1"}}},
		}
		_ = indexer.Add(pod)
		dialer.ateletConns.Add("atelet-uid-"+nodeName, &ateletConn{ip: "10.0.0.1", conn: conn})
	}

	lister := sandboxConfigListerFor(t, []*atev1alpha1.SandboxConfig{{
		ObjectMeta: metav1.ObjectMeta{Name: "gvisor"},
		Spec: atev1alpha1.SandboxConfigSpec{
			SandboxClass:   atev1alpha1.SandboxClassGvisor,
			DefaultVersion: "v1",
			Versions: []atev1alpha1.SandboxVersionConfig{{
				Name:       "v1",
				PauseImage: "pause@sha256:abc",
				Assets:     testAssets(),
			}},
		},
	}})

	return &ActorWorkflow{
		store:               st,
		scheduler:           scheduling.New(st),
		dialer:              dialer,
		sandboxConfigLister: lister,
	}
}

func TestHandleAteletError_MarksWorkerUnavailable(t *testing.T) {
	ctx := context.Background()
	st := newFakeWorkflowStore()
	actorRef := resources.ActorRef{Atespace: "team-a", Name: "actor-1"}
	st.actors[actorRef] = &ateapipb.Actor{
		Metadata: &ateapipb.ResourceMetadata{Name: "actor-1", Atespace: "team-a", Uid: "actor-uid-1"},
		Status: &ateapipb.ActorStatus{
			State: ateapipb.ActorState_ACTOR_STATE_RUNNING,
			WorkerAssignment: &ateapipb.WorkerAssignment{
				Worker: &ateapipb.ObjectRef{Name: "w1"},
			},
		},
	}
	st.workers["w1"] = &ateapipb.Worker{
		Metadata: &ateapipb.ResourceMetadata{Name: "w1"},
		Status:   &ateapipb.WorkerStatus{State: ateapipb.WorkerState_WORKER_STATE_ACTIVE},
	}

	reachabilityErr := status.Error(codes.Unavailable, "connection refused")
	err := handleAteletError(ctx, st, actorRef, ateattr.OperationResume, "Restore", false, reachabilityErr)
	if got := apierror.Code(err); got != codes.Unavailable {
		t.Fatalf("apierror.Code = %v, want %v", got, codes.Unavailable)
	}

	w1, err := st.GetWorker(ctx, "w1")
	if err != nil {
		t.Fatalf("GetWorker: %v", err)
	}
	if w1.GetStatus().GetState() != ateapipb.WorkerState_WORKER_STATE_UNAVAILABLE {
		t.Errorf("worker state = %v, want WORKER_STATE_UNAVAILABLE", w1.GetStatus().GetState())
	}
}

func TestHandleAteletError_DrainingWorkerRemainsDraining(t *testing.T) {
	ctx := context.Background()
	st := newFakeWorkflowStore()
	actorRef := resources.ActorRef{Atespace: "team-a", Name: "actor-1"}
	st.actors[actorRef] = &ateapipb.Actor{
		Metadata: &ateapipb.ResourceMetadata{Name: "actor-1", Atespace: "team-a", Uid: "actor-uid-1"},
		Status: &ateapipb.ActorStatus{
			State: ateapipb.ActorState_ACTOR_STATE_RUNNING,
			WorkerAssignment: &ateapipb.WorkerAssignment{
				Worker: &ateapipb.ObjectRef{Name: "w1"},
			},
		},
	}
	st.workers["w1"] = &ateapipb.Worker{
		Metadata: &ateapipb.ResourceMetadata{Name: "w1"},
		Status:   &ateapipb.WorkerStatus{State: ateapipb.WorkerState_WORKER_STATE_DRAINING},
	}

	reachabilityErr := status.Error(codes.Unavailable, "connection refused")
	_ = handleAteletError(ctx, st, actorRef, ateattr.OperationResume, "Restore", false, reachabilityErr)

	w1, _ := st.GetWorker(ctx, "w1")
	if w1.GetStatus().GetState() != ateapipb.WorkerState_WORKER_STATE_DRAINING {
		t.Errorf("draining worker moved out of DRAINING: %v", w1.GetStatus().GetState())
	}
}

func TestResumeActor_RollbackAndRescheduleOnUnavailable(t *testing.T) {
	ctx := context.Background()
	st := newFakeWorkflowStore()
	actorRef := resources.ActorRef{Atespace: "team-a", Name: "actor-1"}
	templateRef := resources.ActorTemplateRef{Atespace: "team-a", Name: "tmpl-1"}

	st.templates[templateRef] = &ateapipb.ActorTemplate{
		Metadata: &ateapipb.ResourceMetadata{Name: "tmpl-1", Atespace: "team-a", Uid: "tmpl-uid-1"},
		SandboxConfig: &ateapipb.SandboxConfig{
			SandboxClass: "gvisor",
			ConfigName:   "gvisor",
		},
	}

	st.actors[actorRef] = &ateapipb.Actor{
		Metadata:      &ateapipb.ResourceMetadata{Name: "actor-1", Atespace: "team-a", Uid: "actor-uid-1", Version: 1},
		ActorTemplate: &ateapipb.ObjectRef{Name: "tmpl-1", Atespace: "team-a"},
		Status: &ateapipb.ActorStatus{
			State: ateapipb.ActorState_ACTOR_STATE_SUSPENDED,
		},
	}

	// Worker 1: initially active, but its atelet fails with codes.Unavailable
	st.workers["w1"] = &ateapipb.Worker{
		Metadata:        &ateapipb.ResourceMetadata{Name: "w1", Version: 1},
		WorkerNamespace: "default",
		WorkerPod:       "pod-1",
		WorkerPodUid:    "pod-1-uid",
		NodeName:        "node-1",
		SandboxClass:    "gvisor",
		Status: &ateapipb.WorkerStatus{
			State:    ateapipb.WorkerState_WORKER_STATE_ACTIVE,
			Capacity: &ateapipb.WorkerResources{Actors: 1},
		},
	}

	mockAtelet := &configurableAtelet{
		restoreErr: status.Error(codes.Unavailable, "connection refused"),
		runErr:     status.Error(codes.Unavailable, "connection refused"),
	}
	wf := buildTestWorkflowWithAtelet(t, st, mockAtelet, "node-1")

	// 1. Resume should fail with Unavailable
	_, resumed, err := wf.ResumeActor(ctx, actorRef)
	if resumed {
		t.Errorf("resumed = true, want false")
	}
	if got := apierror.Code(err); got != codes.Unavailable {
		t.Fatalf("ResumeActor code = %v, want %v (err: %v)", got, codes.Unavailable, err)
	}

	// 2. Assert worker 1 was marked UNAVAILABLE
	w1, _ := st.GetWorker(ctx, "w1")
	if w1.GetStatus().GetState() != ateapipb.WorkerState_WORKER_STATE_UNAVAILABLE {
		t.Errorf("worker 1 state = %v, want WORKER_STATE_UNAVAILABLE", w1.GetStatus().GetState())
	}

	// 3. Assert actor was rolled back to SUSPENDED with assignment cleared
	actor, _ := st.GetActor(ctx, actorRef)
	if actor.GetStatus().GetState() != ateapipb.ActorState_ACTOR_STATE_SUSPENDED {
		t.Errorf("actor state after rollback = %v, want ACTOR_STATE_SUSPENDED", actor.GetStatus().GetState())
	}
	if actor.GetStatus().GetWorkerAssignment() != nil {
		t.Errorf("actor worker assignment = %v, want nil", actor.GetStatus().GetWorkerAssignment())
	}
	if actor.GetStatus().GetAssignedNode() != "" {
		t.Errorf("actor assigned node = %q, want empty", actor.GetStatus().GetAssignedNode())
	}
}

func TestResumeActor_LocalSnapshotNotRolledBack(t *testing.T) {
	ctx := context.Background()
	st := newFakeWorkflowStore()
	actorRef := resources.ActorRef{Atespace: "team-a", Name: "actor-1"}
	templateRef := resources.ActorTemplateRef{Atespace: "team-a", Name: "tmpl-1"}

	st.templates[templateRef] = &ateapipb.ActorTemplate{
		Metadata: &ateapipb.ResourceMetadata{Name: "tmpl-1", Atespace: "team-a", Uid: "tmpl-uid-1"},
		SandboxConfig: &ateapipb.SandboxConfig{
			SandboxClass: "gvisor",
			ConfigName:   "gvisor",
		},
	}

	localSnap := newLocalSnapshot(1, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "tmpl-uid-1", "snap-1", "node-1")
	st.actors[actorRef] = &ateapipb.Actor{
		Metadata:      &ateapipb.ResourceMetadata{Name: "actor-1", Atespace: "team-a", Uid: "actor-uid-1", Version: 1},
		ActorTemplate: &ateapipb.ObjectRef{Name: "tmpl-1", Atespace: "team-a"},
		Status: &ateapipb.ActorStatus{
			State:                  ateapipb.ActorState_ACTOR_STATE_SUSPENDED,
			AssignedNode:           "node-1",
			LastAssignedGeneration: 1,
			Snapshots:              []*ateapipb.Snapshot{localSnap},
		},
	}

	st.workers["w1"] = &ateapipb.Worker{
		Metadata:        &ateapipb.ResourceMetadata{Name: "w1", Version: 1},
		WorkerNamespace: "default",
		WorkerPod:       "pod-1",
		WorkerPodUid:    "pod-1-uid",
		NodeName:        "node-1",
		SandboxClass:    "gvisor",
		Status: &ateapipb.WorkerStatus{
			State:    ateapipb.WorkerState_WORKER_STATE_ACTIVE,
			Capacity: &ateapipb.WorkerResources{Actors: 1},
		},
	}

	mockAtelet := &configurableAtelet{
		restoreErr: status.Error(codes.Unavailable, "connection refused"),
	}
	wf := buildTestWorkflowWithAtelet(t, st, mockAtelet, "node-1")

	_, resumed, err := wf.ResumeActor(ctx, actorRef)
	if resumed {
		t.Errorf("resumed = true, want false")
	}
	if got := apierror.Code(err); got != codes.Unavailable {
		t.Fatalf("ResumeActor code = %v, want %v", got, codes.Unavailable)
	}

	// For an actor with a local snapshot, the assignment should NOT be rolled back because
	// the snapshot is local to that node.
	actor, _ := st.GetActor(ctx, actorRef)
	if actor.GetStatus().GetState() != ateapipb.ActorState_ACTOR_STATE_RESUMING {
		t.Errorf("local snapshot actor state = %v, want ACTOR_STATE_RESUMING", actor.GetStatus().GetState())
	}
	if actor.GetStatus().GetWorkerAssignment() == nil {
		t.Errorf("local snapshot actor worker assignment should remain intact")
	}
}

func TestRevertActor_ResumingAndSuspendingStates(t *testing.T) {
	ctx := context.Background()

	t.Run("reverting ACTOR_STATE_RESUMING succeeds", func(t *testing.T) {
		st := newFakeWorkflowStore()
		actorRef := resources.ActorRef{Atespace: "team-a", Name: "actor-resuming"}
		templateRef := resources.ActorTemplateRef{Atespace: "team-a", Name: "tmpl-1"}

		st.templates[templateRef] = &ateapipb.ActorTemplate{
			Metadata: &ateapipb.ResourceMetadata{Name: "tmpl-1", Atespace: "team-a", Uid: "tmpl-uid-1"},
		}
		st.actors[actorRef] = &ateapipb.Actor{
			Metadata:      &ateapipb.ResourceMetadata{Name: "actor-resuming", Atespace: "team-a", Uid: "actor-uid-resuming", Version: 1},
			ActorTemplate: &ateapipb.ObjectRef{Name: "tmpl-1", Atespace: "team-a"},
			Status: &ateapipb.ActorStatus{
				State:        ateapipb.ActorState_ACTOR_STATE_RESUMING,
				AssignedNode: "node-1",
				WorkerAssignment: &ateapipb.WorkerAssignment{
					Worker:   &ateapipb.ObjectRef{Name: "w1"},
					NodeName: "node-1",
				},
			},
		}
		st.workers["w1"] = &ateapipb.Worker{
			Metadata: &ateapipb.ResourceMetadata{Name: "w1", Version: 1},
			NodeName: "node-1",
			Status:   &ateapipb.WorkerStatus{State: ateapipb.WorkerState_WORKER_STATE_UNAVAILABLE},
		}

		mockAtelet := &configurableAtelet{
			terminateErr: status.Error(codes.Unavailable, "node down"),
		}
		wf := buildTestWorkflowWithAtelet(t, st, mockAtelet, "node-1")

		finalized, err := wf.RevertActor(ctx, actorRef)
		if err != nil {
			t.Fatalf("RevertActor failed: %v", err)
		}
		if finalized.GetStatus().GetState() != ateapipb.ActorState_ACTOR_STATE_SUSPENDED {
			t.Errorf("reverted actor state = %v, want ACTOR_STATE_SUSPENDED", finalized.GetStatus().GetState())
		}
		if finalized.GetStatus().GetWorkerAssignment() != nil {
			t.Errorf("reverted actor worker assignment = %v, want nil", finalized.GetStatus().GetWorkerAssignment())
		}
	})

	t.Run("reverting ACTOR_STATE_SUSPENDING with unavailable worker succeeds", func(t *testing.T) {
		st := newFakeWorkflowStore()
		actorRef := resources.ActorRef{Atespace: "team-a", Name: "actor-suspending"}
		templateRef := resources.ActorTemplateRef{Atespace: "team-a", Name: "tmpl-1"}

		st.templates[templateRef] = &ateapipb.ActorTemplate{
			Metadata: &ateapipb.ResourceMetadata{Name: "tmpl-1", Atespace: "team-a", Uid: "tmpl-uid-1"},
		}
		st.actors[actorRef] = &ateapipb.Actor{
			Metadata:      &ateapipb.ResourceMetadata{Name: "actor-suspending", Atespace: "team-a", Uid: "actor-uid-suspending", Version: 1},
			ActorTemplate: &ateapipb.ObjectRef{Name: "tmpl-1", Atespace: "team-a"},
			Status: &ateapipb.ActorStatus{
				State:        ateapipb.ActorState_ACTOR_STATE_SUSPENDING,
				AssignedNode: "node-1",
				WorkerAssignment: &ateapipb.WorkerAssignment{
					Worker:   &ateapipb.ObjectRef{Name: "w1"},
					NodeName: "node-1",
				},
			},
		}
		st.workers["w1"] = &ateapipb.Worker{
			Metadata: &ateapipb.ResourceMetadata{Name: "w1", Version: 1},
			NodeName: "node-1",
			Status:   &ateapipb.WorkerStatus{State: ateapipb.WorkerState_WORKER_STATE_UNAVAILABLE},
		}

		mockAtelet := &configurableAtelet{
			terminateErr: status.Error(codes.Unavailable, "node down"),
		}
		wf := buildTestWorkflowWithAtelet(t, st, mockAtelet, "node-1")

		finalized, err := wf.RevertActor(ctx, actorRef)
		if err != nil {
			t.Fatalf("RevertActor failed: %v", err)
		}
		if finalized.GetStatus().GetState() != ateapipb.ActorState_ACTOR_STATE_SUSPENDED {
			t.Errorf("reverted actor state = %v, want ACTOR_STATE_SUSPENDED", finalized.GetStatus().GetState())
		}
	})

	t.Run("reverting ACTOR_STATE_SUSPENDING with active worker rejected", func(t *testing.T) {
		st := newFakeWorkflowStore()
		actorRef := resources.ActorRef{Atespace: "team-a", Name: "actor-active-suspending"}
		st.actors[actorRef] = &ateapipb.Actor{
			Metadata:      &ateapipb.ResourceMetadata{Name: "actor-active-suspending", Atespace: "team-a", Uid: "actor-uid-act", Version: 1},
			ActorTemplate: &ateapipb.ObjectRef{Name: "tmpl-1", Atespace: "team-a"},
			Status: &ateapipb.ActorStatus{
				State: ateapipb.ActorState_ACTOR_STATE_SUSPENDING,
				WorkerAssignment: &ateapipb.WorkerAssignment{
					Worker: &ateapipb.ObjectRef{Name: "w-active"},
				},
			},
		}
		st.workers["w-active"] = &ateapipb.Worker{
			Metadata: &ateapipb.ResourceMetadata{Name: "w-active", Version: 1},
			Status:   &ateapipb.WorkerStatus{State: ateapipb.WorkerState_WORKER_STATE_ACTIVE},
		}

		wf := &ActorWorkflow{store: st}
		_, err := wf.RevertActor(ctx, actorRef)
		if got := apierror.Code(err); got != codes.FailedPrecondition {
			t.Fatalf("RevertActor code = %v, want %v", got, codes.FailedPrecondition)
		}
	})
}

func TestDeleteActor_ToleratesUnavailableWorker(t *testing.T) {
	ctx := context.Background()
	st := newFakeWorkflowStore()
	actorRef := resources.ActorRef{Atespace: "team-a", Name: "actor-del"}
	templateRef := resources.ActorTemplateRef{Atespace: "team-a", Name: "tmpl-1"}

	st.templates[templateRef] = &ateapipb.ActorTemplate{
		Metadata: &ateapipb.ResourceMetadata{Name: "tmpl-1", Atespace: "team-a", Uid: "tmpl-uid-1"},
	}
	st.actors[actorRef] = &ateapipb.Actor{
		Metadata:      &ateapipb.ResourceMetadata{Name: "actor-del", Atespace: "team-a", Uid: "actor-uid-del", Version: 1},
		ActorTemplate: &ateapipb.ObjectRef{Name: "tmpl-1", Atespace: "team-a"},
		Status: &ateapipb.ActorStatus{
			State:        ateapipb.ActorState_ACTOR_STATE_RUNNING,
			AssignedNode: "node-1",
			WorkerAssignment: &ateapipb.WorkerAssignment{
				Worker:   &ateapipb.ObjectRef{Name: "w1"},
				NodeName: "node-1",
			},
		},
	}
	st.workers["w1"] = &ateapipb.Worker{
		Metadata: &ateapipb.ResourceMetadata{Name: "w1", Version: 1},
		NodeName: "node-1",
		Status:   &ateapipb.WorkerStatus{State: ateapipb.WorkerState_WORKER_STATE_UNAVAILABLE},
	}

	mockAtelet := &configurableAtelet{
		terminateErr: status.Error(codes.Unavailable, "node unreachable"),
	}
	wf := buildTestWorkflowWithAtelet(t, st, mockAtelet, "node-1")

	_, err := wf.DeleteActor(ctx, actorRef, true, store.DeletePreconditions{})
	if err != nil {
		t.Fatalf("DeleteActor failed on unavailable worker: %v", err)
	}

	// Verify actor is deleted from store
	_, err = st.GetActor(ctx, actorRef)
	if !errors.Is(err, store.ErrNotFound) {
		t.Errorf("actor still in store after DeleteActor: %v", err)
	}
}

func TestSuspendActor_UnavailableWorkerCrashesActor(t *testing.T) {
	ctx := context.Background()
	st := newFakeWorkflowStore()
	actorRef := resources.ActorRef{Atespace: "team-a", Name: "actor-suspend"}
	templateRef := resources.ActorTemplateRef{Atespace: "team-a", Name: "tmpl-1"}

	st.templates[templateRef] = &ateapipb.ActorTemplate{
		Metadata: &ateapipb.ResourceMetadata{Name: "tmpl-1", Atespace: "team-a", Uid: "tmpl-uid-1"},
		SnapshotConfig: &ateapipb.SnapshotConfig{
			StorageLocation: "gs://bucket/test",
		},
	}
	st.actors[actorRef] = &ateapipb.Actor{
		Metadata:      &ateapipb.ResourceMetadata{Name: "actor-suspend", Atespace: "team-a", Uid: "actor-uid-suspend", Version: 1},
		ActorTemplate: &ateapipb.ObjectRef{Name: "tmpl-1", Atespace: "team-a"},
		Status: &ateapipb.ActorStatus{
			State:        ateapipb.ActorState_ACTOR_STATE_RUNNING,
			AssignedNode: "node-1",
			WorkerAssignment: &ateapipb.WorkerAssignment{
				Worker:   &ateapipb.ObjectRef{Name: "w1"},
				NodeName: "node-1",
			},
		},
	}
	st.workers["w1"] = &ateapipb.Worker{
		Metadata: &ateapipb.ResourceMetadata{Name: "w1", Version: 1},
		NodeName: "node-1",
		Status:   &ateapipb.WorkerStatus{State: ateapipb.WorkerState_WORKER_STATE_ACTIVE},
	}

	mockAtelet := &configurableAtelet{
		checkpointErr: status.Error(codes.Unavailable, "node unreachable"),
	}
	wf := buildTestWorkflowWithAtelet(t, st, mockAtelet, "node-1")

	_, err := wf.SuspendActor(ctx, actorRef)
	if err == nil {
		t.Fatalf("SuspendActor succeeded, want crash error")
	}

	// Verify actor was transitioned to CRASHED state
	actor, _ := st.GetActor(ctx, actorRef)
	if actor.GetStatus().GetState() != ateapipb.ActorState_ACTOR_STATE_CRASHED {
		t.Errorf("actor state = %v, want ACTOR_STATE_CRASHED", actor.GetStatus().GetState())
	}
	if actor.GetStatus().GetWorkerAssignment() != nil {
		t.Errorf("crashed actor worker assignment = %v, want nil", actor.GetStatus().GetWorkerAssignment())
	}

	// Verify worker was marked UNAVAILABLE
	w1, _ := st.GetWorker(ctx, "w1")
	if w1.GetStatus().GetState() != ateapipb.WorkerState_WORKER_STATE_UNAVAILABLE {
		t.Errorf("worker state = %v, want WORKER_STATE_UNAVAILABLE", w1.GetStatus().GetState())
	}
}

func TestResumeActor_SubsequentResumePlacesOnHealthyWorker(t *testing.T) {
	ctx := context.Background()
	st := newFakeWorkflowStore()
	actorRef := resources.ActorRef{Atespace: "team-a", Name: "actor-multi"}
	templateRef := resources.ActorTemplateRef{Atespace: "team-a", Name: "tmpl-1"}

	st.templates[templateRef] = &ateapipb.ActorTemplate{
		Metadata: &ateapipb.ResourceMetadata{Name: "tmpl-1", Atespace: "team-a", Uid: "tmpl-uid-1"},
		SandboxConfig: &ateapipb.SandboxConfig{
			SandboxClass: "gvisor",
			ConfigName:   "gvisor",
		},
	}

	st.actors[actorRef] = &ateapipb.Actor{
		Metadata:      &ateapipb.ResourceMetadata{Name: "actor-multi", Atespace: "team-a", Uid: "actor-uid-multi", Version: 1},
		ActorTemplate: &ateapipb.ObjectRef{Name: "tmpl-1", Atespace: "team-a"},
		Status: &ateapipb.ActorStatus{
			State: ateapipb.ActorState_ACTOR_STATE_SUSPENDED,
		},
	}

	// w1 on node-1: active initially, but will fail Restore with Unavailable
	st.workers["w1"] = &ateapipb.Worker{
		Metadata:        &ateapipb.ResourceMetadata{Name: "w1", Version: 1},
		WorkerNamespace: "default",
		WorkerPod:       "pod-1",
		WorkerPodUid:    "pod-1-uid",
		NodeName:        "node-1",
		SandboxClass:    "gvisor",
		Status: &ateapipb.WorkerStatus{
			State:    ateapipb.WorkerState_WORKER_STATE_ACTIVE,
			Capacity: &ateapipb.WorkerResources{Actors: 1},
		},
	}
	// w2 on node-2: healthy worker
	st.workers["w2"] = &ateapipb.Worker{
		Metadata:        &ateapipb.ResourceMetadata{Name: "w2", Version: 1},
		WorkerNamespace: "default",
		WorkerPod:       "pod-2",
		WorkerPodUid:    "pod-2-uid",
		NodeName:        "node-2",
		SandboxClass:    "gvisor",
		Status: &ateapipb.WorkerStatus{
			State:    ateapipb.WorkerState_WORKER_STATE_ACTIVE,
			Capacity: &ateapipb.WorkerResources{Actors: 1},
		},
	}

	mockAtelet1 := &configurableAtelet{
		restoreErr: status.Error(codes.Unavailable, "node-1 unreachable"),
		runErr:     status.Error(codes.Unavailable, "node-1 unreachable"),
	}
	mockAtelet2 := &configurableAtelet{}

	wf := buildTestWorkflowWithMultiNodeAtelet(t, st, map[string]*configurableAtelet{
		"node-1": mockAtelet1,
		"node-2": mockAtelet2,
	})

	// Pre-assign w1 so the first resume attempts w1
	st.actors[actorRef].Status.State = ateapipb.ActorState_ACTOR_STATE_RESUMING
	st.actors[actorRef].Status.WorkerAssignment = &ateapipb.WorkerAssignment{
		Worker:          &ateapipb.ObjectRef{Name: "w1"},
		WorkerNamespace: "default",
		WorkerPool:      "pool",
		WorkerPod:       "pod-1",
		WorkerPodUid:    "pod-1-uid",
		NodeName:        "node-1",
	}
	st.actors[actorRef].Status.AssignedNode = "node-1"
	_ = st.BindActorToWorker(ctx, "w1", &ateapipb.ActorAssignment{
		Actor:    &ateapipb.ObjectRef{Atespace: "team-a", Name: "actor-multi"},
		ActorUid: "actor-uid-multi",
	}, nil)

	// 1. First Resume fails on w1 because atelet is unreachable
	_, _, err := wf.ResumeActor(ctx, actorRef)
	if got := apierror.Code(err); got != codes.Unavailable {
		t.Fatalf("first ResumeActor code = %v, want %v (err: %v)", got, codes.Unavailable, err)
	}

	// 2. w1 should now be marked UNAVAILABLE
	w1, _ := st.GetWorker(ctx, "w1")
	if w1.GetStatus().GetState() != ateapipb.WorkerState_WORKER_STATE_UNAVAILABLE {
		t.Fatalf("w1 state = %v, want WORKER_STATE_UNAVAILABLE", w1.GetStatus().GetState())
	}

	// 3. Actor was rolled back to SUSPENDED with assignment nil
	actor, _ := st.GetActor(ctx, actorRef)
	if actor.GetStatus().GetState() != ateapipb.ActorState_ACTOR_STATE_SUSPENDED {
		t.Fatalf("actor state after rollback = %v, want ACTOR_STATE_SUSPENDED", actor.GetStatus().GetState())
	}
	if actor.GetStatus().GetWorkerAssignment() != nil {
		t.Fatalf("actor worker assignment = %v, want nil", actor.GetStatus().GetWorkerAssignment())
	}

	// 4. Retry ResumeActor: should schedule onto w2 and succeed!
	running, resumed, err := wf.ResumeActor(ctx, actorRef)
	if err != nil {
		t.Fatalf("second ResumeActor failed: %v", err)
	}
	if !resumed {
		t.Errorf("resumed = false, want true")
	}
	if running.GetStatus().GetState() != ateapipb.ActorState_ACTOR_STATE_RUNNING {
		t.Errorf("running actor state = %v, want ACTOR_STATE_RUNNING", running.GetStatus().GetState())
	}
	if running.GetStatus().GetWorkerAssignment().GetWorker().GetName() != "w2" {
		t.Errorf("assigned worker = %q, want w2", running.GetStatus().GetWorkerAssignment().GetWorker().GetName())
	}
}

func TestValidateAssignedWorker_UnavailableWorkerRescheduled(t *testing.T) {
	ctx := context.Background()
	st := newFakeWorkflowStore()
	actorRef := resources.ActorRef{Atespace: "team-a", Name: "actor-val"}
	templateRef := resources.ActorTemplateRef{Atespace: "team-a", Name: "tmpl-1"}

	tmpl := &ateapipb.ActorTemplate{
		Metadata: &ateapipb.ResourceMetadata{Name: "tmpl-1", Atespace: "team-a", Uid: "tmpl-uid-1"},
		SandboxConfig: &ateapipb.SandboxConfig{
			SandboxClass: "gvisor",
			ConfigName:   "gvisor",
		},
	}
	st.templates[templateRef] = tmpl

	st.actors[actorRef] = &ateapipb.Actor{
		Metadata:      &ateapipb.ResourceMetadata{Name: "actor-val", Atespace: "team-a", Uid: "actor-uid-val", Version: 1},
		ActorTemplate: &ateapipb.ObjectRef{Name: "tmpl-1", Atespace: "team-a"},
		Status: &ateapipb.ActorStatus{
			State:        ateapipb.ActorState_ACTOR_STATE_RESUMING,
			AssignedNode: "node-1",
			WorkerAssignment: &ateapipb.WorkerAssignment{
				Worker:   &ateapipb.ObjectRef{Name: "w1"},
				NodeName: "node-1",
			},
		},
	}
	st.workers["w1"] = &ateapipb.Worker{
		Metadata: &ateapipb.ResourceMetadata{Name: "w1", Version: 1},
		NodeName: "node-1",
		Status:   &ateapipb.WorkerStatus{State: ateapipb.WorkerState_WORKER_STATE_UNAVAILABLE},
	}
	_ = st.BindActorToWorker(ctx, "w1", &ateapipb.ActorAssignment{
		Actor:    &ateapipb.ObjectRef{Atespace: "team-a", Name: "actor-val"},
		ActorUid: "actor-uid-val",
	}, nil)

	wf := &ActorWorkflow{store: st}

	// Calling validateAssignedWorker when worker is unavailable (and no local snapshot):
	// Should release assignment, update actor to SUSPENDED, and return nil, nil.
	worker, err := wf.validateAssignedWorker(ctx, actorRef, st.actors[actorRef], tmpl)
	if err != nil {
		t.Fatalf("validateAssignedWorker error = %v, want nil", err)
	}
	if worker != nil {
		t.Fatalf("validateAssignedWorker returned worker %v, want nil", worker)
	}

	// Verify actor in store is now SUSPENDED with assignment nil
	stored, err := st.GetActor(ctx, actorRef)
	if err != nil {
		t.Fatalf("GetActor: %v", err)
	}
	if stored.GetStatus().GetState() != ateapipb.ActorState_ACTOR_STATE_SUSPENDED {
		t.Errorf("stored actor state = %v, want ACTOR_STATE_SUSPENDED", stored.GetStatus().GetState())
	}
	if stored.GetStatus().GetWorkerAssignment() != nil {
		t.Errorf("stored worker assignment = %v, want nil", stored.GetStatus().GetWorkerAssignment())
	}
	if stored.GetStatus().GetAssignedNode() != "" {
		t.Errorf("stored assigned node = %q, want empty", stored.GetStatus().GetAssignedNode())
	}

	// Verify assignment was released from w1
	if _, err := st.GetWorkerAssignment(ctx, "w1", "actor-uid-val"); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("worker assignment on w1 not released: %v", err)
	}
}

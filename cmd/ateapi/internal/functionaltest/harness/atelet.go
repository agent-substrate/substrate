// Copyright 2026 The Agent Substrate Authors
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

package harness

import (
	"cmp"
	"context"
	"fmt"
	"slices"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"go.ate.dev/substrate/internal/proto/ateletpb"
)

// AteletMethod is the gRPC full name of an atelet RPC a fault can be injected
// into.
type AteletMethod string

const (
	AteletRun             = AteletMethod(ateletpb.Atelet_Run_FullMethodName)
	AteletCheckpoint      = AteletMethod(ateletpb.Atelet_Checkpoint_FullMethodName)
	AteletRestore         = AteletMethod(ateletpb.Atelet_Restore_FullMethodName)
	AteletPromoteSnapshot = AteletMethod(ateletpb.Atelet_PromoteSnapshot_FullMethodName)
	AteletDetachActor     = AteletMethod(ateletpb.Atelet_DetachActor_FullMethodName)
	AteletTerminate       = AteletMethod(ateletpb.Atelet_Terminate_FullMethodName)
)

// ErrSandboxFailed is the sandbox itself failing an operation: ateom or runsc
// returned an error. A real atelet sends most failures as codes.Internal.
var ErrSandboxFailed = status.Error(codes.Internal, "harness: sandbox operation failed")

// snapshotObjects names the objects a fake upload writes. Nothing in ate-api
// reads them; it copies and deletes whatever shares a snapshot's prefix.
var snapshotObjects = []string{"manifest.json", "memory.zst"}

// Sandbox is an actor's workload running on a worker.
type Sandbox struct {
	ActorUID  string
	Atespace  string
	ActorName string
	// WorkerUID is the worker's pod UID, which is its Worker's name.
	WorkerUID string
}

// Atelet is the fake atelet of one node. It keeps the sandboxes running on the
// node's workers and each actor's latest local snapshot, and uploads snapshots
// to the test's ObjectStore. It never uploads or detaches on its own: a
// deferred snapshot stays local until ate-api promotes or detaches it. It
// refuses what a real atelet cannot do, such as checkpointing an actor that is
// not running here.
type Atelet struct {
	objectStore *ObjectStore
	fake        *fakeServer[AteletMethod]

	mu                      sync.Mutex
	epochsByWorkerUID       map[string]int64
	sandboxesByActorUID     map[string]Sandbox
	localSnapshotByActorUID map[string]*ateletpb.Snapshot
}

func newAtelet(objectStore *ObjectStore) *Atelet {
	return &Atelet{
		objectStore:             objectStore,
		fake:                    newFakeServer(AteletRun, AteletCheckpoint, AteletRestore, AteletPromoteSnapshot, AteletDetachActor, AteletTerminate),
		epochsByWorkerUID:       map[string]int64{},
		sandboxesByActorUID:     map[string]Sandbox{},
		localSnapshotByActorUID: map[string]*ateletpb.Snapshot{},
	}
}

// ateletServer serves an Atelet's RPCs to ate-api.
type ateletServer struct {
	ateletpb.UnimplementedAteletServer
	atelet *Atelet
}

// grpcServer returns a gRPC server of a's RPCs, which applies a's faults and
// records its calls. Tests hold only the Atelet, so they reach its RPCs only
// through ate-api.
func (a *Atelet) grpcServer() *grpc.Server {
	srv := a.fake.newServer()
	ateletpb.RegisterAteletServer(srv, ateletServer{atelet: a})
	return srv
}

// On returns the Fault deciding how calls to method behave.
func (a *Atelet) On(method AteletMethod) *Fault {
	return a.fake.on(method)
}

// Sandboxes returns the sandboxes running on this node, sorted by actor UID.
func (a *Atelet) Sandboxes() []Sandbox {
	a.mu.Lock()
	defer a.mu.Unlock()
	out := make([]Sandbox, 0, len(a.sandboxesByActorUID))
	for _, s := range a.sandboxesByActorUID {
		out = append(out, s)
	}
	slices.SortFunc(out, func(x, y Sandbox) int { return cmp.Compare(x.ActorUID, y.ActorUID) })
	return out
}

// LocalSnapshot returns the UUID of the snapshot this node holds for the
// actor, or "" if it holds none.
func (a *Atelet) LocalSnapshot(actorUID string) string {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.localSnapshotByActorUID[actorUID].GetSnapshotUuid()
}

// RecordCalls starts recording the calls a receives from now on.
func (a *Atelet) RecordCalls() *Recording[AteletMethod] {
	return a.fake.record()
}

// registerWorker records the epoch a worker process on this node registered
// with, as atelet learns it when forwarding the registration.
func (a *Atelet) registerWorker(workerUID string, epoch int64) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.epochsByWorkerUID[workerUID] = epoch
}

// restartWorker records the epoch a worker's new process registered with, and
// drops the sandboxes its old process ran.
func (a *Atelet) restartWorker(workerUID string, epoch int64) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.epochsByWorkerUID[workerUID] = epoch
	for uid, s := range a.sandboxesByActorUID {
		if s.WorkerUID == workerUID {
			delete(a.sandboxesByActorUID, uid)
		}
	}
}

func (s ateletServer) Run(_ context.Context, req *ateletpb.RunRequest) (*ateletpb.RunResponse, error) {
	a := s.atelet
	a.mu.Lock()
	defer a.mu.Unlock()
	epoch, ok := a.epochsByWorkerUID[req.GetWorkerPodUid()]
	if !ok {
		return nil, status.Errorf(codes.Unavailable, "while running the workload: no worker %s on this node", req.GetWorkerPodUid())
	}
	a.sandboxesByActorUID[req.GetActorUid()] = Sandbox{
		ActorUID: req.GetActorUid(), Atespace: req.GetAtespace(), ActorName: req.GetActorName(), WorkerUID: req.GetWorkerPodUid(),
	}
	return &ateletpb.RunResponse{WorkerEpoch: epoch}, nil
}

// Checkpoint takes the snapshot locally, replacing the actor's older local
// snapshot, and under UPLOAD_MODE_IMMEDIATE also uploads it. A retry of a
// Checkpoint that took the snapshot is answered from it, as atelet does.
func (s ateletServer) Checkpoint(_ context.Context, req *ateletpb.CheckpointRequest) (*ateletpb.CheckpointResponse, error) {
	a := s.atelet
	a.mu.Lock()
	defer a.mu.Unlock()
	uid := req.GetActorUid()
	snap := req.GetSnapshot()
	sandbox, ok := a.sandboxesByActorUID[uid]
	retry := !ok && a.localSnapshotByActorUID[uid].GetSnapshotUuid() == snap.GetSnapshotUuid()
	if !retry && (!ok || sandbox.WorkerUID != req.GetWorkerPodUid()) {
		return nil, status.Errorf(codes.Internal, "while checkpointing the workload: actor %s is not running on worker %s", uid, req.GetWorkerPodUid())
	}
	state := &ateletpb.ActorSnapshotState{LatestLocal: &ateletpb.SnapshotReport{Snapshot: proto.CloneOf(snap)}}
	switch req.GetUploadConfig().GetMode() {
	case ateletpb.UploadMode_UPLOAD_MODE_DEFERRED:
	case ateletpb.UploadMode_UPLOAD_MODE_IMMEDIATE:
		if err := a.objectStore.writeSnapshot(snap.GetObject().GetSnapshotUri(), snapshotObjects...); err != nil {
			return nil, status.Errorf(codes.Internal, "while uploading the snapshot: %v", err)
		}
		state.LatestDurable = &ateletpb.SnapshotReport{Snapshot: proto.CloneOf(snap)}
	default:
		return nil, status.Errorf(codes.InvalidArgument, "unsupported upload mode %v", req.GetUploadConfig().GetMode())
	}
	a.localSnapshotByActorUID[uid] = proto.CloneOf(snap)
	delete(a.sandboxesByActorUID, uid)
	return &ateletpb.CheckpointResponse{State: state}, nil
}

// Restore reads the snapshot's local copy if this node holds it, and its
// durable copy otherwise.
func (s ateletServer) Restore(_ context.Context, req *ateletpb.RestoreRequest) (*ateletpb.RestoreResponse, error) {
	a := s.atelet
	uid := req.GetActorUid()
	snap := req.GetSnapshot()
	if a.LocalSnapshot(uid) != snap.GetSnapshotUuid() {
		uri := snap.GetObject().GetSnapshotUri()
		names, err := a.objectStore.Snapshot(uri)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "while fetching the snapshot manifest: %v", err)
		}
		// atelet keeps the object-store plugin's code for a missing manifest.
		if len(names) == 0 {
			return nil, status.Errorf(codes.NotFound, "while fetching the snapshot manifest: %s not found", uri)
		}
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	epoch, ok := a.epochsByWorkerUID[req.GetWorkerPodUid()]
	if !ok {
		return nil, status.Errorf(codes.Unavailable, "while restoring the workload: no worker %s on this node", req.GetWorkerPodUid())
	}
	a.sandboxesByActorUID[uid] = Sandbox{
		ActorUID: uid, Atespace: req.GetAtespace(), ActorName: req.GetActorName(), WorkerUID: req.GetWorkerPodUid(),
	}
	return &ateletpb.RestoreResponse{WorkerEpoch: epoch}, nil
}

// PromoteSnapshot uploads the actor's local snapshot and keeps the local copy.
// A snapshot already durable is answered from object storage.
func (s ateletServer) PromoteSnapshot(_ context.Context, req *ateletpb.PromoteSnapshotRequest) (*ateletpb.PromoteSnapshotResponse, error) {
	a := s.atelet
	snap := req.GetSnapshot()
	uri := snap.GetObject().GetSnapshotUri()
	state := &ateletpb.ActorSnapshotState{LatestDurable: &ateletpb.SnapshotReport{Snapshot: proto.CloneOf(snap)}}
	if a.LocalSnapshot(req.GetActorUid()) == snap.GetSnapshotUuid() {
		if err := a.objectStore.writeSnapshot(uri, snapshotObjects...); err != nil {
			return nil, status.Errorf(codes.Internal, "while uploading the snapshot: %v", err)
		}
		state.LatestLocal = &ateletpb.SnapshotReport{Snapshot: proto.CloneOf(snap)}
		return &ateletpb.PromoteSnapshotResponse{State: state}, nil
	}
	names, err := a.objectStore.Snapshot(uri)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "while fetching the snapshot manifest: %v", err)
	}
	if len(names) == 0 {
		return nil, status.Errorf(codes.NotFound, "snapshot %s is neither on this node nor in object storage", snap.GetSnapshotUuid())
	}
	return &ateletpb.PromoteSnapshotResponse{State: state}, nil
}

// upload writes the actor's local snapshot to object storage, keeping the
// local copy, as a background upload does. It returns the snapshot's UUID.
func (a *Atelet) upload(uid string) (string, error) {
	a.mu.Lock()
	snap, ok := a.localSnapshotByActorUID[uid]
	a.mu.Unlock()
	if !ok {
		return "", fmt.Errorf("actor %s has no local snapshot on this node", uid)
	}
	if err := a.objectStore.writeSnapshot(snap.GetObject().GetSnapshotUri(), snapshotObjects...); err != nil {
		return "", err
	}
	return snap.GetSnapshotUuid(), nil
}

// DetachActor uploads the actor's local snapshot, then deletes it from the
// node. An actor with no local snapshot here returns an empty state.
func (s ateletServer) DetachActor(_ context.Context, req *ateletpb.DetachActorRequest) (*ateletpb.DetachActorResponse, error) {
	state, err := s.atelet.detach(req.GetActorUid())
	if err != nil {
		return nil, err
	}
	return &ateletpb.DetachActorResponse{State: state}, nil
}

func (a *Atelet) detach(uid string) (*ateletpb.ActorSnapshotState, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if _, ok := a.sandboxesByActorUID[uid]; ok {
		return nil, status.Errorf(codes.Aborted, "actor %s is running on this node", uid)
	}
	snap, ok := a.localSnapshotByActorUID[uid]
	if !ok {
		return &ateletpb.ActorSnapshotState{}, nil
	}
	if err := a.objectStore.writeSnapshot(snap.GetObject().GetSnapshotUri(), snapshotObjects...); err != nil {
		return nil, status.Errorf(codes.Internal, "while uploading the snapshot: %v", err)
	}
	delete(a.localSnapshotByActorUID, uid)
	return &ateletpb.ActorSnapshotState{LatestDurable: &ateletpb.SnapshotReport{Snapshot: snap}}, nil
}

// Terminate stops the actor's sandbox, if any, and drops its local snapshot
// on this node. Terminating an actor that is not running succeeds.
func (s ateletServer) Terminate(_ context.Context, req *ateletpb.TerminateRequest) (*ateletpb.TerminateResponse, error) {
	a := s.atelet
	a.mu.Lock()
	defer a.mu.Unlock()
	delete(a.sandboxesByActorUID, req.GetActorUid())
	delete(a.localSnapshotByActorUID, req.GetActorUid())
	return &ateletpb.TerminateResponse{}, nil
}

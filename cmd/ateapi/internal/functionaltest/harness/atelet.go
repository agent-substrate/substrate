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

package harness

import (
	"cmp"
	"context"
	"fmt"
	"slices"
	"sync"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"go.ate.dev/substrate/internal/proto/ateletpb"
)

// AteletOp names an atelet RPC a fault can be injected into.
type AteletOp string

const (
	AteletRun             AteletOp = "Run"
	AteletCheckpoint      AteletOp = "Checkpoint"
	AteletRestore         AteletOp = "Restore"
	AteletPromoteSnapshot AteletOp = "PromoteSnapshot"
	AteletDetachActor     AteletOp = "DetachActor"
	AteletTerminate       AteletOp = "Terminate"
)

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

// Call is one RPC an atelet received, faulted or not.
type Call struct {
	Op      AteletOp
	Request proto.Message
}

// Atelet is the fake atelet of one node. It keeps the sandboxes running on the
// node's workers and each actor's latest local snapshot, and uploads snapshots
// to the test's ObjectStore. It never uploads or detaches on its own: a
// deferred snapshot stays local until ate-api promotes or detaches it. It
// refuses what a real atelet cannot do, such as checkpointing an actor that is
// not running here.
type Atelet struct {
	ateletpb.UnimplementedAteletServer

	objectStore *ObjectStore
	faults      map[AteletOp]*Fault

	mu                      sync.Mutex
	calls                   []Call
	epochsByWorkerUID       map[string]int64
	sandboxesByActorUID     map[string]Sandbox
	localSnapshotByActorUID map[string]*ateletpb.Snapshot
}

func newAtelet(objectStore *ObjectStore) *Atelet {
	return &Atelet{
		objectStore: objectStore,
		faults: map[AteletOp]*Fault{
			AteletRun: {}, AteletCheckpoint: {}, AteletRestore: {}, AteletPromoteSnapshot: {}, AteletDetachActor: {}, AteletTerminate: {},
		},
		epochsByWorkerUID:       map[string]int64{},
		sandboxesByActorUID:     map[string]Sandbox{},
		localSnapshotByActorUID: map[string]*ateletpb.Snapshot{},
	}
}

// On returns the Fault deciding how calls to op behave.
func (a *Atelet) On(op AteletOp) *Fault {
	return a.faults[op]
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

// Calls returns every RPC this atelet received, in order.
func (a *Atelet) Calls() []Call {
	a.mu.Lock()
	defer a.mu.Unlock()
	return slices.Clone(a.calls)
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

func (a *Atelet) record(op AteletOp, req proto.Message) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.calls = append(a.calls, Call{Op: op, Request: proto.Clone(req)})
}

func (a *Atelet) Run(ctx context.Context, req *ateletpb.RunRequest) (*ateletpb.RunResponse, error) {
	a.record(AteletRun, req)
	out := a.faults[AteletRun].decide(ctx)
	var epoch int64
	if out.apply {
		a.mu.Lock()
		var ok bool
		epoch, ok = a.epochsByWorkerUID[req.GetWorkerPodUid()]
		if ok {
			a.sandboxesByActorUID[req.GetActorUid()] = Sandbox{
				ActorUID: req.GetActorUid(), Atespace: req.GetAtespace(), ActorName: req.GetActorName(), WorkerUID: req.GetWorkerPodUid(),
			}
		}
		a.mu.Unlock()
		if !ok {
			return nil, status.Errorf(codes.Unknown, "while running the workload: no worker %s on this node", req.GetWorkerPodUid())
		}
	}
	if err := out.result(ctx); err != nil {
		return nil, err
	}
	return &ateletpb.RunResponse{WorkerEpoch: epoch}, nil
}

// Checkpoint takes the snapshot locally, replacing the actor's older local
// snapshot, and under UPLOAD_MODE_IMMEDIATE also uploads it. A retry of a
// Checkpoint that took the snapshot is answered from it, as atelet does.
func (a *Atelet) Checkpoint(ctx context.Context, req *ateletpb.CheckpointRequest) (*ateletpb.CheckpointResponse, error) {
	a.record(AteletCheckpoint, req)
	out := a.faults[AteletCheckpoint].decide(ctx)
	var state *ateletpb.ActorSnapshotState
	if out.apply {
		var err error
		if state, err = a.checkpoint(req); err != nil {
			return nil, err
		}
	}
	if err := out.result(ctx); err != nil {
		return nil, err
	}
	return &ateletpb.CheckpointResponse{State: state}, nil
}

func (a *Atelet) checkpoint(req *ateletpb.CheckpointRequest) (*ateletpb.ActorSnapshotState, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	uid := req.GetActorUid()
	snap := req.GetSnapshot()
	s, ok := a.sandboxesByActorUID[uid]
	retry := !ok && a.localSnapshotByActorUID[uid].GetSnapshotUuid() == snap.GetSnapshotUuid()
	if !retry && (!ok || s.WorkerUID != req.GetWorkerPodUid()) {
		return nil, status.Errorf(codes.Unknown, "while checkpointing the workload: actor %s is not running on worker %s", uid, req.GetWorkerPodUid())
	}
	state := &ateletpb.ActorSnapshotState{LatestLocal: &ateletpb.SnapshotReport{Snapshot: proto.CloneOf(snap)}}
	switch req.GetUploadConfig().GetMode() {
	case ateletpb.UploadMode_UPLOAD_MODE_DEFERRED:
	case ateletpb.UploadMode_UPLOAD_MODE_IMMEDIATE:
		if err := a.objectStore.writeSnapshot(snap.GetObject().GetSnapshotUri(), snapshotObjects...); err != nil {
			return nil, status.Errorf(codes.Unknown, "while uploading the snapshot: %v", err)
		}
		state.LatestDurable = &ateletpb.SnapshotReport{Snapshot: proto.CloneOf(snap)}
	default:
		return nil, status.Errorf(codes.InvalidArgument, "unsupported upload mode %v", req.GetUploadConfig().GetMode())
	}
	a.localSnapshotByActorUID[uid] = proto.CloneOf(snap)
	delete(a.sandboxesByActorUID, uid)
	return state, nil
}

// Restore reads the snapshot's local copy if this node holds it, and its
// durable copy otherwise.
func (a *Atelet) Restore(ctx context.Context, req *ateletpb.RestoreRequest) (*ateletpb.RestoreResponse, error) {
	a.record(AteletRestore, req)
	out := a.faults[AteletRestore].decide(ctx)
	var epoch int64
	if out.apply {
		var err error
		if epoch, err = a.restore(req); err != nil {
			return nil, err
		}
	}
	if err := out.result(ctx); err != nil {
		return nil, err
	}
	return &ateletpb.RestoreResponse{WorkerEpoch: epoch}, nil
}

func (a *Atelet) restore(req *ateletpb.RestoreRequest) (int64, error) {
	uid := req.GetActorUid()
	snap := req.GetSnapshot()
	if a.LocalSnapshot(uid) != snap.GetSnapshotUuid() {
		uri := snap.GetObject().GetSnapshotUri()
		names, err := a.objectStore.Snapshot(uri)
		if err != nil {
			return 0, status.Errorf(codes.Unknown, "while fetching the snapshot manifest: %v", err)
		}
		// atelet keeps the object-store plugin's code for a missing manifest.
		if len(names) == 0 {
			return 0, status.Errorf(codes.NotFound, "while fetching the snapshot manifest: %s not found", uri)
		}
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	epoch, ok := a.epochsByWorkerUID[req.GetWorkerPodUid()]
	if !ok {
		return 0, status.Errorf(codes.Unknown, "while restoring the workload: no worker %s on this node", req.GetWorkerPodUid())
	}
	a.sandboxesByActorUID[uid] = Sandbox{
		ActorUID: uid, Atespace: req.GetAtespace(), ActorName: req.GetActorName(), WorkerUID: req.GetWorkerPodUid(),
	}
	return epoch, nil
}

// PromoteSnapshot uploads the actor's local snapshot and keeps the local copy.
// A snapshot already durable is answered from object storage.
func (a *Atelet) PromoteSnapshot(ctx context.Context, req *ateletpb.PromoteSnapshotRequest) (*ateletpb.PromoteSnapshotResponse, error) {
	a.record(AteletPromoteSnapshot, req)
	out := a.faults[AteletPromoteSnapshot].decide(ctx)
	var state *ateletpb.ActorSnapshotState
	if out.apply {
		var err error
		if state, err = a.promote(req.GetActorUid(), req.GetSnapshot()); err != nil {
			return nil, err
		}
	}
	if err := out.result(ctx); err != nil {
		return nil, err
	}
	return &ateletpb.PromoteSnapshotResponse{State: state}, nil
}

func (a *Atelet) promote(uid string, snap *ateletpb.Snapshot) (*ateletpb.ActorSnapshotState, error) {
	uri := snap.GetObject().GetSnapshotUri()
	state := &ateletpb.ActorSnapshotState{LatestDurable: &ateletpb.SnapshotReport{Snapshot: proto.CloneOf(snap)}}
	if a.LocalSnapshot(uid) == snap.GetSnapshotUuid() {
		if err := a.objectStore.writeSnapshot(uri, snapshotObjects...); err != nil {
			return nil, status.Errorf(codes.Unknown, "while uploading the snapshot: %v", err)
		}
		state.LatestLocal = &ateletpb.SnapshotReport{Snapshot: proto.CloneOf(snap)}
		return state, nil
	}
	names, err := a.objectStore.Snapshot(uri)
	if err != nil {
		return nil, status.Errorf(codes.Unknown, "while fetching the snapshot manifest: %v", err)
	}
	if len(names) == 0 {
		return nil, status.Errorf(codes.NotFound, "snapshot %s is neither on this node nor in object storage", snap.GetSnapshotUuid())
	}
	return state, nil
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
func (a *Atelet) DetachActor(ctx context.Context, req *ateletpb.DetachActorRequest) (*ateletpb.DetachActorResponse, error) {
	a.record(AteletDetachActor, req)
	out := a.faults[AteletDetachActor].decide(ctx)
	state := &ateletpb.ActorSnapshotState{}
	if out.apply {
		var err error
		if state, err = a.detach(req.GetActorUid()); err != nil {
			return nil, err
		}
	}
	if err := out.result(ctx); err != nil {
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
		return nil, status.Errorf(codes.FailedPrecondition, "while uploading the snapshot: %v", err)
	}
	delete(a.localSnapshotByActorUID, uid)
	return &ateletpb.ActorSnapshotState{LatestDurable: &ateletpb.SnapshotReport{Snapshot: snap}}, nil
}

// Terminate stops the actor's sandbox, if any, and drops its local snapshot
// on this node. Terminating an actor that is not running succeeds.
func (a *Atelet) Terminate(ctx context.Context, req *ateletpb.TerminateRequest) (*ateletpb.TerminateResponse, error) {
	a.record(AteletTerminate, req)
	out := a.faults[AteletTerminate].decide(ctx)
	if out.apply {
		a.mu.Lock()
		delete(a.sandboxesByActorUID, req.GetActorUid())
		delete(a.localSnapshotByActorUID, req.GetActorUid())
		a.mu.Unlock()
	}
	if err := out.result(ctx); err != nil {
		return nil, err
	}
	return &ateletpb.TerminateResponse{}, nil
}

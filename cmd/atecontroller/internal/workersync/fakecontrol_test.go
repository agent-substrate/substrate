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

package workersync

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"sync"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// fakeControl is an in-memory stand-in for the Worker half of the Control API.
//
// It reproduces the behavior the syncer is written against and nothing more:
// server-assigned uid and version, the uid+version precondition every update
// carries, the NOT_FOUND / ALREADY_EXISTS / ABORTED codes, a DrainWorker that
// is idempotent down to leaving the version alone, and paged ListWorkers.
//
// Request validation is not mirrored — that is the server's own contract, and
// duplicating it here would only test the copy. A test that needs a rejection
// injects one through setCreateHook.
// The embedded ControlClient is left nil: it supplies the RPCs outside the
// Worker surface so this satisfies the interface, and panics if the syncer ever
// reaches for one of them.
type fakeControl struct {
	ateapipb.ControlClient

	mu      sync.Mutex
	workers map[string]*ateapipb.Worker
	uidSeq  int

	// createHook, when set, decides CreateWorker's outcome: a non-nil error is
	// returned and nothing is registered.
	createHook func(*ateapipb.Worker) error
	// listHook, when set, runs before each ListWorkers and can fail the call.
	listHook func(*ateapipb.ListWorkersRequest) error
	// updateHook, when set, runs once before an UpdateWorker applies, so a test
	// can land a concurrent write and make the precondition fail.
	updateHook func(name string)
	// listPageSize overrides the requested page size when positive, so a
	// handful of workers can be made to span several pages.
	listPageSize int
}

func newFakeControl() *fakeControl {
	return &fakeControl{workers: map[string]*ateapipb.Worker{}}
}

// get returns the registered worker by name, or nil if there is none.
func (f *fakeControl) get(name string) *ateapipb.Worker {
	f.mu.Lock()
	defer f.mu.Unlock()
	w, ok := f.workers[name]
	if !ok {
		return nil
	}
	return proto.Clone(w).(*ateapipb.Worker)
}

// names returns the names of every registered worker, sorted.
func (f *fakeControl) names() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Sorted(maps.Keys(f.workers))
}

// put registers a worker as if it had been created through the API, assigning
// it a uid and version, so a test can start from a worker that already exists.
func (f *fakeControl) put(w *ateapipb.Worker) *ateapipb.Worker {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.putLocked(w)
}

func (f *fakeControl) putLocked(w *ateapipb.Worker) *ateapipb.Worker {
	stored := proto.Clone(w).(*ateapipb.Worker)
	f.uidSeq++
	stored.Metadata = &ateapipb.ResourceMetadata{
		Name:    w.GetMetadata().GetName(),
		Uid:     fmt.Sprintf("worker-uid-%d", f.uidSeq),
		Version: 1,
	}
	if stored.GetStatus() == nil {
		stored.Status = &ateapipb.WorkerStatus{State: ateapipb.WorkerState_WORKER_STATE_ACTIVE}
	}
	f.workers[stored.GetMetadata().GetName()] = stored
	return proto.Clone(stored).(*ateapipb.Worker)
}

func (f *fakeControl) setCreateHook(hook func(*ateapipb.Worker) error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.createHook = hook
}

func (f *fakeControl) setListHook(hook func(*ateapipb.ListWorkersRequest) error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.listHook = hook
}

// setUpdateHookOnce arms a hook that fires on the next UpdateWorker and then
// disarms itself. Firing once is what makes a conflict test converge: the retry
// has to be able to succeed.
func (f *fakeControl) setUpdateHookOnce(hook func(name string)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.updateHook = hook
}

func (f *fakeControl) takeUpdateHook() func(string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	hook := f.updateHook
	f.updateHook = nil
	return hook
}

func (f *fakeControl) GetWorker(_ context.Context, in *ateapipb.GetWorkerRequest, _ ...grpc.CallOption) (*ateapipb.Worker, error) {
	name := in.GetWorker().GetName()
	if w := f.get(name); w != nil {
		return w, nil
	}
	return nil, status.Errorf(codes.NotFound, "Worker %s not found", name)
}

func (f *fakeControl) CreateWorker(_ context.Context, in *ateapipb.CreateWorkerRequest, _ ...grpc.CallOption) (*ateapipb.Worker, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.createHook != nil {
		if err := f.createHook(in.GetWorker()); err != nil {
			return nil, err
		}
	}
	name := in.GetWorker().GetMetadata().GetName()
	if _, ok := f.workers[name]; ok {
		return nil, status.Errorf(codes.AlreadyExists, "Worker %s already exists", name)
	}
	// status is output-only: a Worker is registered only once its pod has an
	// IP, so ACTIVE is the only state it can be born in.
	created := proto.Clone(in.GetWorker()).(*ateapipb.Worker)
	created.Status = &ateapipb.WorkerStatus{State: ateapipb.WorkerState_WORKER_STATE_ACTIVE}
	return f.putLocked(created), nil
}

func (f *fakeControl) UpdateWorker(_ context.Context, in *ateapipb.UpdateWorkerRequest, _ ...grpc.CallOption) (*ateapipb.Worker, error) {
	md := in.GetWorker().GetMetadata()
	// Outside the lock: the hook is how a test lands a concurrent write, and it
	// does that by calling back in.
	if hook := f.takeUpdateHook(); hook != nil {
		hook(md.GetName())
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	// Both guards are required. An update that pins neither is a blind write,
	// which the server rejects before it reaches the store.
	if md.GetUid() == "" || md.GetVersion() == 0 {
		return nil, status.Error(codes.InvalidArgument, "update must carry metadata.uid and metadata.version")
	}
	stored, ok := f.workers[md.GetName()]
	if !ok {
		return nil, status.Errorf(codes.NotFound, "Worker %s not found", md.GetName())
	}
	if md.GetUid() != stored.GetMetadata().GetUid() || md.GetVersion() != stored.GetMetadata().GetVersion() {
		return nil, status.Error(codes.Aborted, "concurrent update conflict, please retry")
	}

	// UpdateWorker replaces the whole resource: the request's Worker is taken
	// wholesale and only the server-owned metadata and status are kept as
	// stored.
	updated := proto.Clone(in.GetWorker()).(*ateapipb.Worker)
	updated.Metadata = proto.Clone(stored.GetMetadata()).(*ateapipb.ResourceMetadata)
	updated.Status = proto.Clone(stored.GetStatus()).(*ateapipb.WorkerStatus)

	// sandbox_class and labels are the only fields an update may change, so
	// pinning those two to what is stored leaves any remaining difference on a
	// field that is immutable after create — including one the request cleared
	// by omitting it.
	probe := proto.Clone(updated).(*ateapipb.Worker)
	probe.SandboxClass = stored.GetSandboxClass()
	probe.Labels = stored.GetLabels()
	if !proto.Equal(probe, stored) {
		return nil, status.Error(codes.InvalidArgument, "update changed a field that is immutable after create")
	}

	updated.Metadata.Version++
	f.workers[md.GetName()] = updated
	return proto.Clone(updated).(*ateapipb.Worker), nil
}

func (f *fakeControl) DeleteWorker(_ context.Context, in *ateapipb.DeleteWorkerRequest, _ ...grpc.CallOption) (*ateapipb.Worker, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	name := in.GetWorker().GetName()
	stored, ok := f.workers[name]
	if !ok {
		return nil, status.Errorf(codes.NotFound, "Worker %s not found", name)
	}
	delete(f.workers, name)
	return stored, nil
}

func (f *fakeControl) DrainWorker(_ context.Context, in *ateapipb.DrainWorkerRequest, _ ...grpc.CallOption) (*ateapipb.Worker, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	name := in.GetWorker().GetName()
	stored, ok := f.workers[name]
	if !ok {
		return nil, status.Errorf(codes.NotFound, "Worker %s not found", name)
	}
	if stored.GetStatus().GetState() == ateapipb.WorkerState_WORKER_STATE_DRAINING {
		// Already draining: no write, and specifically no version bump.
		return proto.Clone(stored).(*ateapipb.Worker), nil
	}
	drained := proto.Clone(stored).(*ateapipb.Worker)
	drained.Status.State = ateapipb.WorkerState_WORKER_STATE_DRAINING
	drained.Metadata.Version++
	f.workers[name] = drained
	return proto.Clone(drained).(*ateapipb.Worker), nil
}

func (f *fakeControl) ListWorkers(_ context.Context, in *ateapipb.ListWorkersRequest, _ ...grpc.CallOption) (*ateapipb.ListWorkersResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.listHook != nil {
		if err := f.listHook(in); err != nil {
			return nil, err
		}
	}

	// The page token is a stateless cursor into the sorted name list, so
	// retrying a failed page with the same token resumes from the same place.
	names := slices.Sorted(maps.Keys(f.workers))
	start := 0
	if in.GetPageToken() != "" {
		var err error
		if start, err = strconv.Atoi(in.GetPageToken()); err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "malformed page token %q", in.GetPageToken())
		}
	}
	size := int(in.GetPageSize())
	if f.listPageSize > 0 {
		size = f.listPageSize
	}
	if size <= 0 {
		size = len(names)
	}
	end := min(start+size, len(names))

	resp := &ateapipb.ListWorkersResponse{}
	for _, name := range names[start:end] {
		resp.Workers = append(resp.Workers, proto.Clone(f.workers[name]).(*ateapipb.Worker))
	}
	if end < len(names) {
		resp.NextPageToken = strconv.Itoa(end)
	}
	return resp, nil
}

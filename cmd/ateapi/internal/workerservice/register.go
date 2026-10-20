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
	"cmp"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"

	"go.ate.dev/substrate/cmd/ateapi/internal/apivalidation"
	"go.ate.dev/substrate/cmd/ateapi/internal/ateletauth"
	"go.ate.dev/substrate/cmd/ateapi/internal/store"
	"go.ate.dev/substrate/internal/apierror"
	"go.ate.dev/substrate/internal/hardware"
	"go.ate.dev/substrate/internal/resources"
	"go.ate.dev/substrate/pkg/proto/ateapipb"
	"google.golang.org/protobuf/proto"
)

// AcquireWorkerEpoch hands out the next epoch of a Worker. As with MintCert,
// the caller must be an atelet running on the Worker's node.
func (s *Server) AcquireWorkerEpoch(ctx context.Context, req *ateapipb.AcquireWorkerEpochRequest) (*ateapipb.AcquireWorkerEpochResponse, error) {
	caller, err := ateletauth.Authenticate(ctx, s.ateletSPIFFEID)
	if err != nil {
		return nil, err
	}
	if errs := apivalidation.ValidateAcquireWorkerEpochRequest(ctx, req); len(errs) > 0 {
		return nil, resources.ToAPIError(errs)
	}
	name := req.GetWorker().GetName()
	worker, err := s.workerOnCallerNode(ctx, name, caller)
	if err != nil {
		return nil, err
	}

	updated, err := s.store.UpdateWorker(ctx, name, store.PreconditionFrom(worker), func(toUpdate *ateapipb.Worker) error {
		toUpdate.Status.IssuedEpoch++
		return nil
	})
	if err != nil {
		return nil, workerWriteError(name, "acquiring an epoch", err)
	}
	epoch := updated.GetStatus().GetIssuedEpoch()
	slog.InfoContext(ctx, "Worker acquired an epoch",
		slog.String("worker", name),
		slog.Int64("epoch", epoch),
		slog.Int64("registered_epoch", updated.GetStatus().GetRegisteredEpoch()))
	return &ateapipb.AcquireWorkerEpochResponse{Epoch: epoch}, nil
}

// RegisterWorker activates the epoch a worker process acquired and records its
// pod IPs and capacity in one write. As with MintCert, the caller must be an
// atelet running on the Worker's node.
func (s *Server) RegisterWorker(ctx context.Context, req *ateapipb.RegisterWorkerRequest) (*ateapipb.RegisterWorkerResponse, error) {
	// TODO(identity): This check should be handled by OpenFGA.
	caller, err := ateletauth.Authenticate(ctx, s.ateletSPIFFEID)
	if err != nil {
		return nil, err
	}
	if errs := apivalidation.ValidateRegisterWorkerRequest(ctx, req); len(errs) > 0 {
		return nil, resources.ToAPIError(errs)
	}
	name := req.GetWorker().GetName()
	worker, err := s.workerOnCallerNode(ctx, name, caller)
	if err != nil {
		return nil, err
	}

	epoch := req.GetEpoch()
	status := worker.GetStatus()
	switch registered := status.GetRegisteredEpoch(); {
	case epoch > status.GetIssuedEpoch():
		return nil, apierror.FailedPrecondition("epoch %d was never handed out for Worker %s", epoch, name)
	case epoch < registered:
		return nil, apierror.FailedPrecondition("epoch %d of Worker %s is superseded by epoch %d", epoch, name, registered)
	case epoch == registered:
		// A worker process registers once, so the registered epoch's registration
		// does not change: an identical one is a retry of the registration
		// that landed, and any other is refused.
		if slices.Equal(status.GetIps(), req.GetIps()) && proto.Equal(status.GetCapacity(), req.GetCapacity()) {
			return &ateapipb.RegisterWorkerResponse{Worker: worker}, nil
		}
		return nil, apierror.FailedPrecondition("epoch %d of Worker %s is already registered with other IPs or capacity", epoch, name)
	}

	// The precondition holds this write to the Worker read above, so epoch is
	// above its registered epoch.
	updated, err := s.store.UpdateWorker(ctx, name, store.PreconditionFrom(worker), func(toUpdate *ateapipb.Worker) error {
		toUpdate.Status.RegisteredEpoch = epoch
		// The sandbox classes were the previous worker process's. This one
		// reports its own, and the Worker takes no Actors until it does.
		toUpdate.Status.SandboxClasses = nil
		toUpdate.Status.Ips = slices.Clone(req.GetIps())
		// Replaces rather than merges: a Worker reports everything it has, so a
		// dimension this report leaves out is one it no longer supplies.
		toUpdate.Status.Capacity = req.GetCapacity()
		return nil
	})
	if err != nil {
		return nil, workerWriteError(name, "registering", err)
	}
	slog.InfoContext(ctx, "Worker registered",
		slog.String("worker", name),
		slog.Int64("epoch", epoch),
		slog.Int64("previous_epoch", status.GetRegisteredEpoch()),
		slog.Any("ips", req.GetIps()),
		slog.String("capacity", req.GetCapacity().String()))
	return &ateapipb.RegisterWorkerResponse{Worker: updated}, nil
}

// SetWorkerSandboxRuntimes records the sandbox classes and runtimes of the
// registered worker process. As with MintCert, the caller must be an atelet
// running on the Worker's node.
func (s *Server) SetWorkerSandboxRuntimes(ctx context.Context, req *ateapipb.SetWorkerSandboxRuntimesRequest) (*ateapipb.SetWorkerSandboxRuntimesResponse, error) {
	caller, err := ateletauth.Authenticate(ctx, s.ateletSPIFFEID)
	if err != nil {
		return nil, err
	}
	if errs := apivalidation.ValidateSetWorkerSandboxRuntimesRequest(ctx, req); len(errs) > 0 {
		return nil, resources.ToAPIError(errs)
	}
	classes := req.GetSandboxClasses()
	name := req.GetWorker().GetName()
	for _, c := range classes {
		sortAttributes(c.GetDefaultRuntime().GetVersion())
		for _, rt := range c.GetRestorableRuntimes() {
			sortAttributes(rt.GetVersion())
		}
	}

	worker, err := s.workerOnCallerNode(ctx, name, caller)
	if err != nil {
		return nil, err
	}
	status := worker.GetStatus()
	if registered := status.GetRegisteredEpoch(); req.GetEpoch() != registered {
		return nil, apierror.FailedPrecondition("epoch %d is not the registered epoch %d of Worker %s", req.GetEpoch(), registered, name)
	}
	if err := validateSandboxClasses(hardware.SandboxClass(status), classes); err != nil {
		return nil, err
	}
	if sameClasses(status.GetSandboxClasses(), classes) {
		return &ateapipb.SetWorkerSandboxRuntimesResponse{Worker: worker}, nil
	}

	updated, err := s.store.UpdateWorker(ctx, name, store.PreconditionFrom(worker), func(toUpdate *ateapipb.Worker) error {
		// Replaces rather than merges: a Worker reports every runtime it has.
		toUpdate.Status.SandboxClasses = classes
		return nil
	})
	if err != nil {
		return nil, workerWriteError(name, "recording sandbox runtimes", err)
	}
	slog.InfoContext(ctx, "Worker reported its sandbox runtimes",
		slog.String("worker", name),
		slog.Int64("epoch", req.GetEpoch()),
		slog.String("sandbox_classes", fmt.Sprint(updated.GetStatus().GetSandboxClasses())))
	return &ateapipb.SetWorkerSandboxRuntimesResponse{Worker: updated}, nil
}

// workerOnCallerNode reads the named Worker, authorized by authoritative state:
// a Worker on another node than the calling atelet's is reported as not found.
func (s *Server) workerOnCallerNode(ctx context.Context, name string, caller *ateletauth.Caller) (*ateapipb.Worker, error) {
	worker, err := s.store.GetWorker(ctx, name)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, apierror.NotFound("Worker %s not found", name)
		}
		return nil, fmt.Errorf("while fetching worker %s: %w", name, err)
	}
	if worker.GetNodeName() != caller.NodeName {
		// Do not disclose Workers on other nodes.
		slog.WarnContext(ctx, "Refusing a report for a worker on another node",
			slog.String("worker", name),
			slog.String("worker_node", worker.GetNodeName()),
			slog.String("caller_node", caller.NodeName),
			slog.String("caller_pod", caller.PodName))
		return nil, apierror.NotFound("Worker %s not found", name)
	}
	if worker.GetStatus() == nil {
		worker.Status = &ateapipb.WorkerStatus{}
	}
	return worker, nil
}

// workerWriteError maps a failed write to a Worker to the error its caller
// retries on: a concurrent write is Aborted, which the caller resends after
// reading the Worker again.
func workerWriteError(name, doing string, err error) error {
	switch {
	case errors.Is(err, store.ErrNotFound):
		return apierror.NotFound("Worker %s not found", name)
	case errors.Is(err, store.ErrUIDConflict), errors.Is(err, store.ErrVersionConflict):
		return apierror.Aborted("concurrent update conflict, please retry")
	default:
		return fmt.Errorf("while %s for worker %s: %w", doing, name, err)
	}
}

// validateSandboxClasses rejects an entry of a class other than the one
// already recorded on the Worker. A worker pod runs one sandbox class for its
// lifetime, and the first report of each worker process records it, so a
// later report of another class contradicts the recorded state. A Worker
// whose worker process has not reported yet has nothing for a report to
// contradict.
func validateSandboxClasses(recorded string, classes []*ateapipb.WorkerSandboxClass) error {
	if recorded == "" {
		return nil
	}
	for i, c := range classes {
		if got := c.GetSandboxClass(); got != recorded {
			return apierror.FailedPrecondition("sandbox_classes[%d].sandbox_class %q does not match Worker's recorded sandbox_class %q", i, got, recorded)
		}
	}
	return nil
}

// sameClasses reports whether two class lists are identical entry for entry,
// in order, restorable runtimes included. A report is compared as sent, so a
// Worker that reorders its runtimes between reports is recorded again.
func sameClasses(a, b []*ateapipb.WorkerSandboxClass) bool {
	return slices.EqualFunc(a, b, func(x, y *ateapipb.WorkerSandboxClass) bool { return proto.Equal(x, y) })
}

// sortAttributes orders a runtime's compatibility attributes by key, in place.
// Attribute order carries no meaning, but proto equality is order-sensitive, so
// a report is canonicalized before it is compared with what is stored: the same
// attributes in another order are the same runtime and write nothing.
func sortAttributes(compat *ateapipb.VersionedSandboxCompat) {
	slices.SortFunc(compat.GetAttributes(), func(a, b *ateapipb.AttributeEntry) int {
		return cmp.Compare(a.GetKey(), b.GetKey())
	})
}

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

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/apivalidation"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/ateletauth"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/internal/apierror"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/protobuf/proto"
)

// RegisterWorker records a Worker's reported capacity and sandbox runtimes in
// one write. As with MintCert, the caller must be an atelet running on the
// Worker's node.
func (s *Server) RegisterWorker(ctx context.Context, req *ateapipb.RegisterWorkerRequest) (*ateapipb.RegisterWorkerResponse, error) {
	// TODO(identity): This check should be handled by OpenFGA.
	caller, err := ateletauth.Authenticate(ctx, s.ateletSPIFFEID)
	if err != nil {
		return nil, err
	}
	if errs := apivalidation.ValidateRegisterWorkerRequest(ctx, req); len(errs) > 0 {
		return nil, resources.ToAPIError(errs)
	}
	reported := req.GetCapacity()
	defaultRuntime := req.GetDefaultRuntime()
	restorable := req.GetRestorableRuntimes()
	name := req.GetWorker().GetName()
	sortAttributes(defaultRuntime)
	for _, rt := range restorable {
		sortAttributes(rt)
	}

	// Use authoritative state to authorize the write.
	worker, err := s.store.GetWorker(ctx, name)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, apierror.NotFound("Worker %s not found", name)
		}
		return nil, fmt.Errorf("while fetching worker %s: %w", name, err)
	}
	if worker.GetNodeName() != caller.NodeName {
		// Do not disclose Workers on other nodes.
		slog.WarnContext(ctx, "Refusing a capacity report for a worker on another node",
			slog.String("worker", name),
			slog.String("worker_node", worker.GetNodeName()),
			slog.String("caller_node", caller.NodeName),
			slog.String("caller_pod", caller.PodName))
		return nil, apierror.NotFound("Worker %s not found", name)
	}
	if err := validateRuntimeClasses(worker.GetSandboxClass(), req); err != nil {
		return nil, err
	}

	status := worker.GetStatus()
	if proto.Equal(status.GetCapacity(), reported) &&
		proto.Equal(status.GetDefaultRuntime(), defaultRuntime) &&
		sameRuntimes(status.GetRestorableRuntimes(), restorable) {
		return &ateapipb.RegisterWorkerResponse{Worker: worker}, nil
	}

	updated, err := s.store.UpdateWorker(ctx, name, store.PreconditionFrom(worker), func(toUpdate *ateapipb.Worker) error {
		// Replaces rather than merges: a Worker reports everything it has, so a
		// dimension or runtime this report leaves out is one it no longer
		// supplies.
		toUpdate.Status.Capacity = reported
		toUpdate.Status.DefaultRuntime = defaultRuntime
		toUpdate.Status.RestorableRuntimes = restorable
		return nil
	})
	switch {
	case err == nil:
	case errors.Is(err, store.ErrNotFound):
		return nil, apierror.NotFound("Worker %s not found", name)
	case errors.Is(err, store.ErrUIDConflict), errors.Is(err, store.ErrVersionConflict):
		return nil, apierror.Aborted("concurrent update conflict, please retry")
	default:
		return nil, fmt.Errorf("while recording capacity for worker %s: %w", name, err)
	}
	slog.InfoContext(ctx, "Worker registered its capacity and sandbox runtimes",
		slog.String("worker", name),
		slog.String("was", worker.GetStatus().GetCapacity().String()),
		slog.String("now", updated.GetStatus().GetCapacity().String()),
		slog.String("default_runtime", updated.GetStatus().GetDefaultRuntime().String()),
		slog.Int("restorable_runtimes", len(updated.GetStatus().GetRestorableRuntimes())))
	return &ateapipb.RegisterWorkerResponse{Worker: updated}, nil
}

// validateRuntimeClasses rejects a runtime of a class other than the Worker's
// own. The class is fixed when the pool creates the Worker and names the
// sandbox its ateom runs, so a runtime of another class contradicts the
// recorded Worker state. A Worker recorded without a class has nothing for a
// report to contradict.
func validateRuntimeClasses(class string, req *ateapipb.RegisterWorkerRequest) error {
	if class == "" {
		return nil
	}
	if got := req.GetDefaultRuntime().GetSandboxClass(); got != class {
		return apierror.FailedPrecondition("default_runtime.sandbox_class %q does not match Worker's sandbox_class %q", got, class)
	}
	for i, rt := range req.GetRestorableRuntimes() {
		if got := rt.GetSandboxClass(); got != class {
			return apierror.FailedPrecondition("restorable_runtimes[%d].sandbox_class %q does not match Worker's sandbox_class %q", i, got, class)
		}
	}
	return nil
}

// sameRuntimes reports whether two runtime lists are identical entry for
// entry, in order. A report is compared as sent, so a Worker that reorders its
// runtimes between reports is recorded again.
func sameRuntimes(a, b []*ateapipb.SandboxRuntime) bool {
	return slices.EqualFunc(a, b, func(x, y *ateapipb.SandboxRuntime) bool { return proto.Equal(x, y) })
}

// sortAttributes orders a runtime's compatibility attributes by key, in place.
// Attribute order carries no meaning, but proto equality is order-sensitive, so
// a report is canonicalized before it is compared with what is stored: the same
// attributes in another order are the same runtime and write nothing.
func sortAttributes(rt *ateapipb.SandboxRuntime) {
	slices.SortFunc(rt.GetVersion().GetAttributes(), func(a, b *ateapipb.AttributeEntry) int {
		return cmp.Compare(a.GetKey(), b.GetKey())
	})
}

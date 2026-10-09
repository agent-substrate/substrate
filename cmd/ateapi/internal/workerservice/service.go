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

// Package workerservice serves the RPCs a Worker uses to tell the control
// plane about itself and what it observes.
package workerservice

import (
	"context"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/internal/localca"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

// Server implements ateapipb.WorkerServiceServer.
type Server struct {
	ateapipb.UnimplementedWorkerServiceServer

	// store is where a Worker's reports are recorded, and the authoritative
	// state every request is authorized against.
	store store.Interface

	// suspender runs the suspend a Worker asks for. It is the same entry point
	// Control.SuspendActor uses, so a Worker's request is subject to every
	// precondition a client's would be.
	suspender actorSuspender

	// workers lists every Worker the control plane knows, for the answers
	// scoped to the caller's node. It is the watch-fed worker cache rather
	// than the store, so serving a node does not read every Worker row.
	workers workerLister

	// ateletSPIFFEID is the identity the calling atelet must present.
	ateletSPIFFEID string

	actorIDCAPool localca.Pool
}

// actorSuspender is the in-process slice of the Control service this package
// drives. *controlapi.RPCService satisfies it.
type actorSuspender interface {
	SuspendActor(ctx context.Context, req *ateapipb.SuspendActorRequest) (*ateapipb.SuspendActorResponse, error)
}

// workerLister is the slice of the worker cache this package reads.
// *workercache.Cache satisfies it.
type workerLister interface {
	Workers() ([]*ateapipb.Worker, error)
}

var _ ateapipb.WorkerServiceServer = (*Server)(nil)

func New(store store.Interface, suspender actorSuspender, workers workerLister, ateletSPIFFEID string, actorIDCAPool localca.Pool) *Server {
	return &Server{
		store:          store,
		suspender:      suspender,
		workers:        workers,
		ateletSPIFFEID: ateletSPIFFEID,
		actorIDCAPool:  actorIDCAPool,
	}
}

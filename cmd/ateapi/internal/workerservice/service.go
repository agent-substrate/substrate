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

	"go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/cmd/ateapi/internal/store"
	"go.ate.dev/substrate/internal/localca"
	"go.ate.dev/substrate/internal/localjwtauthority"
	"go.ate.dev/substrate/internal/resources"
)

// Server implements ateapi.WorkerServiceServer.
type Server struct {
	ateapi.UnimplementedWorkerServiceServer

	// store is where a Worker's reports are recorded, and the authoritative
	// state every request is authorized against.
	store store.Interface

	// control runs the operations a Worker asks for through the same entry
	// points Control uses, so a Worker's request is subject to every
	// precondition a client's would be.
	control actorControl

	// ateletSPIFFEID is the identity the calling atelet must present.
	ateletSPIFFEID string

	actorIDCAPool localca.Pool

	// actorJWTIssuer is copied verbatim into the iss claim of every
	// worker-for-actor JWT, and actorIDJWTPool signs them. Both are the ones
	// Control.MintActorJwt uses.
	actorJWTIssuer string
	actorIDJWTPool localjwtauthority.Pool
}

// actorControl is the in-process slice of the Control service this package
// drives. *controlapi.RPCService satisfies it.
type actorControl interface {
	SuspendActor(ctx context.Context, req *ateapi.SuspendActorRequest) (*ateapi.SuspendActorResponse, error)
	ApplyActorSnapshotState(ctx context.Context, actorRef resources.ActorRef, actorUID, node string, state *ateapi.ActorSnapshotState) error
	CrashActorReportedByWorker(ctx context.Context, actorRef resources.ActorRef, actorUID, workerName, message string) (*ateapi.Actor, error)
}

var _ ateapi.WorkerServiceServer = (*Server)(nil)

func New(store store.Interface, control actorControl, ateletSPIFFEID string, actorIDCAPool localca.Pool, actorJWTIssuer string, actorIDJWTPool localjwtauthority.Pool) *Server {
	return &Server{
		store:          store,
		control:        control,
		ateletSPIFFEID: ateletSPIFFEID,
		actorIDCAPool:  actorIDCAPool,
		actorJWTIssuer: actorJWTIssuer,
		actorIDJWTPool: actorIDJWTPool,
	}
}

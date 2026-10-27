// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package workerservice

import (
	"context"

	"go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/cmd/ateapi/internal/apivalidation"
	"go.ate.dev/substrate/cmd/ateapi/internal/ateletauth"
	"go.ate.dev/substrate/internal/resources"
)

// ReportActorSnapshotState records the snapshot state that the calling atelet
// reports for an Actor on its node.
func (s *Server) ReportActorSnapshotState(ctx context.Context, req *ateapi.ReportActorSnapshotStateRequest) (*ateapi.ReportActorSnapshotStateResponse, error) {
	// Authz doesn't cover WorkerService, so authenticate before reading the
	// request.
	caller, err := ateletauth.Authenticate(ctx, s.ateletSPIFFEID)
	if err != nil {
		return nil, err
	}
	if errs := apivalidation.ValidateReportActorSnapshotStateRequest(ctx, req); len(errs) > 0 {
		return nil, resources.ToAPIError(errs)
	}
	actorRef := resources.ActorRefFromObjectRef(req.GetActor())
	if err := s.control.ApplyActorSnapshotState(ctx, actorRef, req.GetActorUid(), caller.NodeName, req.GetState()); err != nil {
		return nil, err
	}
	return &ateapi.ReportActorSnapshotStateResponse{}, nil
}

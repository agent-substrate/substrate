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
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/apivalidation"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/ateletauth"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/internal/apierror"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

// nodeActorsPageSize is the page size for reading one Worker's assignments. A
// variable so a test can spread a Worker's assignments across pages.
var nodeActorsPageSize = store.DefaultPageSize

// ListNodeActorUIDs returns the UIDs of the Actors assigned to the Workers on
// the caller's node. The caller must be an atelet, and the node is the one its
// certificate names.
//
// The Workers come from the worker cache and their assignments from the store.
// The cache is fed by a watch, so it lags the store: a Worker created within
// the watch's delay is missing, and so is one whose event was dropped, until
// the cache's next successful full list. Either way its Actors are missing with
// it. So the call fails with UNAVAILABLE once the last full list that succeeded
// started max_staleness_seconds or more ago. A Worker the answer misses was
// then created less than max_staleness_seconds before the call, and so were
// its Actors' directories, since an Actor is placed after its Worker exists. A
// caller that keeps every directory younger than the bound keeps theirs, as
// long as it measures the ages from before the call. A cache whose watch broke
// reports not-ready, and the call fails rather than answer from a partial view.
func (s *Server) ListNodeActorUIDs(ctx context.Context, req *ateapipb.ListNodeActorUIDsRequest) (*ateapipb.ListNodeActorUIDsResponse, error) {
	caller, err := ateletauth.Authenticate(ctx, s.ateletSPIFFEID)
	if err != nil {
		return nil, err
	}
	if errs := apivalidation.ValidateListNodeActorUIDsRequest(ctx, req); len(errs) > 0 {
		return nil, resources.ToAPIError(errs)
	}

	// Read before Workers: the cache records a list's start only once the
	// list's result is in place, so the Workers below are at least this fresh.
	synced := s.workers.LastSynced()
	workers, err := s.workers.Workers()
	if err != nil {
		return nil, apierror.Unavailable("the worker cache is not ready: %v", err)
	}
	// A cache that never listed returns the zero time, whose age saturates.
	maxStaleness := time.Duration(req.GetMaxStalenessSeconds()) * time.Second
	if age := time.Since(synced); age >= maxStaleness {
		return nil, apierror.Unavailable("the worker cache's last full list started %v ago, at least max_staleness_seconds (%v)", age.Round(time.Second), maxStaleness)
	}
	var uids []string
	nodeWorkers := 0
	for _, w := range workers {
		// Authenticate refuses a certificate that names no node, so this
		// never matches a Worker without one.
		//
		// TODO: a linear scan of every Worker per call. Index the cache by
		// node if this shows up in profiles.
		if w.GetNodeName() != caller.NodeName {
			continue
		}
		nodeWorkers++
		if uids, err = s.appendAssignedActorUIDs(ctx, uids, w.GetMetadata().GetName()); err != nil {
			return nil, err
		}
	}
	slog.DebugContext(ctx, "Listed the actors placed on a node",
		slog.String("node", caller.NodeName),
		slog.Int("workers", nodeWorkers),
		slog.Int("actors", len(uids)))
	return &ateapipb.ListNodeActorUIDsResponse{ActorUids: uids}, nil
}

// appendAssignedActorUIDs appends the UIDs of the Actors assigned to one
// Worker. A Worker deleted since the cache last saw it has no assignments
// left, so it adds nothing rather than failing the call.
func (s *Server) appendAssignedActorUIDs(ctx context.Context, uids []string, workerName string) ([]string, error) {
	for token := ""; ; {
		page, err := s.store.ListWorkerAssignments(ctx, workerName, store.ListOptions{PageSize: nodeActorsPageSize, PageToken: token})
		if err != nil {
			return nil, fmt.Errorf("while listing the actors assigned to worker %s: %w", workerName, err)
		}
		for _, assignment := range page.Items {
			uids = append(uids, assignment.GetActorUid())
		}
		if !page.HasNextPage() {
			return uids, nil
		}
		token = page.NextPageToken
	}
}

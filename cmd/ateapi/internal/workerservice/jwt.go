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
	"crypto/rand"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"go.ate.dev/substrate/cmd/ateapi/internal/apivalidation"
	"go.ate.dev/substrate/cmd/ateapi/internal/ateletauth"
	"go.ate.dev/substrate/cmd/ateapi/internal/controlapi"
	"go.ate.dev/substrate/cmd/ateapi/internal/store"
	"go.ate.dev/substrate/internal/actoridjwt"
	"go.ate.dev/substrate/internal/apierror"
	"go.ate.dev/substrate/internal/resources"
	"go.ate.dev/substrate/pkg/proto/ateapipb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// workerActorJWTLifetime is the fixed lifetime of a worker-for-actor JWT. It
// is not chosen by the caller: the token's only consumers are node-side
// plugins that exchange it for short-lived credentials.
const workerActorJWTLifetime = time.Hour

func (s *Server) MintWorkerActorJwt(ctx context.Context, req *ateapipb.MintWorkerActorJwtRequest) (*ateapipb.MintWorkerActorJwtResponse, error) {
	if errs := apivalidation.ValidateMintWorkerActorJwtRequest(ctx, req); len(errs) > 0 {
		return nil, resources.ToAPIError(errs)
	}

	caller, err := ateletauth.Authenticate(ctx, s.ateletSPIFFEID)
	if err != nil {
		return nil, err
	}

	// The actor doesn't need to be running: tokens are needed to fetch a
	// snapshot before resume and to upload a checkpoint during or after suspend.
	dbActor, err := s.store.GetActor(ctx, resources.ActorRefFromObjectRef(req.GetActor()))
	if errors.Is(err, store.ErrNotFound) {
		return nil, apierror.NotFound("actor not found")
	} else if err != nil {
		return nil, fmt.Errorf("while retrieving actor: %w", err)
	}
	// The node check comes first so a node that does not host the actor gets
	// NotFound whatever UID it sends, and cannot use the UID check to learn
	// that the actor exists.
	if err := s.checkActorOnNode(ctx, dbActor, caller.NodeName, "JWT"); err != nil {
		return nil, err
	}
	// A stale UID is Aborted, as in MintWorkerActorCertificate.
	if dbActor.GetMetadata().GetUid() != req.GetActorUid() {
		return nil, apierror.Aborted("conflict; actor has been deleted and recreated")
	}

	// JWT timestamps have one-second resolution. Truncating keeps expire_time
	// equal to the exp claim.
	now := time.Now().Truncate(time.Second)
	expiresAt := now.Add(workerActorJWTLifetime)
	meta := dbActor.GetMetadata()
	claims := &actoridjwt.Claims{
		Kind:       actoridjwt.KindWorkerForActor,
		Issuer:     s.actorJWTIssuer,
		Subject:    fmt.Sprintf("%s/%s/%s", actoridjwt.KindWorkerForActor.SubjectPrefix(), meta.GetAtespace(), meta.GetName()),
		Audiences:  req.GetAudiences(),
		Expiration: expiresAt,
		NotBefore:  now.Add(-5 * time.Minute),
		IssuedAt:   now,
		JTI:        rand.Text(),

		Substrate: actoridjwt.SubstrateClaims{
			Atespace:  meta.GetAtespace(),
			ActorName: meta.GetName(),
			ActorUID:  meta.GetUid(),
			Node:      caller.NodeName,
		},
	}
	token, err := s.actorIDJWTPool.SignJWT(claims)
	if err != nil {
		return nil, fmt.Errorf("while signing worker-for-actor JWT: %w", err)
	}
	return &ateapipb.MintWorkerActorJwtResponse{
		ActorJwt:   token,
		ExpireTime: timestamppb.New(expiresAt),
	}, nil
}

// checkActorOnNode refuses a mint from an atelet whose node does not host the
// actor, as RequestActorSuspend does. Authoritative state decides, never the
// request. The refusal is NotFound, so a node learns nothing about actors it
// does not host. credential names what the caller mints, for the refusal log.
func (s *Server) checkActorOnNode(ctx context.Context, actor *ateapipb.Actor, callerNode, credential string) error {
	hostingNode, err := controlapi.HostingNode(ctx, s.store, actor)
	if err != nil {
		return err
	}
	if hostingNode == "" || hostingNode != callerNode {
		slog.WarnContext(ctx, "Refusing a worker-for-actor "+credential+" for an actor the caller's node does not host",
			slog.String("actor", resources.ActorRefFromActor(actor).String()),
			slog.String("hosting_node", hostingNode),
			slog.String("caller_node", callerNode))
		return apierror.NotFound("actor not found")
	}
	return nil
}

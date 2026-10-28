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

package main

import (
	"context"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/utils/lru"

	"go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/internal/objectstoreplugin/exchange"
)

// actorRef identifies the actor a transfer is for, as atelet's RPCs carry it.
type actorRef struct {
	atespace, name, uid string
}

const (
	// workerActorJWTRefreshMargin is the least remaining life a cached JWT is used
	// with. The JWT only has to be valid when the node plugin exchanges it,
	// not for the duration of a transfer.
	workerActorJWTRefreshMargin = 5 * time.Minute
	// workerActorJWTCacheSize bounds the per-actor cache. A miss costs one mint,
	// never correctness.
	workerActorJWTCacheSize = 10_000
)

type cachedWorkerActorJWT struct {
	jwt    string
	expiry time.Time
}

// workerActorJWTs mints worker-for-actor JWTs through ate-api-server and caches
// them per actor UID. The node plugin trades one for storage credentials
// scoped to the actor, so atelet sends one with every snapshot transfer.
type workerActorJWTs struct {
	workers ateapi.WorkerServiceClient
	now     func() time.Time
	cache   *lru.Cache
}

func newWorkerActorJWTs(workers ateapi.WorkerServiceClient) *workerActorJWTs {
	return &workerActorJWTs{workers: workers, now: time.Now, cache: lru.New(workerActorJWTCacheSize)}
}

// get returns a JWT for the actor. The cache serves it while more than
// workerActorJWTRefreshMargin remains, otherwise ate-api-server mints a new
// one. A mint failure comes back as a gRPC status with the code a plugin
// failure would carry, so the transfer helpers handle both the same way:
//   - Unavailable: ate-api-server could not be reached. Kept, so the callers
//     that retry a transfer do.
//   - FailedPrecondition: the actor is not this node's to transfer for, which
//     the mint reports as NotFound or Aborted. Renamed because callers read
//     NotFound from a transfer as a missing snapshot object.
//   - Internal: anything else, including an empty JWT in the response.
func (j *workerActorJWTs) get(ctx context.Context, actor actorRef) (string, error) {
	if v, ok := j.cache.Get(actor.uid); ok {
		if c := v.(cachedWorkerActorJWT); c.expiry.Sub(j.now()) > workerActorJWTRefreshMargin {
			return c.jwt, nil
		}
	}
	resp, err := j.workers.MintWorkerActorJwt(ctx, &ateapi.MintWorkerActorJwtRequest{
		Actor:     &ateapi.ObjectRef{Atespace: actor.atespace, Name: actor.name},
		ActorUid:  actor.uid,
		Audiences: []string{exchange.ActorJWTAudience},
	})
	switch status.Code(err) {
	case codes.OK:
	case codes.Unavailable:
		return "", status.Errorf(codes.Unavailable, "while minting the actor JWT for %s/%s: %s", actor.atespace, actor.name, status.Convert(err).Message())
	case codes.NotFound, codes.Aborted:
		return "", status.Errorf(codes.FailedPrecondition, "actor %s/%s cannot be transferred from this node: %s", actor.atespace, actor.name, status.Convert(err).Message())
	default:
		return "", status.Errorf(codes.Internal, "while minting the actor JWT for %s/%s: %s", actor.atespace, actor.name, status.Convert(err).Message())
	}
	if resp.GetActorJwt() == "" {
		return "", status.Errorf(codes.Internal, "ate-api-server minted an empty actor JWT for %s/%s", actor.atespace, actor.name)
	}
	j.cache.Add(actor.uid, cachedWorkerActorJWT{jwt: resp.GetActorJwt(), expiry: resp.GetExpireTime().AsTime()})
	return resp.GetActorJwt(), nil
}

// workerActorJWT is the JWT a transfer for actor carries.
func (s *Atelet) workerActorJWT(ctx context.Context, actor actorRef) (string, error) {
	if s.workerActorJWTs == nil {
		return "", status.Error(codes.Internal, "actor JWT source is not configured")
	}
	return s.workerActorJWTs.get(ctx, actor)
}

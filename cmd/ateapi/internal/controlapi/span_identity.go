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

package controlapi

import (
	"context"

	"go.opentelemetry.io/otel/trace"

	"github.com/agent-substrate/substrate/internal/ateattr"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

// setSpanActorIdentity annotates the RPC's server span (from ctx) with the
// actor's full identity. A no-op when ctx carries no recording span.
func setSpanActorIdentity(ctx context.Context, a *ateapipb.Actor) {
	trace.SpanFromContext(ctx).SetAttributes(ateattr.ActorIdentity(a)...)
}

// setSpanActorRefIdentity is setSpanActorIdentity for the identity subset known
// before the Actor record resolves, so a failed lookup still carries who/where.
func setSpanActorRefIdentity(ctx context.Context, atespace, actorID string) {
	trace.SpanFromContext(ctx).SetAttributes(ateattr.ActorRefIdentity(atespace, actorID)...)
}

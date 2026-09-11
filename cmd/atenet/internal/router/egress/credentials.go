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

package egress

import (
	"context"
	"log/slog"

	envoy_type "github.com/envoyproxy/go-control-plane/envoy/type/v3"

	"github.com/agent-substrate/substrate/cmd/atenet/internal/router/extproc"
	"github.com/agent-substrate/substrate/internal/egresspolicy"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

// applyEffects applies a matched hostname rule's effects to the request.
//
// TODO(yufan-su): implement credential injection. For each
// CredentialHeaderInjection, resolve credential_uri through the Credential
// Provider RPC (a service the gateway will call when a request needs a
// secret; the URI names the provider class and name), then set the header
// to prefix + value with OVERWRITE_IF_EXISTS_OR_ADD so an actor-supplied
// header never wins. Until that lands, a rule that promises an injection
// denies: the policy author expects the secret to be attached, and forwarding
// without it would be the API saying one thing and the gateway doing another.
func applyEffects(ctx context.Context, ref resources.ActorRef, dest egresspolicy.Destination, effects *ateapipb.EgressRuleEffects) error {
	if len(effects.GetInjectStaticHeaders()) == 0 {
		return nil
	}
	slog.WarnContext(ctx, "egress denied: matching rule requires credential injection, which is not implemented",
		slog.Any("actor", ref), slog.String("host", dest.Hostname))
	return extproc.NewReqError(envoy_type.StatusCode_NotImplemented, deniedBody)
}

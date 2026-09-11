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
	"errors"
	"fmt"
	"log/slog"

	envoy_type "github.com/envoyproxy/go-control-plane/envoy/type/v3"

	"github.com/agent-substrate/substrate/cmd/atenet/internal/router/extproc"
	"github.com/agent-substrate/substrate/internal/egresspolicy"
	"github.com/agent-substrate/substrate/internal/resources"
)

// handleRequest authorizes one request the gateway can read: cleartext HTTP,
// or HTTPS the sdsmint gateway terminated. It runs per request, because the
// Host can change between requests on one connection.
//
// The destination is the request's own :authority, which is what the gateway
// dials. An IP-literal Host is matched by ip_blocks, a DNS name by hostnames
// or all. The tunnel's original destination is not consulted: the gateway does
// not dial it on these legs.
func (h *Handler) handleRequest(ctx context.Context, md *extproc.RequestMetadata, leg string) (extproc.Result, error) {
	ref, err := actorFromFilterState(md)
	if err != nil {
		slog.WarnContext(ctx, "egress denied: request carries no actor identity", slog.String("leg", leg), slog.Any("err", err))
		return extproc.Result{}, extproc.WrapReqError(envoy_type.StatusCode_Forbidden, err, deniedBody)
	}
	dest, err := requestDestination(md)
	if err != nil {
		slog.WarnContext(ctx, "egress denied: request names no destination a rule could allow",
			slog.Any("actor", ref), slog.String("leg", leg), slog.Any("err", err))
		return extproc.Result{}, extproc.WrapReqError(envoy_type.StatusCode_Forbidden, err, deniedBody)
	}
	policy, err := h.lookupPolicy(ctx, leg, ref)
	if err != nil {
		return extproc.Result{}, err
	}

	decision := policy.Evaluate(dest)
	// Built lazily: the allow path logs nothing at the default level.
	attrs := func() []any {
		return []any{
			slog.Any("actor", ref),
			slog.String("leg", leg),
			slog.String("method", md.Method),
			slog.String("host", md.Host),
			slog.String("originalDestination", md.Attribute(extproc.OriginalDstAttribute)),
			slog.String("sni", md.Attribute(extproc.RequestedServerNameAttribute)),
			slog.Int("rule", decision.RuleIndex),
		}
	}
	if !decision.Allowed {
		// TODO(liorlieberman): do we need an audit mode to roll a policy out
		// against live traffic without denying it first?
		slog.WarnContext(ctx, "egress denied: no rule allows the destination", attrs()...)
		return extproc.Result{}, extproc.NewReqError(envoy_type.StatusCode_Forbidden, deniedBody)
	}
	if err := applyEffects(ctx, ref, dest, decision.Effects); err != nil {
		return extproc.Result{}, err
	}
	if slog.Default().Enabled(ctx, slog.LevelDebug) {
		slog.DebugContext(ctx, "egress allowed", attrs()...)
	}
	return allow(), nil
}

// requestDestination is the request's :authority, which dynamic_forward_proxy
// dials. A Host header naming something else is refused rather than policed on
// one name and dialed on the other. Envoy itself never delivers two different
// values: HTTP/1.1 has only Host, which it stores as :authority, and on HTTP/2
// it drops a Host sent alongside :authority. The check guards a dataplane that
// forwards both.
func requestDestination(md *extproc.RequestMetadata) (egresspolicy.Destination, error) {
	authority := md.Header(extproc.AuthorityHeader)
	host := md.Header("host")
	if authority == "" {
		authority = host
	}
	dest, err := egresspolicy.NormalizeAuthority(authority)
	if err != nil {
		return egresspolicy.Destination{}, err
	}
	if host != "" && host != authority {
		fromHost, err := egresspolicy.NormalizeAuthority(host)
		if err != nil || fromHost != dest {
			return egresspolicy.Destination{}, fmt.Errorf("request :authority %q and Host %q name different destinations", authority, host)
		}
	}
	return dest, nil
}

// actorFromFilterState reads the actor from the identity filter state the
// outer CONNECT chain set from the verified peer certificate.
func actorFromFilterState(md *extproc.RequestMetadata) (resources.ActorRef, error) {
	id := md.Attribute(extproc.ActorIdentityFilterStateAttribute)
	if id == "" {
		return resources.ActorRef{}, errors.New("no actor identity in filter state")
	}
	return resources.ActorRefFromSPIFFEID(id)
}

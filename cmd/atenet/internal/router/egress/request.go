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
	"net"
	"strings"

	extprocv3 "github.com/envoyproxy/go-control-plane/envoy/service/ext_proc/v3"
	envoy_type "github.com/envoyproxy/go-control-plane/envoy/type/v3"

	"github.com/agent-substrate/substrate/cmd/atenet/internal/router/extproc"
	"github.com/agent-substrate/substrate/internal/egresspolicy"
	"github.com/agent-substrate/substrate/internal/resources"
)

// handleRequest authorizes one request the gateway can read: cleartext HTTP,
// or HTTPS the sdsmint gateway terminated. It runs per request, because the
// Host can change between requests on one connection.
//
// The rules are walked once, in policy order, over the Host the request named
// and the address the actor dialed; the first match decides. The answer also
// says where the request goes, so the bytes reach what the rule checked: a
// hostname match is resolved and dialed by name, an address or all match goes
// to the address the actor dialed.
func (h *Handler) handleRequest(ctx context.Context, md *extproc.RequestMetadata, leg string) (extproc.Result, error) {
	if isWebSocketUpgrade(md) {
		slog.WarnContext(ctx, "egress denied: WebSocket upgrades are not supported", slog.String("leg", leg), slog.String("host", md.Host))
		return extproc.Result{}, extproc.NewReqError(envoy_type.StatusCode_Forbidden, deniedBody)
	}

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
	dial := extproc.EgressDialAddress
	if decision.ByName {
		dial = extproc.EgressDialName
	}
	// Built lazily: the allow path logs nothing at the default level.
	attrs := func() []any {
		return []any{
			slog.Any("actor", ref),
			slog.String("leg", leg),
			slog.String("method", md.Method),
			slog.String("host", md.Host),
			slog.String("originalDestination", dialedAddress(md)),
			slog.String("sni", md.Attribute(extproc.RequestedServerNameAttribute)),
			slog.Int("rule", decision.RuleIndex),
			slog.String("dial", dial),
		}
	}
	if !decision.Allowed {
		// TODO(liorlieberman): do we need an audit mode to roll a policy out
		// against live traffic without denying it first?
		slog.WarnContext(ctx, "egress denied: no rule allows the destination", attrs()...)
		return extproc.Result{}, extproc.NewReqError(envoy_type.StatusCode_Forbidden, deniedBody)
	}
	injected, err := h.applyEffects(ctx, ref, dest, leg, decision.Effects)
	if err != nil {
		return extproc.Result{}, err
	}
	if slog.Default().Enabled(ctx, slog.LevelDebug) {
		slog.DebugContext(ctx, "egress allowed", append(attrs(), slog.Int("injectedHeaders", len(injected)))...)
	}
	res := allow()
	// ext_proc only honors the clear when the response also carries a header
	// mutation, on the assumption that nothing else can move a route, so an
	// empty one goes along — carrying any injected credential headers.
	res.Response.Response.ClearRouteCache = true
	res.Response.Response.HeaderMutation = &extprocv3.HeaderMutation{SetHeaders: injected}
	res.DynamicMetadata = metadataAnswer(extproc.EgressDialKey, dial)
	return res, nil
}

func isWebSocketUpgrade(md *extproc.RequestMetadata) bool {
	return md.Method == "GET" && hasHTTPToken(md.Header("connection"), "upgrade") && hasHTTPToken(md.Header("upgrade"), "websocket")
}

func hasHTTPToken(value, want string) bool {
	for _, token := range strings.Split(value, ",") {
		if strings.EqualFold(strings.TrimSpace(token), want) {
			return true
		}
	}
	return false
}

// requestDestination is what the request is going to: the Host, when it is a
// DNS name, and the address the actor dialed, which the CONNECT leg's answer
// shares as filter state when an address rule allowed it. An IP-literal Host
// names no host, and cidrs rules match the dialed address, never the Host.
//
// A Host header naming something other than :authority is refused rather than
// policed on one name and dialed on the other. Envoy itself never delivers two
// different values: HTTP/1.1 has only Host, which it stores as :authority, and
// on HTTP/2 it drops a Host sent alongside :authority. The check guards a
// dataplane that forwards both.
func requestDestination(md *extproc.RequestMetadata) (egresspolicy.Destination, error) {
	authority := md.Header(extproc.AuthorityHeader)
	host := md.Header("host")
	if authority == "" {
		authority = host
	}
	named, err := egresspolicy.NormalizeAuthority(authority)
	if err != nil {
		return egresspolicy.Destination{}, err
	}
	if host != "" && host != authority {
		fromHost, err := egresspolicy.NormalizeAuthority(host)
		if err != nil || fromHost != named {
			return egresspolicy.Destination{}, fmt.Errorf("request :authority %q and Host %q name different destinations", authority, host)
		}
	}
	dest := egresspolicy.Destination{Hostname: named.Hostname, Port: named.Port}
	if raw := dialedAddress(md); raw != "" {
		dialed, err := egresspolicy.NormalizeAuthority(raw)
		if err != nil || !dialed.IP.IsValid() {
			return egresspolicy.Destination{}, fmt.Errorf("tunnel destination %q is not an IP:port", raw)
		}
		dest.IP, dest.Port = dialed.IP, dialed.Port
	}
	return dest, nil
}

// dialedAddress is the address the actor dialed as IP:port, from the fields
// of the ORIGINAL_DST filter state the CONNECT leg's answer set, or "" when
// no address rule allowed it and nothing was set.
func dialedAddress(md *extproc.RequestMetadata) string {
	ip := md.Attribute(extproc.OriginalDstIPAttribute)
	if ip == "" {
		return ""
	}
	return net.JoinHostPort(ip, md.Attribute(extproc.OriginalDstPortAttribute))
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

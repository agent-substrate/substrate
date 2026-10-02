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

	corev3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	envoy_type "github.com/envoyproxy/go-control-plane/envoy/type/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"

	"github.com/agent-substrate/substrate/cmd/atenet/internal/router/extproc"
	"github.com/agent-substrate/substrate/internal/egresspolicy"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/agent-substrate/substrate/pkg/proto/credproviderpb"
)

// The bodies of the denials a missing or unreachable credential provider
// causes. Unlike a policy denial these name their reason: the fault is the
// gateway's configuration, not anything the actor sent.
const (
	noProviderBody          = "egress denied: no credential provider configured"
	providerUnavailableBody = "egress denied: credential provider unavailable"
)

// mapCredentialProviderError converts a FetchSecret failure into a
// client-facing ext_proc denial, mirroring mapEgressIdentityError. reached
// reports whether the call got through to the provider at all.
//
// A provider the gateway could not reach — not deployed, a wrong address, a
// failed handshake — denies as 500 with providerUnavailableBody. Otherwise the
// provider's own answer decides: a credential it does not hold or will not
// release (NotFound, PermissionDenied) denies as 403, since retrying cannot
// succeed, while a transient failure (Unavailable, DeadlineExceeded) fails
// closed as a retryable 503. Anything unexpected denies rather than inviting
// retries of a request that cannot be completed as the policy promised.
func mapCredentialProviderError(err error, reached bool) error {
	if !reached {
		return extproc.WrapReqError(envoy_type.StatusCode_InternalServerError, err, providerUnavailableBody)
	}
	switch status.Code(err) {
	case codes.NotFound, codes.PermissionDenied:
		return extproc.WrapReqError(envoy_type.StatusCode_Forbidden, err, deniedBody)
	case codes.Unavailable, codes.DeadlineExceeded:
		return extproc.WrapReqError(envoy_type.StatusCode_ServiceUnavailable, err, deniedBody)
	default:
		return extproc.WrapReqError(envoy_type.StatusCode_Forbidden, err, deniedBody)
	}
}

// applyEffects resolves a matched rule's credential injections and returns the
// header mutations to add to the request, or an error that denies it. A rule
// with no injections adds nothing.
//
// A credential is only ever injected on the TLS-terminated MITM leg. On a
// cleartext request injection is skipped and the request is let through
// without the credential rather than denied, so a secret never rides a
// cleartext wire.
//
// On the MITM leg any failure to produce the credential the policy required
// fails closed, including a gateway with no credential provider configured.
//
// This gateway cannot mint actor JWTs yet, so on the MITM leg a rule that asks
// for one is denied. Actor JWTs don't come from the credential provider, so
// that holds with no provider configured.
func (h *Handler) applyEffects(ctx context.Context, ref resources.ActorRef, dest egresspolicy.Destination, leg string, effects *ateapipb.HttpRuleEffects) ([]*corev3.HeaderValueOption, error) {
	injections := effects.GetReplaceHeaders()
	if len(injections) == 0 {
		return nil, nil
	}

	if leg != extproc.EgressTLSMITMFilterChainName {
		slog.WarnContext(ctx, "egress: skipping credential injection on a non-TLS leg; the request proceeds without the credential",
			slog.Any("actor", ref), slog.String("host", dest.Hostname), slog.String("leg", leg))
		return nil, nil
	}
	// TODO(identity): mint actor JWTs through Control.MintActorJWT.
	for _, inj := range injections {
		if inj.GetActorJwt() != nil {
			slog.ErrorContext(ctx, "egress denied: this gateway cannot inject actor JWTs yet",
				slog.Any("actor", ref), slog.String("host", dest.Hostname), slog.String("header", inj.GetHeader()))
			return nil, extproc.NewReqError(envoy_type.StatusCode_NotImplemented, deniedBody)
		}
	}
	if h.provider == nil {
		slog.ErrorContext(ctx, "egress denied: the policy requires a credential but no credential provider is configured (--credential-provider-address)",
			slog.Any("actor", ref), slog.String("host", dest.Hostname))
		return nil, extproc.NewReqError(envoy_type.StatusCode_InternalServerError, noProviderBody)
	}

	// Atunnel connected to us with an ateom-for-actor SPIFFE ID; translate it
	// to a pure actor SPIFFE ID for plugins to make decisions on.
	actorSpiffeID := resources.ActorSPIFFEID(ref).String()

	setHeaders := make([]*corev3.HeaderValueOption, 0, len(injections))
	for _, inj := range injections {
		if err := validateInjectHeader(inj.GetHeader()); err != nil {
			slog.ErrorContext(ctx, "egress denied: policy names an unusable injection header",
				slog.Any("actor", ref), slog.String("host", dest.Hostname), slog.String("header", inj.GetHeader()), slog.Any("err", err))
			return nil, extproc.WrapReqError(envoy_type.StatusCode_InternalServerError, err, deniedBody)
		}

		// Confirm the credential URI names the provider this gateway serves
		// before dialing: the configured connection fronts one provider, so a URI
		// naming another cannot be resolved here and must fail closed rather than
		// be sent to the wrong provider.
		if h.providerName != "" {
			name, err := providerNameFromURI(inj.GetCredentialUri())
			if err != nil {
				slog.ErrorContext(ctx, "egress denied: policy names an unparseable credential URI",
					slog.Any("actor", ref), slog.String("host", dest.Hostname), slog.String("uri", inj.GetCredentialUri()), slog.Any("err", err))
				return nil, extproc.WrapReqError(envoy_type.StatusCode_InternalServerError, err, deniedBody)
			}
			if name != h.providerName {
				slog.ErrorContext(ctx, "egress denied: credential URI names a provider this gateway does not serve",
					slog.Any("actor", ref), slog.String("host", dest.Hostname), slog.String("uri", inj.GetCredentialUri()),
					slog.String("provider", name), slog.String("serves", h.providerName))
				return nil, extproc.NewReqError(envoy_type.StatusCode_InternalServerError, deniedBody)
			}
		}

		// gRPC fills in the peer only once the call has a stream to the provider,
		// so an empty one means the provider was never reached.
		var p peer.Peer
		resp, err := h.provider.FetchSecret(ctx, &credproviderpb.FetchSecretRequest{
			Uri:           inj.GetCredentialUri(),
			ActorSpiffeId: actorSpiffeID,
		}, grpc.Peer(&p))
		if err != nil {
			// Fail closed: a credential the policy required but we could not fetch
			// must not let the request out without it.
			reached := p.Addr != nil
			msg := "egress denied: credential fetch failed"
			if !reached {
				msg = "egress denied: credential provider unavailable"
			}
			slog.ErrorContext(ctx, msg,
				slog.Any("actor", ref), slog.String("host", dest.Hostname), slog.String("uri", inj.GetCredentialUri()), slog.Any("err", err))
			return nil, mapCredentialProviderError(err, reached)
		}
		secret, err := sanitizeSecret(resp.GetOpaqueBytes())
		if err != nil {
			slog.ErrorContext(ctx, "egress denied: unusable credential",
				slog.Any("actor", ref), slog.String("host", dest.Hostname), slog.String("uri", inj.GetCredentialUri()), slog.Any("err", err))
			return nil, extproc.WrapReqError(envoy_type.StatusCode_ServiceUnavailable, err, deniedBody)
		}

		// Overwrite any header the actor set itself, so a client cannot pre-seed a
		// value that survives injection.
		setHeaders = append(setHeaders, &corev3.HeaderValueOption{
			Header:       &corev3.HeaderValue{Key: inj.GetHeader(), RawValue: append([]byte(inj.GetPrefix()), secret...)},
			AppendAction: corev3.HeaderValueOption_OVERWRITE_IF_EXISTS_OR_ADD,
		})
	}
	return setHeaders, nil
}

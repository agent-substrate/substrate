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

// Package egressinject implements the ext_proc handler that runs on the egress
// gateway's decrypted MITM leg: it fetches the requesting actor's egress policy
// from the ateapi control plane, matches each outbound request against it, and
// on a match that carries a credential injection fetches the credential from the
// credential provider and injects it as a request header. Actor identity comes
// from the CA-signed client cert the gateway verified on the CONNECT leg,
// relayed here as the ate.actor.identity filter-state attribute — never from a
// request header.
package egressinject

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"strings"

	corev3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	extprocv3 "github.com/envoyproxy/go-control-plane/envoy/service/ext_proc/v3"
	envoy_type "github.com/envoyproxy/go-control-plane/envoy/type/v3"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/agent-substrate/substrate/cmd/atenet/internal/router/extproc"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/agent-substrate/substrate/pkg/proto/credproviderpb"
)

// actorIdentityAttribute is the CEL request attribute the MITM ext_proc filter
// forwards, carrying the actor's verified identity URI. It is the filter-state
// object ate.actor.identity the CONNECT leg set from the peer cert's URI SAN.
const actorIdentityAttribute = "filter_state['ate.actor.identity']"

// schemeHeader is the HTTP/2 pseudo-header carrying the request scheme. On the
// MITM leg the TLS chain yields "https" and the cleartext chain "http"; the
// injector refuses to inject a credential over anything but https.
const schemeHeader = ":scheme"

// Handler injects credentials into matched egress requests.
type Handler struct {
	apiClient policyClient
	provider  credproviderpb.CredentialProviderClient
	// providerClass is the credential-provider class this injector serves (e.g.
	// "kubernetes.io", parsed from the substrate-secret:// prefix). A credential
	// URI of any other class is refused. Empty disables the check (dev only).
	providerClass string
}

// New builds the injector handler. apiClient fetches an actor's egress policy
// from the ateapi control plane per request; providerClass is the credential
// provider class this injector serves, and a policy URI of another class fails
// closed.
func New(apiClient policyClient, provider credproviderpb.CredentialProviderClient, providerClass string) *Handler {
	return &Handler{apiClient: apiClient, provider: provider, providerClass: providerClass}
}

func (h *Handler) Direction() extproc.Direction { return extproc.DirectionEgressInject }

// HandleRequestHeaders fetches the actor's egress policy, matches the request
// against it, and on a matched credential injection fetches the credential and
// returns a header mutation adding it. A request with no matching injection passes through
// unchanged.
func (h *Handler) HandleRequestHeaders(ctx context.Context, md *extproc.RequestMetadata) (extproc.Result, error) {
	identity := md.Attribute(actorIdentityAttribute)
	host := hostFromAuthority(md.Host)

	atespace, actor, err := parseActorURI(identity)
	if err != nil {
		// No usable identity means no policy can be fetched, so there is nothing
		// to inject; pass the request through
		slog.WarnContext(ctx, "egress-inject: unusable actor identity", slog.String("host", host), slog.Any("err", err))
		return passThrough(host)
	}

	policy, err := h.apiClient.GetActorEgressPolicy(ctx, &ateapipb.GetActorEgressPolicyRequest{
		Actor: &ateapipb.ObjectRef{Atespace: atespace, Name: actor},
	})
	if err != nil {
		if status.Code(err) == codes.NotFound {
			// The actor has no egress policy: nothing to inject.
			slog.InfoContext(ctx, "egress-inject: actor has no egress policy",
				slog.String("atespace", atespace), slog.String("actor", actor), slog.String("host", host))
			return passThrough(host)
		}
		// Fail closed: we cannot tell whether a credential was required, so the
		// request must not go out potentially missing it.
		slog.ErrorContext(ctx, "egress-inject: fetching egress policy failed",
			slog.String("atespace", atespace), slog.String("actor", actor),
			slog.String("host", host), slog.Any("err", err))
		return extproc.Result{Target: host}, extproc.WrapReqError(envoy_type.StatusCode_ServiceUnavailable, err,
			"egress-inject: egress policy unavailable for %s", host)
	}

	injections, matched := evaluate(policy, host)
	if !matched {
		slog.InfoContext(ctx, "egress-inject: no matching policy rule",
			slog.String("atespace", atespace), slog.String("actor", actor), slog.String("host", host))
		return passThrough(host)
	}
	if len(injections) == 0 {
		// A rule matched but injects nothing: pass the request through unchanged.
		return passThrough(host)
	}

	// Refuse to inject a credential over cleartext: on the cleartext MITM chain
	// the request is re-originated without upstream TLS, so the secret would
	// leave the pod in the clear. Test scheme != "https" (not == "http") so a
	// missing or unknown scheme also fails closed. The egress-policy API has no
	// per-rule cleartext opt-in, so this refusal is unconditional.
	if scheme := strings.ToLower(md.Header(schemeHeader)); scheme != "https" {
		slog.WarnContext(ctx, "egress-inject: refusing to inject credential over cleartext",
			slog.String("atespace", atespace), slog.String("actor", actor),
			slog.String("host", host), slog.String("scheme", scheme))
		return extproc.Result{Target: host}, extproc.NewReqError(envoy_type.StatusCode_Forbidden,
			"egress-inject: refusing to inject a credential over cleartext to %s", host)
	}

	setHeaders := make([]*corev3.HeaderValueOption, 0, len(injections))
	for _, inj := range injections {
		if err := validateInjectHeader(inj.GetHeader()); err != nil {
			slog.ErrorContext(ctx, "egress-inject: policy names an unusable header",
				slog.String("atespace", atespace), slog.String("actor", actor),
				slog.String("host", host), slog.String("header", inj.GetHeader()), slog.Any("err", err))
			return extproc.Result{Target: host}, extproc.WrapReqError(envoy_type.StatusCode_InternalServerError, err,
				"egress-inject: policy for %s names an unusable header", host)
		}

		// Confirm the credential URI targets the provider class this injector
		// serves before dialing it: the configured provider fronts one class, so a
		// URI of another class cannot be resolved here and must fail closed rather
		// than be sent to the wrong provider.
		if h.providerClass != "" {
			class, err := credentialURIClass(inj.GetCredentialUri())
			if err != nil {
				slog.ErrorContext(ctx, "egress-inject: policy names an unparseable credential URI",
					slog.String("atespace", atespace), slog.String("actor", actor),
					slog.String("host", host), slog.String("uri", inj.GetCredentialUri()), slog.Any("err", err))
				return extproc.Result{Target: host}, extproc.WrapReqError(envoy_type.StatusCode_InternalServerError, err,
					"egress-inject: policy for %s names an unparseable credential URI", host)
			}
			if class != h.providerClass {
				slog.ErrorContext(ctx, "egress-inject: credential URI targets an unserved provider class",
					slog.String("atespace", atespace), slog.String("actor", actor),
					slog.String("host", host), slog.String("uri", inj.GetCredentialUri()),
					slog.String("class", class), slog.String("provider", h.providerClass))
				return extproc.Result{Target: host}, extproc.NewReqError(envoy_type.StatusCode_InternalServerError,
					"egress-inject: credential URI for %s targets provider class %q, this injector serves %q", host, class, h.providerClass)
			}
		}

		resp, err := h.provider.RequestSecret(ctx, &credproviderpb.RequestSecretRequest{
			Uri: inj.GetCredentialUri(),
			Context: &credproviderpb.SecretRequestContext{
				ActorIdentity: identity,
			},
		})
		if err != nil {
			// Fail closed: a credential we were told to inject but could not fetch
			// must not let the request out without it.
			slog.ErrorContext(ctx, "egress-inject: credential fetch failed",
				slog.String("atespace", atespace), slog.String("actor", actor),
				slog.String("host", host), slog.String("uri", inj.GetCredentialUri()), slog.Any("err", err))
			return extproc.Result{Target: host}, extproc.WrapReqError(envoy_type.StatusCode_ServiceUnavailable, err,
				"egress-inject: credential unavailable for %s", host)
		}

		secret, err := sanitizeSecret(resp.GetSecret())
		if err != nil {
			// Fail closed: an empty or malformed credential must not go upstream as
			// a bare "Bearer " nor as a header value Envoy would reject.
			slog.ErrorContext(ctx, "egress-inject: unusable credential",
				slog.String("atespace", atespace), slog.String("actor", actor),
				slog.String("host", host), slog.String("uri", inj.GetCredentialUri()), slog.Any("err", err))
			return extproc.Result{Target: host}, extproc.WrapReqError(envoy_type.StatusCode_ServiceUnavailable, err,
				"egress-inject: unusable credential for %s", host)
		}

		// Overwrite any header the actor set itself, so a client cannot pre-seed
		// a value that survives injection.
		setHeaders = append(setHeaders, &corev3.HeaderValueOption{
			Header:       &corev3.HeaderValue{Key: inj.GetHeader(), RawValue: append([]byte(inj.GetPrefix()), secret...)},
			AppendAction: corev3.HeaderValueOption_OVERWRITE_IF_EXISTS_OR_ADD,
		})
	}

	slog.InfoContext(ctx, "egress-inject: injecting credential(s)",
		slog.String("atespace", atespace), slog.String("actor", actor),
		slog.String("host", host), slog.Int("headers", len(setHeaders)))

	return extproc.Result{
		Target: host,
		Response: &extprocv3.HeadersResponse{
			Response: &extprocv3.CommonResponse{
				HeaderMutation: &extprocv3.HeaderMutation{SetHeaders: setHeaders},
			},
		},
	}, nil
}

// sanitizeSecret prepares resolved secret bytes for use as (part of) an HTTP
// header value. It trims a trailing newline — a Kubernetes Secret created from a
// file commonly carries one — then rejects an empty secret or one containing any
// control character, which Envoy would reject as an invalid header value and, in
// the case of CR/LF, would allow header injection.
func sanitizeSecret(secret []byte) ([]byte, error) {
	secret = bytes.TrimRight(secret, "\r\n")
	if len(secret) == 0 {
		return nil, fmt.Errorf("resolved credential is empty")
	}
	for _, b := range secret {
		if b < 0x20 || b == 0x7f {
			return nil, fmt.Errorf("resolved credential contains a control character")
		}
	}
	return secret, nil
}

// passThrough lets the request proceed unchanged, injecting nothing.
func passThrough(host string) (extproc.Result, error) {
	return extproc.Result{Target: host, Response: &extprocv3.HeadersResponse{Response: &extprocv3.CommonResponse{}}}, nil
}

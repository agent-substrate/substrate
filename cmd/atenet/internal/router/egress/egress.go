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

// Package egress implements the ext_proc handler for outbound actor traffic:
// it authenticates the actor behind an egress CONNECT before the gateway
// tunnels it out.
//
// The identity this handler acts on comes from the actor certificate presented
// in the mTLS handshake and signed by the actor-identity CA — never from a
// request header. That is the opposite of the ingress package's model, where
// every header is unauthenticated client input. Keeping the two in separate
// packages keeps that difference explicit; the ext_proc mux is what guarantees
// a request only ever reaches the handler for the filter chain that accepted
// it.
package egress

import (
	"context"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"log/slog"
	"net/url"
	"slices"
	"strings"
	"time"

	extprocv3 "github.com/envoyproxy/go-control-plane/envoy/service/ext_proc/v3"
	envoy_type "github.com/envoyproxy/go-control-plane/envoy/type/v3"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/agent-substrate/substrate/cmd/atenet/internal/router/extproc"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/internal/substratex509"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

const (
	// forwardedClientCertHeader is the header Envoy fills in with details of
	// the mTLS peer, including the PEM chain it validated. The egress filter
	// chain sets forward_client_cert_details: SANITIZE_SET, so whatever a
	// client sends under this name is discarded and replaced by Envoy's own
	// value.
	//
	// This is the only channel that can carry a whole certificate to ext_proc:
	// the CEL request attributes Envoy exposes (subject, SANs, SHA-256 digest)
	// cannot express the custom ActorIdentity X.509 extension this gateway
	// authorizes on.
	forwardedClientCertHeader = "x-forwarded-client-cert"
	// xfccChainKey is the x-forwarded-client-cert key holding the URL-encoded
	// PEM of the full presented chain, leaf first.
	xfccChainKey = "chain"
)

// Handler authenticates the actor behind each egress CONNECT.
type Handler struct {
	apiClient ateapipb.ControlClient
	// actorIdentityRoots is the actor-identity CA bundle every actor
	// certificate must chain to. Nil means the gateway cannot authenticate
	// anyone, and every CONNECT fails closed.
	actorIdentityRoots *x509.CertPool
}

// New builds the egress handler. actorIdentityRoots is the same trust bundle
// the egress listener uses as its trusted_ca; see verifyActorCertificate for
// why the check is made again here.
func New(apiClient ateapipb.ControlClient, actorIdentityRoots *x509.CertPool) *Handler {
	return &Handler{apiClient: apiClient, actorIdentityRoots: actorIdentityRoots}
}

func (h *Handler) Direction() extproc.Direction { return extproc.DirectionEgress }

// HandleRequestHeaders authenticates the actor behind an egress CONNECT before
// the gateway tunnels it out, using the actor certificate atunnel presented in
// the mTLS handshake. Nothing the actor can write — no CONNECT header, no
// request metadata — contributes to the identity; the only inputs are the
// certificate the actor-identity CA signed and the control plane's own view of
// that actor.
func (h *Handler) HandleRequestHeaders(ctx context.Context, md *extproc.RequestMetadata) (extproc.Result, error) {
	// Sanity check that we were called on the Egress listener filter chain with
	// a CONNECT.
	if !strings.EqualFold(md.Method, "CONNECT") {
		return extproc.Result{}, extproc.NewReqError(envoy_type.StatusCode_MethodNotAllowed,
			"egress denied: expected CONNECT, got %q", md.Method)
	}

	// No roots means the gateway cannot authenticate anyone. Fail closed, and
	// as 503 rather than 403: this is our misconfiguration, not the actor's.
	if h.actorIdentityRoots == nil {
		return extproc.Result{}, extproc.NewReqError(envoy_type.StatusCode_ServiceUnavailable,
			"egress unavailable: no actor-identity CA configured")
	}

	identity, err := h.authenticateActorCertificate(md)
	if err != nil {
		// The body stays generic on purpose: an actor that fails authentication
		// has not proven it is anyone, so it gets no detail about why. The
		// specific reason rides along as the wrapped cause, which only the
		// server-side log below reads.
		slog.WarnContext(ctx, "egress denied: actor certificate rejected", slog.Any("err", err))
		return extproc.Result{}, extproc.WrapReqError(envoy_type.StatusCode_Forbidden, err,
			"egress denied: invalid actor certificate")
	}

	if err := validateIdentity(identity); err != nil {
		return extproc.Result{}, err
	}
	if err := h.validateActor(ctx, identity); err != nil {
		return extproc.Result{}, err
	}

	slog.InfoContext(ctx, "egress identity authenticated",
		slog.String("atespace", identity.Atespace),
		slog.String("actor", identity.ActorName),
		slog.String("actorUid", identity.ActorUid),
		// For a CONNECT the :authority is the actor's original destination
		// (IP:port).
		slog.String("destination", md.Host))

	// Identity is authenticated; let the CONNECT proceed unchanged.
	return extproc.Result{
		Response: &extprocv3.HeadersResponse{
			Response: &extprocv3.CommonResponse{},
		},
	}, nil
}

// validateIdentity checks that the identity a verified actor certificate
// carries names an actor that could exist at all, before it is used as a
// control-plane lookup key.
func validateIdentity(identity *substratex509.ActorIdentity) error {
	// The CA only ever mints these from control-plane state, so a name that is
	// not a legal resource name means the CA or its inputs are compromised.
	if !resources.IsValidResourceName(identity.Atespace) || !resources.IsValidResourceName(identity.ActorName) {
		return extproc.NewReqError(envoy_type.StatusCode_Forbidden,
			"egress denied: invalid actor identity %q/%q", identity.Atespace, identity.ActorName)
	}
	return nil
}

// validateActor checks the identity a certificate certifies against the control
// plane's current view of that actor: it still exists, it is the actor the
// certificate was issued to, and it is running. Every error it returns is
// already a client-facing ext_proc denial.
func (h *Handler) validateActor(ctx context.Context, identity *substratex509.ActorIdentity) error {
	atespace := identity.Atespace
	actorName := identity.ActorName
	actorUID := identity.ActorUid

	// Confirm the certified actor still exists. The name is only a lookup key
	// here; the UID below is what actually authorizes.
	// TODO: this can cause heavy load on ate api server. Change it based on https://github.com/agent-substrate/substrate/issues/592.
	actor, err := h.apiClient.GetActor(ctx, &ateapipb.GetActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: atespace, Name: actorName},
	})
	if err != nil {
		return mapEgressIdentityError(atespace, actorName, err)
	}

	// Authorize on the UID, not the name. The UID the CA certified has to match the UID the
	// control plane holds right now.
	if uid := actor.GetMetadata().GetUid(); uid != actorUID {
		slog.WarnContext(ctx, "egress denied: actor UID mismatch",
			slog.String("atespace", atespace),
			slog.String("actor", actorName),
			slog.String("certificateActorUid", actorUID),
			slog.String("currentActorUid", uid))
		return extproc.NewReqError(envoy_type.StatusCode_Forbidden,
			"egress denied: actor %q/%q is not the actor this certificate was issued to", atespace, actorName)
	}

	// The actor performing egress must actually be running.
	if actor.GetStatus().GetState() != ateapipb.ActorState_ACTOR_STATE_RUNNING {
		return extproc.NewReqError(envoy_type.StatusCode_Forbidden,
			"egress denied: actor %q/%q is %s, not running", atespace, actorName, actor.GetStatus().GetState())
	}
	return nil
}

// authenticateActorCertificate turns the mTLS peer certificate Envoy recorded
// on the request into a verified ActorIdentity, or an error describing why it
// cannot be trusted.
func (h *Handler) authenticateActorCertificate(md *extproc.RequestMetadata) (*substratex509.ActorIdentity, error) {
	header := md.Header(forwardedClientCertHeader)
	if header == "" {
		return nil, fmt.Errorf("request carries no %s header", forwardedClientCertHeader)
	}
	chain, err := parseXFCCChain(header)
	if err != nil {
		return nil, err
	}
	return h.verifyActorCertificate(chain)
}

// verifyActorCertificate checks that chain[0] is a live, non-CA, client-auth
// actor certificate issued by the actor-identity CA, and returns the single
// ActorIdentity it carries.
//
// The chain is verified here even though Envoy already did it at the handshake
// (require_client_certificate with the actor-identity CA as trusted_ca). We have
// to parse the certificate anyway to read the ActorIdentity extension, which
// Envoy cannot see, and trusting a parsed-but-unverified certificate is a
// well-worn source of CVEs. It also keeps the handler safe if the Envoy config
// is ever loosened, and costs one signature check per CONNECT rather than per
// request. The IsCA, ClientAuth-EKU, and purpose checks below have no Envoy-side
// equivalent at all.
func (h *Handler) verifyActorCertificate(chain []*x509.Certificate) (*substratex509.ActorIdentity, error) {
	leaf := chain[0]
	intermediates := x509.NewCertPool()
	for _, cert := range chain[1:] {
		intermediates.AddCert(cert)
	}

	now := time.Now()
	if now.Before(leaf.NotBefore) || !now.Before(leaf.NotAfter) {
		return nil, fmt.Errorf("actor certificate is outside its validity period (%s..%s)",
			leaf.NotBefore.Format(time.RFC3339), leaf.NotAfter.Format(time.RFC3339))
	}
	// An actor certificate is an end-entity credential. Refusing IsCA here stops
	// a leaked or mis-issued CA certificate from being replayed as a leaf: chain
	// verification alone would happily accept one.
	if leaf.IsCA {
		return nil, fmt.Errorf("actor certificate is a CA certificate")
	}
	// Require ClientAuth explicitly rather than relying on VerifyOptions.KeyUsages:
	// an empty ExtKeyUsage means "any usage" to crypto/x509 and would pass. This
	// mirrors the check atunnel makes on the certificate when it mints it.
	if !slices.Contains(leaf.ExtKeyUsage, x509.ExtKeyUsageClientAuth) {
		return nil, fmt.Errorf("actor certificate cannot authenticate a TLS client")
	}
	if _, err := leaf.Verify(x509.VerifyOptions{
		Roots:         h.actorIdentityRoots,
		Intermediates: intermediates,
		CurrentTime:   now,
		KeyUsages:     []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}); err != nil {
		return nil, fmt.Errorf("actor certificate is not signed by the actor-identity CA: %w", err)
	}

	// ActorIdentityFromCertificate returns (nil, nil) when the extension is
	// absent, an error when there is more than one or when its contents are
	// malformed, empty, or carry a purpose other than atunnel.
	identity, err := substratex509.ActorIdentityFromCertificate(leaf)
	if err != nil {
		return nil, fmt.Errorf("actor certificate has no single valid ActorIdentity extension: %w", err)
	}
	if identity == nil {
		return nil, fmt.Errorf("actor certificate has no ActorIdentity extension")
	}
	// Restate what ActorIdentityFromCertificate enforces. The gateway is the
	// component that gets hurt if that helper ever loosens, and "reject anything
	// not scoped to atunnel" is the property this endpoint depends on: a
	// certificate minted for some future purpose must not open a tunnel.
	if identity.Atespace == "" || identity.ActorName == "" || identity.ActorUid == "" {
		return nil, fmt.Errorf("actor certificate identity is incomplete")
	}
	if identity.Purpose != substratex509.ActorIdentityPurposeAtunnel {
		return nil, fmt.Errorf("actor certificate purpose %q is not %q",
			identity.Purpose, substratex509.ActorIdentityPurposeAtunnel)
	}
	return identity, nil
}

// parseXFCCChain extracts the presented certificate chain, leaf first, from an
// x-forwarded-client-cert header value.
func parseXFCCChain(header string) ([]*x509.Certificate, error) {
	// One element per proxy hop. SANITIZE_SET makes Envoy the only writer, so
	// anything but exactly one element means either an unexpected proxy in front
	// of the gateway or a listener that lost SANITIZE_SET — in both cases we no
	// longer know which element describes our actual peer, so refuse to guess.
	elements := splitXFCCUnquoted(header, ',')
	if len(elements) != 1 {
		return nil, fmt.Errorf("expected exactly one %s element, got %d", forwardedClientCertHeader, len(elements))
	}
	encoded, ok := xfccValue(elements[0], xfccChainKey)
	if !ok {
		return nil, fmt.Errorf("%s carries no %q value", forwardedClientCertHeader, xfccChainKey)
	}
	// Envoy percent-encodes the PEM. PathUnescape, not QueryUnescape: base64
	// bodies contain '+', and query unescaping would decode it to a space and
	// silently corrupt the DER.
	chainPEM, err := url.PathUnescape(encoded)
	if err != nil {
		return nil, fmt.Errorf("decoding the client certificate chain: %w", err)
	}

	var chain []*x509.Certificate
	rest := []byte(chainPEM)
	for {
		var block *pem.Block
		block, rest = pem.Decode(rest)
		if block == nil {
			break
		}
		if block.Type != "CERTIFICATE" {
			continue
		}
		cert, err := x509.ParseCertificate(block.Bytes)
		if err != nil {
			return nil, fmt.Errorf("parsing the client certificate chain: %w", err)
		}
		chain = append(chain, cert)
	}
	if len(chain) == 0 {
		return nil, fmt.Errorf("%s carries no certificate", forwardedClientCertHeader)
	}
	return chain, nil
}

// xfccValue returns the value of key in one x-forwarded-client-cert element.
// Keys are matched case-insensitively; Envoy emits "Chain", but the header is
// consumed by enough different proxies that assuming its casing is not worth
// the failure mode.
func xfccValue(element, key string) (string, bool) {
	for _, pair := range splitXFCCUnquoted(element, ';') {
		k, v, found := strings.Cut(pair, "=")
		if !found || !strings.EqualFold(strings.TrimSpace(k), key) {
			continue
		}
		return unquoteXFCC(strings.TrimSpace(v)), true
	}
	return "", false
}

// splitXFCCUnquoted splits on sep, ignoring separators inside a quoted value.
// x-forwarded-client-cert quotes any value containing its own delimiters, which
// the PEM ones always do.
func splitXFCCUnquoted(s string, sep rune) []string {
	var parts []string
	var current strings.Builder
	quoted := false
	escaped := false
	for _, r := range s {
		switch {
		case escaped:
			current.WriteRune(r)
			escaped = false
		case quoted && r == '\\':
			current.WriteRune(r)
			escaped = true
		case r == '"':
			quoted = !quoted
			current.WriteRune(r)
		case r == sep && !quoted:
			parts = append(parts, current.String())
			current.Reset()
		default:
			current.WriteRune(r)
		}
	}
	parts = append(parts, current.String())

	trimmed := make([]string, 0, len(parts))
	for _, part := range parts {
		if part = strings.TrimSpace(part); part != "" {
			trimmed = append(trimmed, part)
		}
	}
	return trimmed
}

// unquoteXFCC strips the surrounding quotes from an x-forwarded-client-cert
// value and undoes the backslash escaping inside them.
func unquoteXFCC(value string) string {
	if len(value) < 2 || !strings.HasPrefix(value, `"`) || !strings.HasSuffix(value, `"`) {
		return value
	}
	inner := value[1 : len(value)-1]
	var out strings.Builder
	escaped := false
	for _, r := range inner {
		if escaped {
			out.WriteRune(r)
			escaped = false
			continue
		}
		if r == '\\' {
			escaped = true
			continue
		}
		out.WriteRune(r)
	}
	return out.String()
}

// mapEgressIdentityError converts a GetActor failure into a client-facing
// ext_proc denial. An unknown actor is treated as forbidden (the actor was
// deleted out from under a still-valid certificate); transient control-plane
// failures fail closed with 503.
func mapEgressIdentityError(atespace, actorName string, err error) error {
	switch status.Code(err) {
	case codes.NotFound:
		return extproc.WrapReqError(envoy_type.StatusCode_Forbidden, err,
			"egress denied: unknown actor %q/%q", atespace, actorName)
	case codes.Unavailable, codes.DeadlineExceeded:
		return extproc.WrapReqError(envoy_type.StatusCode_ServiceUnavailable, err,
			"egress identity check unavailable for %q/%q: %v", atespace, actorName, err)
	default:
		return extproc.WrapReqError(envoy_type.StatusCode_Forbidden, err,
			"egress denied for %q/%q: %v", atespace, actorName, err)
	}
}

// LoadActorIdentityRoots reads the actor-identity CA trust bundle the egress
// gateway verifies actor client certificates against.
func LoadActorIdentityRoots(pemBytes []byte) (*x509.CertPool, error) {
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(pemBytes) {
		return nil, fmt.Errorf("actor-identity CA bundle contains no certificates")
	}
	return roots, nil
}

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

package router

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

	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/internal/substratex509"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

const (
	// EgressFilterChainName is the Envoy filter chain that terminates actor
	// egress CONNECTs. It must stay in sync with the filter chain name in
	// manifests/ate-install/atenet-egress.yaml.
	EgressFilterChainName = "egress"
	// FilterChainNameAttribute is the CEL attribute carrying the name of the
	// filter chain that accepted the request. The egress Envoy asks for it via
	// request_attributes on its ext_proc filter.
	//
	// SEE(lior): this was xds.listener_name, which reads more naturally but
	// which Envoy 1.34 cannot parse: it logs "error parsing cel expression
	// xds.listener_name" at trace level, then sends the ProcessingRequest with
	// an empty attributes map rather than failing config load. Because an
	// absent attribute means "ingress" (the fail-safe direction), every egress
	// CONNECT silently took the ingress path and 404'd on the actor DNS name
	// parse. xds.filter_chain_name parses on the same Envoy build and is
	// equally Envoy-asserted, so the trust model is unchanged.
	FilterChainNameAttribute = "xds.filter_chain_name"

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

// isEgressRequest reports whether an ext_proc RequestHeaders callback arrived on
// the egress gateway's filter chain rather than on an ingress one. This lets one
// ext_proc server handle both directions off the same stream.
//
// Dispatch is by filter chain, not by :method, because the two handlers apply
// opposite trust models: on egress the actor identity comes from a client
// certificate Envoy validated, while on ingress every request header is
// unauthenticated client input. Keying on :method would let any external client
// sending CONNECT select the egress handler and use its denial messages as an
// actor-existence and status oracle. Envoy asserts the filter chain name; the
// request cannot influence it.
//
// An unrecognized or absent attribute means ingress, the fail-safe direction: an
// egress request misrouted to the ingress handler fails to parse as an actor DNS
// name and 404s, whereas the reverse leaks control-plane state.
func isEgressRequest(req *extprocv3.ProcessingRequest) bool {
	return filterChainName(req) == EgressFilterChainName
}

// filterChainName returns the xds.filter_chain_name attribute Envoy attached to
// the request, or "" when the listener did not request the attribute. The
// attributes map is keyed by the ext_proc filter's name within the HCM chain,
// which we do not want to hardcode here, so scan every entry.
func filterChainName(req *extprocv3.ProcessingRequest) string {
	for _, attrs := range req.GetAttributes() {
		if v, ok := attrs.GetFields()[FilterChainNameAttribute]; ok {
			return v.GetStringValue()
		}
	}
	return ""
}

// handleEgressRequestHeaders authenticates the actor behind an egress CONNECT
// before the gateway tunnels it out, using the actor certificate atunnel
// presented in the mTLS handshake. Nothing the actor can write — no CONNECT
// header, no request metadata — contributes to the identity; the only inputs
// are the certificate the actor-identity CA signed and the control plane's own
// view of that actor.
//
// Authorization by destination and credential/token injection are deliberately
// a TODO once we have SessionIdentity RPC service figured out.
//
// The signature mirrors handleRequestHeaders so Process can dispatch to either
// with a single branch. The (target, tmplNs, tmplName) results are unused for
// egress and returned empty.
//
// The trailing ResumeOutcome exists only to match handleRequestHeaders.
// Egress never resumes an actor — it requires one already RUNNING -
// so every path returns ResumeOutcomeNone.
func (s *ExtProcServer) handleEgressRequestHeaders(
	ctx context.Context,
	reqHeaders *extprocv3.HttpHeaders,
) (*extprocv3.HeadersResponse, *requestMetadata, string, string, string, ResumeOutcome, error) {
	metadata := newRequestMetadata(reqHeaders.Headers.GetHeaders())

	// Dispatch is by filter chain, so reaching here means the egress chain
	// accepted the request. That chain only routes CONNECT (its sole route is a
	// connect_matcher), so anything else is config drift rather than a client
	// the gateway should tunnel for.
	if !strings.EqualFold(metadata.method, "CONNECT") {
		return nil, metadata, "", "", "", ResumeOutcomeNone, newReqError(envoy_type.StatusCode_MethodNotAllowed,
			"egress denied: expected CONNECT, got %q", metadata.method)
	}

	// No roots means the gateway cannot authenticate anyone. Fail closed, and
	// as 503 rather than 403: this is our misconfiguration, not the actor's.
	if s.actorIdentityRoots == nil {
		return nil, metadata, "", "", "", ResumeOutcomeNone, newReqError(envoy_type.StatusCode_ServiceUnavailable,
			"egress unavailable: no actor-identity CA configured")
	}

	identity, err := s.authenticateActorCertificate(metadata)
	if err != nil {
		// The message stays generic on purpose: an actor that fails
		// authentication has not proven it is anyone, so it gets no detail
		// about why. The specific reason is logged below instead.
		slog.WarnContext(ctx, "egress denied: actor certificate rejected", slog.Any("err", err))
		return nil, metadata, "", "", "", ResumeOutcomeNone, newReqError(envoy_type.StatusCode_Forbidden,
			"egress denied: invalid actor certificate")
	}

	atespace := identity.Atespace
	actorName := identity.ActorName
	actorUID := identity.ActorUid
	// For a CONNECT the :authority is the actor's original destination (IP:port).
	destination := metadata.host

	// The CA only ever mints these from control-plane state, so a name that is
	// not a legal resource name means the CA or its inputs are compromised.
	if !resources.IsValidResourceName(atespace) || !resources.IsValidResourceName(actorName) {
		return nil, metadata, "", "", "", ResumeOutcomeNone, newReqError(envoy_type.StatusCode_Forbidden,
			"egress denied: invalid actor identity %q/%q", atespace, actorName)
	}

	// Confirm the certified actor still exists. The name is only a lookup key
	// here; the UID below is what actually authorizes.
	actor, err := s.apiClient.GetActor(ctx, &ateapipb.GetActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: atespace, Name: actorName},
	})
	if err != nil {
		return nil, metadata, "", "", "", ResumeOutcomeNone, mapEgressIdentityError(atespace, actorName, err)
	}

	// Authorize on the UID, not the name. Names are reused: delete an actor and
	// recreate it under the same atespace/name and it is a different actor with
	// a different UID. A certificate outliving its actor must not carry over to
	// the successor, so the UID the CA certified has to match the UID the
	// control plane holds right now.
	if uid := actor.GetMetadata().GetUid(); uid != actorUID {
		slog.WarnContext(ctx, "egress denied: actor UID mismatch",
			slog.String("atespace", atespace),
			slog.String("actor", actorName),
			slog.String("certificateActorUid", actorUID),
			slog.String("currentActorUid", uid))
		return nil, metadata, "", "", "", ResumeOutcomeNone, newReqError(envoy_type.StatusCode_Forbidden,
			"egress denied: actor %q/%q is not the actor this certificate was issued to", atespace, actorName)
	}

	// The actor performing egress must actually be running.
	if actor.GetStatus() != ateapipb.Actor_STATUS_RUNNING {
		return nil, metadata, "", "", "", ResumeOutcomeNone, newReqError(envoy_type.StatusCode_Forbidden,
			"egress denied: actor %q/%q is %s, not running", atespace, actorName, actor.GetStatus())
	}

	slog.InfoContext(ctx, "egress identity authenticated",
		slog.String("atespace", atespace),
		slog.String("actor", actorName),
		slog.String("actorUid", actorUID),
		slog.String("destination", destination),
		slog.String("status", actor.GetStatus().String()))

	// Identity is authenticated; let the CONNECT proceed unchanged. Milestone 2
	// would additionally authorize `destination` and inject upstream credentials
	// here by returning a HeaderMutation.
	return &extprocv3.HeadersResponse{
		Response: &extprocv3.CommonResponse{},
	}, metadata, "", "", "", ResumeOutcomeNone, nil
}

// authenticateActorCertificate turns the mTLS peer certificate Envoy recorded
// on the request into a verified ActorIdentity, or an error describing why it
// cannot be trusted.
func (s *ExtProcServer) authenticateActorCertificate(metadata *requestMetadata) (*substratex509.ActorIdentity, error) {
	header := metadata.headers[forwardedClientCertHeader]
	if header == "" {
		return nil, fmt.Errorf("request carries no %s header", forwardedClientCertHeader)
	}
	chain, err := parseXFCCChain(header)
	if err != nil {
		return nil, err
	}
	return s.verifyActorCertificate(chain)
}

// verifyActorCertificate checks that chain[0] is a live, non-CA, client-auth
// actor certificate issued by the actor-identity CA, and returns the single
// ActorIdentity it carries.
//
// ###########################################################################
// SEE(lior): READ THIS IF YOU ARE WONDERING WHY WE VERIFY THE CHAIN TWICE.
//
// The egress listener already does full mTLS: require_client_certificate with
// the actor-identity CA as its trusted_ca, so Envoy refuses the handshake for
// anything this function would also reject on chain, expiry, or signature. The
// re-verification below is therefore redundant *today*, and it is here on
// purpose:
//
//   - Envoy validates the chain but cannot look at the ActorIdentity
//     extension. This function has to parse the certificate regardless, and
//     parsing an unverified certificate and then trusting its contents is the
//     failure mode that keeps producing CVEs. Verifying what we parse keeps the
//     trust decision in one place instead of split across a YAML file and a Go
//     file.
//   - It makes the handler safe under Envoy config drift. Someone relaxing
//     require_client_certificate, widening trusted_ca, or putting another proxy
//     in front should not silently turn this into an unauthenticated endpoint.
//   - It costs one signature verification per CONNECT, not per request: the
//     tunnel is established once and then carries raw TCP.
//
// If you decide the Envoy-side check is authoritative and this is dead weight,
// this function is the thing to delete — but keep the ActorIdentity extraction
// and the IsCA/EKU/purpose checks below it, because Envoy does none of those.
// ###########################################################################
func (s *ExtProcServer) verifyActorCertificate(chain []*x509.Certificate) (*substratex509.ActorIdentity, error) {
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
		Roots:         s.actorIdentityRoots,
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
// ext_proc denial. An unknown actor is treated as a forbidden (the actor was
// deleted out from under a still-valid certificate); transient control-plane
// failures fail closed with 503.
func mapEgressIdentityError(atespace, actorName string, err error) error {
	switch status.Code(err) {
	case codes.NotFound:
		return newReqError(envoy_type.StatusCode_Forbidden,
			"egress denied: unknown actor %q/%q", atespace, actorName)
	case codes.Unavailable, codes.DeadlineExceeded:
		return newReqError(envoy_type.StatusCode_ServiceUnavailable,
			"egress identity check unavailable for %q/%q: %v", atespace, actorName, err)
	default:
		return newReqError(envoy_type.StatusCode_Forbidden,
			"egress denied for %q/%q: %v", atespace, actorName, err)
	}
}

// loadActorIdentityRoots reads the actor-identity CA trust bundle the egress
// gateway verifies actor client certificates against.
func loadActorIdentityRoots(pemBytes []byte) (*x509.CertPool, error) {
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(pemBytes) {
		return nil, fmt.Errorf("actor-identity CA bundle contains no certificates")
	}
	return roots, nil
}

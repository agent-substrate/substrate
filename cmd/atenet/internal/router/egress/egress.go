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

// Package egress implements the ext_proc handler for outbound actor traffic.
// It authenticates the actor behind an egress CONNECT and authorizes what goes
// through the tunnel against the actor's EgressPolicy. A request the gateway
// can read is decided the way the API says: the rules in order, over the Host
// it named and the address the actor dialed, first match wins. What the
// gateway cannot read is decided at the CONNECT, by the address alone.
//
// Identity comes from the actor certificate presented in the mTLS handshake,
// never from a request header. On the inner legs it arrives as filter state
// Envoy derived from that certificate, which nothing inside the tunnel can
// write. The filter chain name tells the handler which leg it is on.
package egress

import (
	"context"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/url"
	"strconv"
	"strings"
	"time"

	extprocv3 "github.com/envoyproxy/go-control-plane/envoy/service/ext_proc/v3"
	envoy_type "github.com/envoyproxy/go-control-plane/envoy/type/v3"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/agent-substrate/substrate/cmd/atenet/internal/router/extproc"
	"github.com/agent-substrate/substrate/internal/egresspolicy"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/agent-substrate/substrate/pkg/proto/credproviderpb"
)

const (
	// agentgatewayClientCertificateAttribute is the PEM peer certificate agentgateway
	// computes from the downstream TLS connection for ext_proc.
	agentgatewayClientCertificateAttribute = "source.certificate"
	// forwardedClientCertHeader is the header Envoy fills in with details of
	// the mTLS peer, including the PEM chain it validated. The egress filter
	// chain sets forward_client_cert_details: SANITIZE_SET, so whatever a
	// client sends under this name is discarded and replaced by Envoy's own
	// value.
	//
	// This is the only channel that can carry a whole certificate to ext_proc
	// so the gateway can verify the chain, key usages, and ateom SPIFFE URI.
	//
	// TODO(identity): Audit that this cannot be stomped by a header sent by the
	// actor.
	forwardedClientCertHeader = "x-forwarded-client-cert"
	// xfccChainKey is the x-forwarded-client-cert key holding the URL-encoded
	// PEM of the full presented chain, leaf first.
	xfccChainKey = "chain"
)

// deniedBody is the body of every policy denial. The reason goes to the log,
// not to the actor.
const deniedBody = "egress denied"

// Handler authenticates the actor behind each egress CONNECT and authorizes
// the traffic inside the tunnel against the actor's EgressPolicy.
type Handler struct {
	apiClient ateapipb.ControlClient
	// actorIdentityRoots is the actor-identity CA bundle every actor
	// certificate must chain to. Nil means the gateway cannot authenticate
	// anyone, and every CONNECT fails closed.
	actorIdentityRoots *x509.CertPool
	// policies is the per-actor EgressPolicy cache every leg reads through.
	policies *policyCache
	// provider resolves an egress policy's credential injections. Nil means
	// credential injection is not configured, and injection will be skipped.
	provider credproviderpb.CredentialProviderClient
	// providerName, when set, is the provider this gateway serves (the host of
	// its ate-secret:// prefix); a credential URI naming another provider
	// is refused.
	providerName string
}

// New builds the egress handler. actorIdentityRoots is the egress listener's
// trusted_ca; see verifyActorCertificate for why it is checked again here.
// policyCacheTTL of 0 fetches the policy on every callout.
//
// provider resolves an allowed rule's credential injections on the
// TLS-terminated MITM leg; nil leaves credential injection off, so a rule that
// requires an injection is skipped. providerName, when set, is the provider
// this gateway serves; a credential URI naming another provider is refused.
func New(apiClient ateapipb.ControlClient, actorIdentityRoots *x509.CertPool, policyCacheTTL time.Duration, provider credproviderpb.CredentialProviderClient, providerName string) *Handler {
	return &Handler{
		apiClient:          apiClient,
		actorIdentityRoots: actorIdentityRoots,
		policies:           newPolicyCache(apiClient, policyCacheTTL),
		provider:           provider,
		providerName:       providerName,
	}
}

func (h *Handler) Direction() extproc.Direction { return extproc.DirectionEgress }

// HandleRequestHeaders dispatches on the filter chain the request arrived on.
// An empty chain name is a non-Envoy dataplane, which calls out for the
// CONNECT alone. Anything unrecognized is refused, not guessed.
func (h *Handler) HandleRequestHeaders(ctx context.Context, md *extproc.RequestMetadata) (extproc.Result, error) {
	switch leg := md.Attribute(extproc.FilterChainNameAttribute); leg {
	case extproc.EgressFilterChainName, "":
		return h.handleConnect(ctx, md, leg)
	case extproc.EgressTLSMITMFilterChainName, extproc.EgressCleartextFilterChainName:
		return h.handleRequest(ctx, md, leg)
	default:
		return extproc.Result{}, extproc.NewReqError(envoy_type.StatusCode_NotFound,
			"egress denied: this gateway does not serve filter chain %q", leg)
	}
}

// handleConnect authenticates the actor behind an egress CONNECT from the
// certificate atunnel presented, and decides the policy's address rules
// against the original destination. Nothing the actor can write contributes
// to the identity.
//
// Three outcomes: an address rule allows the destination, so the tunnel opens
// and the destination goes back as dynamic metadata for the passthrough chain
// to dial; no address rule allows it but the policy has hostname rules, so the
// tunnel opens with nothing to dial and only a request a name rule allows can
// go through; neither, so the CONNECT is refused here, where there is still a
// response.
func (h *Handler) handleConnect(ctx context.Context, md *extproc.RequestMetadata, leg string) (extproc.Result, error) {
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

	actorRef, err := h.authenticateActorCertificate(md)
	if err != nil {
		// The body stays generic on purpose: an actor that fails authentication
		// has not proven it is anyone, so it gets no detail about why. The
		// specific reason rides along as the wrapped cause, which only the
		// server-side log below reads.
		slog.WarnContext(ctx, "egress denied: actor certificate rejected", slog.Any("err", err))
		return extproc.Result{}, extproc.WrapReqError(envoy_type.StatusCode_Forbidden, err,
			"egress denied: invalid actor certificate")
	}

	if err := h.validateActor(ctx, actorRef); err != nil {
		return extproc.Result{}, err
	}

	ref := resources.ActorRef{Atespace: actorRef.Atespace, Name: actorRef.Name}

	// atunnel always sends the address the actor's kernel dialed, never a
	// name. Refuse a name here, where there is still a response to do it with.
	dest, err := egresspolicy.NormalizeAuthority(md.Host)
	if err != nil || !dest.IP.IsValid() || dest.Port == 0 {
		slog.WarnContext(ctx, "egress denied: CONNECT authority is not an IP:port", slog.Any("actor", ref), slog.String("leg", leg), slog.String("authority", md.Host), slog.Any("err", err))
		return extproc.Result{}, extproc.NewReqError(envoy_type.StatusCode_Forbidden, deniedBody)
	}

	// This also warms the cache for the requests inside the tunnel. dest has
	// no hostname, so only cidrs and all rules can match here.
	policy, err := h.lookupPolicy(ctx, leg, ref)
	if err != nil {
		return extproc.Result{}, err
	}
	decision := policy.Evaluate(dest)
	attrs := []any{
		slog.Any("actor", ref),
		slog.String("leg", leg),
		slog.String("destination", md.Host),
		slog.Int("rule", decision.RuleIndex),
	}
	switch {
	case decision.Allowed:
		slog.InfoContext(ctx, "egress tunnel opened: an address rule allows the destination", attrs...)
		res := allow()
		res.DynamicMetadata = passthroughDestination(dest)
		return res, nil
	case leg == extproc.EgressFilterChainName && policy.HasHostnameRules():
		// Only the Envoy gateway has request legs behind this one. A dataplane
		// that calls out for the CONNECT alone sends no chain name and is
		// refused below.
		slog.InfoContext(ctx, "egress tunnel opened: no address rule allows the destination, requests inside it are decided one by one", attrs...)
		return allow(), nil
	default:
		slog.WarnContext(ctx, "egress denied: no rule allows the destination", attrs...)
		return extproc.Result{}, extproc.NewReqError(envoy_type.StatusCode_Forbidden, deniedBody)
	}
}

// passthroughDestination is the dynamic metadata naming the one address the
// passthrough chain may dial, as IP:port.
func passthroughDestination(dest egresspolicy.Destination) *structpb.Struct {
	return metadataAnswer(extproc.EgressPassthroughDestinationKey, net.JoinHostPort(dest.IP.String(), strconv.Itoa(int(dest.Port))))
}

// metadataAnswer is a one-entry answer in the egress metadata namespace.
func metadataAnswer(key, value string) *structpb.Struct {
	return &structpb.Struct{Fields: map[string]*structpb.Value{
		extproc.EgressMetadataNamespace: structpb.NewStructValue(&structpb.Struct{Fields: map[string]*structpb.Value{
			key: structpb.NewStringValue(value),
		}}),
	}}
}

// allow is the response for a request the handler lets through unchanged.
func allow() extproc.Result {
	return extproc.Result{
		Response: &extprocv3.HeadersResponse{
			Response: &extprocv3.CommonResponse{},
		},
	}
}

// lookupPolicy is the check every leg starts with. Every error it returns is
// already a client-facing denial.
func (h *Handler) lookupPolicy(ctx context.Context, leg string, ref resources.ActorRef) (*egresspolicy.Policy, error) {
	policy, err := h.policies.get(ctx, ref)
	switch {
	case ctx.Err() != nil && errors.Is(err, ctx.Err()):
		// The caller gave up mid-fetch: not a decision.
		slog.DebugContext(ctx, "egress policy lookup abandoned by the caller", slog.Any("actor", ref), slog.String("leg", leg))
		return nil, extproc.WrapReqError(envoy_type.StatusCode_RequestTimeout, err, "egress request canceled")
	case errors.Is(err, errNoPolicy):
		slog.WarnContext(ctx, "egress denied: actor has no egress policy", slog.Any("actor", ref), slog.String("leg", leg))
		return nil, extproc.WrapReqError(envoy_type.StatusCode_Forbidden, err, deniedBody)
	case err != nil:
		// The control plane failed, not the actor: 503, and nothing is cached.
		slog.ErrorContext(ctx, "egress policy lookup failed", slog.Any("actor", ref), slog.String("leg", leg), slog.Any("err", err))
		return nil, extproc.WrapReqError(envoy_type.StatusCode_ServiceUnavailable, err, "egress unavailable: policy lookup failed")
	case policy.RuleCount() == 0:
		// Can authorize nothing, so the same posture as having no policy.
		slog.WarnContext(ctx, "egress denied: actor's egress policy has no rules", slog.Any("actor", ref), slog.String("leg", leg))
		return nil, extproc.NewReqError(envoy_type.StatusCode_Forbidden, deniedBody)
	}
	return policy, nil
}

// validateActor checks the actor certified by the certificate against the control
// plane's current view of that actor: it still exists and it is running. Every
// error it returns is already a client-facing ext_proc denial.
func (h *Handler) validateActor(ctx context.Context, actorRef resources.ActorRef) error {
	// Confirm the certified actor still exists.
	// TODO: this can cause heavy load on ate api server. Change it based on https://github.com/agent-substrate/substrate/issues/592.
	actor, err := h.apiClient.GetActor(ctx, &ateapipb.GetActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: actorRef.Atespace, Name: actorRef.Name},
	})
	if err != nil {
		return mapEgressIdentityError(actorRef.Atespace, actorRef.Name, err)
	}

	// The actor performing egress must actually be running.
	if actor.GetStatus().GetState() != ateapipb.ActorState_ACTOR_STATE_RUNNING {
		return extproc.NewReqError(envoy_type.StatusCode_Forbidden,
			"egress denied: actor %q/%q is %s, not running", actorRef.Atespace, actorRef.Name, actor.GetStatus().GetState())
	}
	return nil
}

// authenticateActorCertificate turns the mTLS peer certificate Envoy recorded
// on the request into a verified ActorRef, or an error describing why it
// cannot be trusted.
func (h *Handler) authenticateActorCertificate(md *extproc.RequestMetadata) (resources.ActorRef, error) {
	if certificate := md.Attribute(agentgatewayClientCertificateAttribute); certificate != "" {
		chain, err := parseCertificateChainPEM([]byte(certificate))
		if err != nil {
			return resources.ActorRef{}, err
		}
		return h.verifyActorCertificate(chain)
	}
	header := md.Header(forwardedClientCertHeader)
	if header == "" {
		return resources.ActorRef{}, fmt.Errorf("request carries no %s header", forwardedClientCertHeader)
	}
	chain, err := parseXFCCChain(header)
	if err != nil {
		return resources.ActorRef{}, err
	}
	return h.verifyActorCertificate(chain)
}

// verifyActorCertificate checks that chain[0] is a live, non-CA, client-auth
// ateom actor certificate issued by the actor-identity CA, and returns the
// ActorRef from its SPIFFE URI.
//
// The chain is verified here even though Envoy already did it at the handshake
// (require_client_certificate with the actor-identity CA as trusted_ca). We have
// to parse the certificate anyway to inspect its SPIFFE URI and usages, and
// trusting a parsed-but-unverified certificate is a well-worn source of CVEs.
// It also keeps the handler safe if the Envoy config is ever loosened, and costs
// one signature check per CONNECT rather than per request. The IsCA, ClientAuth-EKU,
// and ateom SPIFFE URI checks below have no Envoy-side equivalent at all.
func (h *Handler) verifyActorCertificate(chain []*x509.Certificate) (resources.ActorRef, error) {
	leaf := chain[0]
	intermediates := x509.NewCertPool()
	for _, cert := range chain[1:] {
		intermediates.AddCert(cert)
	}

	now := time.Now()
	if now.Before(leaf.NotBefore) || !now.Before(leaf.NotAfter) {
		return resources.ActorRef{}, fmt.Errorf("actor certificate is outside its validity period (%s..%s)",
			leaf.NotBefore.Format(time.RFC3339), leaf.NotAfter.Format(time.RFC3339))
	}
	// An actor certificate is an end-entity credential. Refusing IsCA here stops
	// a leaked or mis-issued CA certificate from being replayed as a leaf: chain
	// verification alone would happily accept one.
	if leaf.IsCA {
		return resources.ActorRef{}, fmt.Errorf("actor certificate is a CA certificate")
	}

	if _, err := leaf.Verify(x509.VerifyOptions{
		Roots:         h.actorIdentityRoots,
		Intermediates: intermediates,
		CurrentTime:   now,
		KeyUsages:     []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}); err != nil {
		return resources.ActorRef{}, fmt.Errorf("actor certificate is not signed by the actor-identity CA: %w", err)
	}

	// Check that this is an ateom certificate --- the SPIFFE URI should be of
	// the form `spiffe://${trustdomain}/ateom-for-actor/${atespace}/${actor}`.
	if len(leaf.URIs) != 1 {
		return resources.ActorRef{}, fmt.Errorf("actor certificate has %d URI SANs, want 1", len(leaf.URIs))
	}
	ref, err := resources.ActorRefFromAteomForActorSPIFFEURL(leaf.URIs[0])
	if err != nil {
		return resources.ActorRef{}, fmt.Errorf("while parsing actor from SPIFFE ID: %w", err)
	}
	return ref, nil
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

	return parseCertificateChainPEM([]byte(chainPEM))
}

func parseCertificateChainPEM(chainPEM []byte) ([]*x509.Certificate, error) {
	var chain []*x509.Certificate
	rest := chainPEM
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
		return nil, fmt.Errorf("client certificate value carries no certificate")
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

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
	"slices"
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
	"github.com/agent-substrate/substrate/internal/substratex509"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/agent-substrate/substrate/pkg/proto/credproviderpb"
)

// PeerCertificateSource identifies which trusted dataplane-produced field
// carries the actor certificate for this handler. It is selected at handler
// construction from the router's deployment configuration.
type PeerCertificateSource string

const (
	PeerCertificateSourceEnvoy        PeerCertificateSource = "envoy"
	PeerCertificateSourceAgentgateway PeerCertificateSource = "agentgateway"

	// Keep the package-local names used by the egress tests while the contract
	// itself lives with the request attributes.
	agentgatewayClientCertificateAttribute  = extproc.AgentgatewayClientCertificateAttribute
	envoyClientCertificateMetadataNamespace = extproc.EgressPeerCertificateMetadataNamespace
	envoyClientCertificateChainKey          = extproc.EgressPeerCertificateChainKey
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
	// peerCertificateSource is selected from the trusted router configuration;
	// request field presence never selects the certificate source.
	peerCertificateSource PeerCertificateSource
}

// New builds the egress handler. actorIdentityRoots is the egress listener's
// trusted_ca; see verifyActorCertificate for why it is checked again here.
// policyCacheTTL of 0 fetches the policy on every callout.
//
// provider resolves an allowed rule's credential injections on the
// TLS-terminated MITM leg; nil leaves credential injection off, so a rule that
// requires an injection is skipped. providerName, when set, is the provider
// this gateway serves; a credential URI naming another provider is refused.
func New(apiClient ateapipb.ControlClient, actorIdentityRoots *x509.CertPool, policyCacheTTL time.Duration, provider credproviderpb.CredentialProviderClient, providerName string, peerCertificateSource PeerCertificateSource) *Handler {
	return &Handler{
		apiClient:             apiClient,
		actorIdentityRoots:    actorIdentityRoots,
		policies:              newPolicyCache(apiClient, policyCacheTTL),
		provider:              provider,
		providerName:          providerName,
		peerCertificateSource: peerCertificateSource,
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

	ref := resources.ActorRef{Atespace: identity.Atespace, Name: identity.ActorName}

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
		slog.String("actorUid", identity.ActorUid),
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

// authenticateActorCertificate turns the configured dataplane's trusted peer
// certificate evidence into a verified ActorIdentity, or an error describing
// why it cannot be trusted.
func (h *Handler) authenticateActorCertificate(md *extproc.RequestMetadata) (*substratex509.ActorIdentity, error) {
	switch h.peerCertificateSource {
	case PeerCertificateSourceEnvoy:
		peer, present := md.DynamicMetadata[extproc.EgressPeerCertificateMetadataNamespace]
		if !present || peer == nil {
			return nil, fmt.Errorf("request carries no trusted Envoy peer certificate metadata")
		}
		value, ok := peer.GetFields()[extproc.EgressPeerCertificateChainKey]
		if !ok || value == nil {
			return nil, fmt.Errorf("trusted Envoy peer certificate metadata carries no %q field", extproc.EgressPeerCertificateChainKey)
		}
		encoded, ok := value.GetKind().(*structpb.Value_StringValue)
		if !ok || encoded.StringValue == "" {
			return nil, fmt.Errorf("trusted Envoy peer certificate metadata %q field is not a non-empty string", extproc.EgressPeerCertificateChainKey)
		}
		chainPEM, err := url.PathUnescape(encoded.StringValue)
		if err != nil {
			return nil, fmt.Errorf("decoding the client certificate chain: %w", err)
		}
		chain, err := parseCertificateChainPEM([]byte(chainPEM))
		if err != nil {
			return nil, err
		}
		return h.verifyActorCertificate(chain)
	case PeerCertificateSourceAgentgateway:
		certificate := md.Attribute(extproc.AgentgatewayClientCertificateAttribute)
		if certificate == "" {
			return nil, fmt.Errorf("request carries no trusted agentgateway peer certificate")
		}
		chain, err := parseCertificateChainPEM([]byte(certificate))
		if err != nil {
			return nil, err
		}
		return h.verifyActorCertificate(chain)
	default:
		return nil, fmt.Errorf("egress handler has invalid peer certificate source %q", h.peerCertificateSource)
	}
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
	// The CONNECT authenticates on the extension; the request legs attribute
	// traffic to the URI SAN, via Envoy's filter state. ateapi mints both from
	// one actor; check it rather than assume it.
	want := resources.ActorSPIFFEID(resources.ActorRef{Atespace: identity.Atespace, Name: identity.ActorName}).String()
	if len(leaf.URIs) != 1 || leaf.URIs[0].String() != want {
		return nil, fmt.Errorf("actor certificate URI SANs %v do not name the actor in its ActorIdentity extension (%s)", leaf.URIs, want)
	}
	return identity, nil
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

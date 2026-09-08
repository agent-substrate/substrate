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

// Package ateletauth authenticates RPCs that arrive from an atelet, for the
// ateapi services served only to atelet.
package ateletauth

import (
	"context"
	"log/slog"
	"net/url"
	"path"

	"github.com/agent-substrate/substrate/internal/substratex509"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

// The SPIFFE identity that atelet client certs carry, as minted by the
// podidentity signer (cmd/podcertcontroller/internal/podidentitysigner).
//
// These mirror the constants the atelet dialer verifies against in
// cmd/ateapi/internal/controlapi/dialer.go, duplicated rather than imported so
// that this package does not depend on controlapi for three strings.
const (
	TrustDomain    = "cluster.local"
	Namespace      = "ate-system"
	ServiceAccount = "atelet"
)

// Caller is the verified identity of an atelet.
type Caller struct {
	PodName  string
	NodeName string
}

// Authenticate verifies that the RPC arrived over mTLS from an atelet, and
// returns the identity that atelet's certificate asserts.
//
// The certificate chain is already verified by the TLS layer against the
// pod-identity CA (see buildServerCreds in cmd/ateapi/main.go), so the
// extensions read here are trustworthy: only the pod-identity signer can mint
// a certificate carrying a given pod's node name.
func Authenticate(ctx context.Context) (*Caller, error) {
	p, ok := peer.FromContext(ctx)
	if !ok {
		return nil, status.Errorf(codes.Unauthenticated, "no peer transport information found")
	}

	tlsInfo, ok := p.AuthInfo.(credentials.TLSInfo)
	if !ok {
		return nil, status.Errorf(codes.Unauthenticated, "unexpected peer transport credentials")
	}

	if len(tlsInfo.State.PeerCertificates) == 0 {
		return nil, status.Errorf(codes.Unauthenticated, "could not verify peer certificate")
	}
	leaf := tlsInfo.State.PeerCertificates[0]

	// Only atelet may call these RPCs. Everything else with a valid
	// pod-identity certificate — including the actor workloads themselves — is
	// rejected here.
	expected := (&url.URL{
		Scheme: "spiffe",
		Host:   TrustDomain,
		Path:   path.Join("ns", Namespace, "sa", ServiceAccount),
	}).String()
	if len(leaf.URIs) == 0 || leaf.URIs[0].String() != expected {
		slog.WarnContext(ctx, "Denied: caller is not atelet",
			slog.Any("uris", leaf.URIs), slog.String("expected", expected))
		return nil, denied()
	}

	identity, err := substratex509.PodIdentityFromCertificate(leaf)
	if err != nil {
		slog.WarnContext(ctx, "Denied: malformed PodIdentity extension", slog.Any("err", err))
		return nil, denied()
	}
	if identity == nil {
		slog.WarnContext(ctx, "Denied: certificate has no PodIdentity extension")
		return nil, denied()
	}

	return &Caller{PodName: identity.PodName, NodeName: identity.NodeName}, nil
}

// denied is deliberately uniform: a caller learns that it is not atelet, and
// nothing about why.
func denied() error {
	return status.Error(codes.PermissionDenied, "caller is not permitted")
}

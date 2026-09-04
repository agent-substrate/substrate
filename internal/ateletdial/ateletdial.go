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

// Package ateletdial connects a worker Pod to the atelet on its own node over
// the node-local socket, authenticating both ends by Pod certificate.
package ateletdial

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"
	"net/url"
	"os"
	"path"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"github.com/agent-substrate/substrate/internal/credbundle"
	"github.com/agent-substrate/substrate/internal/substratex509"
)

// TLSConfig authenticates this worker to atelet with its Pod certificate, and
// accepts only the atelet on this worker's own node.
func TLSConfig(credentialBundlePath, trustBundlePath string) (*tls.Config, error) {
	if credentialBundlePath == "" || trustBundlePath == "" {
		return nil, fmt.Errorf("worker credentials and trust bundle are required")
	}
	localCert, err := credbundle.Parse(credentialBundlePath)
	if err != nil {
		return nil, fmt.Errorf("load worker identity: %w", err)
	}
	localIdentity, err := substratex509.PodIdentityFromCertificate(localCert.Leaf)
	if err != nil || localIdentity == nil {
		return nil, fmt.Errorf("worker certificate has no valid Pod identity")
	}
	trustPEM, err := os.ReadFile(trustBundlePath)
	if err != nil {
		return nil, fmt.Errorf("read atelet trust bundle: %w", err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(trustPEM) {
		return nil, fmt.Errorf("atelet trust bundle contains no certificates")
	}
	expectedURI := (&url.URL{Scheme: "spiffe", Host: "cluster.local", Path: path.Join("ns", "ate-system", "sa", "atelet")}).String()
	return &tls.Config{
		MinVersion:           tls.VersionTLS13,
		InsecureSkipVerify:   true, // Verification below supports SPIFFE Pod certificates without a DNS name.
		GetClientCertificate: credbundle.ClientLoader(credentialBundlePath),
		VerifyConnection: func(state tls.ConnectionState) error {
			// Verify both the normal server-auth chain and the identities that DNS
			// verification cannot express: atelet's SPIFFE ID and exact node
			// incarnation. This is why InsecureSkipVerify is set above.
			if len(state.PeerCertificates) == 0 {
				return fmt.Errorf("atelet certificate is required")
			}
			intermediates := x509.NewCertPool()
			for _, cert := range state.PeerCertificates[1:] {
				intermediates.AddCert(cert)
			}
			if _, err := state.PeerCertificates[0].Verify(x509.VerifyOptions{Roots: roots, Intermediates: intermediates, KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}); err != nil {
				return fmt.Errorf("verify atelet certificate: %w", err)
			}
			leaf := state.PeerCertificates[0]
			if len(leaf.URIs) != 1 || leaf.URIs[0].String() != expectedURI {
				return fmt.Errorf("node-local peer is not atelet")
			}
			identity, err := substratex509.PodIdentityFromCertificate(leaf)
			if err != nil || identity == nil || identity.NodeName != localIdentity.NodeName || identity.NodeUID != localIdentity.NodeUID {
				return fmt.Errorf("atelet is not on worker node %q (%s)", localIdentity.NodeName, localIdentity.NodeUID)
			}
			return nil
		},
	}, nil
}

// Dial opens a connection to the atelet socket. The caller closes it; a fresh
// connection picks up rotated worker credentials and re-verifies atelet.
func Dial(socketPath string, tlsConfig *tls.Config) (*grpc.ClientConn, error) {
	return grpc.NewClient("passthrough:///atelet",
		grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig)),
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "unix", socketPath)
		}),
	)
}

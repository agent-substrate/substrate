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

package main

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"slices"
)

// gatewaySPIFFE is the only identity allowed to drive egress policy.
const gatewaySPIFFE = "spiffe://cluster.local/ns/ate-system/sa/atenet-egress"

func gatewayTLSConfig() (*tls.Config, error) {
	const (
		// Projected pod-certificate mounts. servicedns puts
		// atenet-egress-extproc.ate-system.svc in the leaf, which is the name
		// the gateway's cluster validates; podidentity is the CA the gateway's
		// own client certificate chains to.
		servingCredBundle = "/run/servicedns.podcert.ate.dev/credential-bundle.pem"
		clientCACerts     = "/run/podidentity.podcert.ate.dev/trust-bundle.pem"
	)
	return serverTLSConfig(servingCredBundle, clientCACerts, []string{gatewaySPIFFE})
}

// serverTLSConfig builds the config extprocd serves with. servingBundle is a
// Kubernetes pod-certificate credential bundle (key and chain), clientCAs the
// PEM trust bundle peers are verified against, and allowedClients the SPIFFE
// IDs permitted to connect -- empty meaning any peer that chains to clientCAs.
func serverTLSConfig(servingBundle, clientCAs string, allowedClients []string) (*tls.Config, error) {
	getCertificate := loadCredentialBundle(servingBundle)
	getClientCAs := loadTrustBundle(clientCAs)

	// Read both once here so that a bad path or an unparseable file is a
	// startup failure with a clear message, rather than a handshake failure
	// per request that reads as "the gateway cannot reach extprocd".
	if _, err := parseCredentialBundle(servingBundle); err != nil {
		return nil, fmt.Errorf("serving credential bundle %q: %w", servingBundle, err)
	}
	if _, err := getClientCAs(); err != nil {
		return nil, err
	}

	verifyPeer := peerVerifier(allowedClients)

	return &tls.Config{
		MinVersion: tls.VersionTLS13,
		ClientAuth: tls.RequireAndVerifyClientCert,
		GetConfigForClient: func(hello *tls.ClientHelloInfo) (*tls.Config, error) {
			clientCAs, err := getClientCAs()
			if err != nil {
				return nil, err
			}
			return &tls.Config{
				MinVersion:            tls.VersionTLS13,
				ClientAuth:            tls.RequireAndVerifyClientCert,
				ClientCAs:             clientCAs,
				GetCertificate:        getCertificate,
				VerifyPeerCertificate: verifyPeer,
			}, nil
		},
	}, nil
}

// peerVerifier returns a VerifyPeerCertificate that additionally requires the
// peer's SPIFFE ID to be one of allowed. A nil result means "chain verification
// is the whole check".
func peerVerifier(allowed []string) func([][]byte, [][]*x509.Certificate) error {
	if len(allowed) == 0 {
		return nil
	}
	return func(_ [][]byte, verifiedChains [][]*x509.Certificate) error {
		// Ranging over chains rather than reading the first: the leaf is the
		// same certificate in every chain, but a peer with no verified chain at
		// all must not fall through to a pass.
		for _, chain := range verifiedChains {
			if len(chain) == 0 {
				continue
			}
			for _, uri := range chain[0].URIs {
				if slices.Contains(allowed, uri.String()) {
					return nil
				}
			}
		}
		return fmt.Errorf("client certificate has no SPIFFE ID among the allowed %v", allowed)
	}
}

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

// Package mtls builds the provider's mutual-TLS server credentials. A caller
// must chain to the client trust bundle and carry the expected URI SAN, so no
// other workload that CA trusts can fetch secrets. Both files reload on
// rotation without a restart.
package mtls

import (
	"crypto/tls"
	"errors"
	"fmt"

	"google.golang.org/grpc/credentials"

	"github.com/agent-substrate/substrate/internal/plugins/gcp-secret-manager/internal/credbundle"
)

// Config names the files and caller identity ServerCredentials serves with.
type Config struct {
	// ServerBundle is the serving credential bundle (PKCS#8 key and chain).
	ServerBundle string
	// ClientCAFile is the trust bundle a caller's certificate must chain to.
	ClientCAFile string
	// CallerIdentity is the URI SAN a caller's certificate must carry.
	CallerIdentity string
}

// ServerCredentials returns gRPC server credentials requiring TLS 1.3 and a
// client certificate satisfying cfg. It fails if a field is empty or either
// bundle is unreadable, so a missing projection fails startup rather than
// leaving a Ready pod that refuses every connection.
func ServerCredentials(cfg Config) (credentials.TransportCredentials, error) {
	switch {
	case cfg.ServerBundle == "":
		return nil, errors.New("a server credential bundle is required")
	case cfg.ClientCAFile == "":
		return nil, errors.New("a client trust bundle is required")
	case cfg.CallerIdentity == "":
		return nil, errors.New("a caller identity is required")
	}

	// A per-connection config picks up a rotated client CA without a restart.
	tlsCfg, err := credbundle.PrepareServerTLSConfig(credbundle.ServerConfig{
		CertPath:     cfg.ServerBundle,
		ClientCAPath: cfg.ClientCAFile,
		ClientAuth:   tls.RequireAndVerifyClientCert,
		VerifyPeer:   verifyCallerSAN(cfg.CallerIdentity),
	})
	if err != nil {
		return nil, err
	}
	return credentials.NewTLS(tlsCfg), nil
}

// verifyCallerSAN returns a VerifyConnection callback requiring expectedSAN
// among the caller certificate's URI SANs.
func verifyCallerSAN(expectedSAN string) func(tls.ConnectionState) error {
	return func(state tls.ConnectionState) error {
		if len(state.PeerCertificates) == 0 {
			return errors.New("client certificate is required")
		}
		leaf := state.PeerCertificates[0]
		for _, u := range leaf.URIs {
			if u.String() == expectedSAN {
				return nil
			}
		}
		return fmt.Errorf("client certificate URI SANs %v do not include the expected caller identity %q", leaf.URIs, expectedSAN)
	}
}

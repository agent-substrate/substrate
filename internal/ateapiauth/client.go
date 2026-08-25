//  Copyright 2026 Google LLC
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

package ateapiauth

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"

	"github.com/agent-substrate/substrate/internal/credbundle"
	"github.com/agent-substrate/substrate/internal/k8sresolver"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"k8s.io/client-go/kubernetes"
)

const DefaultServiceAccountCAFile = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"

// roundRobinServiceConfig spreads RPCs across every address the resolver
// returns.
const roundRobinServiceConfig = `{"loadBalancingConfig": [{"round_robin":{}}]}`

// ClientConfig configures how to dial the ateapi gRPC server with mutual TLS.
// The credential bundle is re-read on every handshake so in-place
// pod-certificate rotations are picked up.
type ClientConfig struct {
	// CAFile is a PEM file containing CA certs that sign the server cert.
	// Required.
	CAFile string

	// ServerName overrides SNI / hostname verification. Optional.
	ServerName string

	// ClientCredBundle is a PEM file containing the client certificate chain
	// and PKCS8 private key presented to the server. Required.
	ClientCredBundle string

	// K8sClient is an optional Kubernetes client. When provided, an EndpointSlice
	// resolver builder using this client will be attached to DialOptions.
	K8sClient kubernetes.Interface
}

// DialOptions returns the grpc.DialOption set described by cfg, suitable to
// pass to grpc.NewClient.
func DialOptions(cfg ClientConfig) ([]grpc.DialOption, error) {
	if cfg.CAFile == "" {
		return nil, fmt.Errorf("ateapiauth: CAFile is required")
	}
	if cfg.ClientCredBundle == "" {
		return nil, fmt.Errorf("ateapiauth: a client credential bundle (mTLS) is required")
	}
	pool, err := loadCAPool(cfg.CAFile)
	if err != nil {
		return nil, err
	}
	tlsCfg := &tls.Config{
		MinVersion: tls.VersionTLS13,
		RootCAs:    pool,
		ServerName: cfg.ServerName,
	}

	opts := []grpc.DialOption{
		grpc.WithDefaultServiceConfig(roundRobinServiceConfig),
	}
	if cfg.K8sClient != nil {
		opts = append(opts, grpc.WithResolvers(k8sresolver.NewBuilder(cfg.K8sClient)))
	}

	tlsCfg.GetClientCertificate = credbundle.ClientLoader(cfg.ClientCredBundle)
	opts = append(opts, grpc.WithTransportCredentials(credentials.NewTLS(tlsCfg)))
	return opts, nil
}

func loadCAPool(caFile string) (*x509.CertPool, error) {
	caPEM, err := os.ReadFile(caFile)
	if err != nil {
		return nil, fmt.Errorf("ateapiauth: reading CA file: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caPEM) {
		return nil, fmt.Errorf("ateapiauth: no certificates found in CA file %q", caFile)
	}
	return pool, nil
}

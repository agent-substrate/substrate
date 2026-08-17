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
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"strings"

	"github.com/agent-substrate/substrate/internal/credbundle"
	"github.com/agent-substrate/substrate/internal/k8sresolver"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"k8s.io/client-go/kubernetes"
)

const (
	DefaultServiceAccountCAFile    = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"
	DefaultServiceAccountTokenFile = "/var/run/secrets/kubernetes.io/serviceaccount/token"
)

// roundRobinServiceConfig spreads RPCs across every address the resolver
// returns.
const roundRobinServiceConfig = `{"loadBalancingConfig": [{"round_robin":{}}]}`

// ClientConfig configures how to dial the ateapi gRPC server. The server
// cert is always validated against CAFile. UseTokenAuth selects the client
// credential: a client certificate from ClientCredBundle (mutual TLS,
// re-read on every handshake so in-place pod-certificate rotations are
// picked up) by default, or a Bearer token from TokenFile sent as per-RPC
// credentials. The path not selected is ignored.
type ClientConfig struct {
	// UseTokenAuth authenticates with the Bearer token from TokenFile instead
	// of the client certificate from ClientCredBundle.
	UseTokenAuth bool

	// CAFile is a PEM file containing CA certs that sign the server cert.
	// Required.
	CAFile string

	// ServerName overrides SNI / hostname verification. Optional.
	ServerName string

	// TokenFile is a path to a Kubernetes projected ServiceAccount token used
	// as a Bearer credential. Required when UseTokenAuth is set, ignored
	// otherwise.
	TokenFile string

	// ClientCredBundle is a PEM file containing the client certificate chain
	// and PKCS8 private key presented to the server. Required unless
	// UseTokenAuth is set, ignored otherwise.
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
	if cfg.UseTokenAuth {
		if cfg.TokenFile == "" {
			return nil, fmt.Errorf("ateapiauth: token auth requires a token file")
		}
	} else if cfg.ClientCredBundle == "" {
		return nil, fmt.Errorf("ateapiauth: a client credential bundle (mTLS) is required unless token auth is enabled")
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

	if cfg.UseTokenAuth {
		opts = append(opts,
			grpc.WithTransportCredentials(credentials.NewTLS(tlsCfg)),
			grpc.WithPerRPCCredentials(&fileTokenCreds{path: cfg.TokenFile}),
		)
		return opts, nil
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

// fileTokenCreds reads a Kubernetes projected SA token from disk for every
// RPC. Kubernetes refreshes the file in place; reading it each time picks up
// rotations.
type fileTokenCreds struct {
	path string
}

func (c *fileTokenCreds) GetRequestMetadata(_ context.Context, _ ...string) (map[string]string, error) {
	b, err := os.ReadFile(c.path)
	if err != nil {
		return nil, fmt.Errorf("ateapiauth: reading token file %q: %w", c.path, err)
	}
	tok := strings.TrimSpace(string(b))
	if tok == "" {
		return nil, fmt.Errorf("ateapiauth: token file %q is empty", c.path)
	}
	return map[string]string{"authorization": "Bearer " + tok}, nil
}

func (c *fileTokenCreds) RequireTransportSecurity() bool { return true }

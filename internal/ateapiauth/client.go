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
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

const (
	DefaultServiceAccountCAFile    = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"
	DefaultServiceAccountTokenFile = "/var/run/secrets/kubernetes.io/serviceaccount/token"
)

// ClientConfig configures how to dial the ateapi gRPC server.
//
//   - Mode=ModeMTLS: mutual TLS. Validates the server cert against CAFile and
//     presents the client certificate from ClientCredBundle, re-read on every
//     handshake so in-place pod-certificate rotations are picked up.
//   - Mode=ModeJWT: validates the server cert against CAFile, sends a Bearer
//     token from TokenFile as per-RPC credentials.
type ClientConfig struct {
	Mode Mode

	// CAFile is a PEM file containing CA certs that sign the server cert.
	// Required in all modes.
	CAFile string

	// ServerName overrides SNI / hostname verification. Optional.
	ServerName string

	// TokenFile is a path to a Kubernetes projected ServiceAccount token used
	// as a Bearer credential. Required for ModeJWT.
	TokenFile string

	// ClientCredBundle is a PEM file containing the client certificate chain
	// and PKCS8 private key presented to the server. Required for ModeMTLS
	// and ignored for ModeJWT.
	ClientCredBundle string
}

// DialOptions returns the grpc.DialOption set described by cfg, suitable to
// pass to grpc.NewClient.
func DialOptions(cfg ClientConfig) ([]grpc.DialOption, error) {
	if cfg.CAFile == "" {
		return nil, fmt.Errorf("ateapiauth: CAFile is required")
	}
	switch cfg.Mode {
	case "", ModeMTLS:
		if cfg.ClientCredBundle == "" {
			return nil, fmt.Errorf("ateapiauth: mtls mode requires ClientCredBundle")
		}
		pool, err := loadCAPool(cfg.CAFile)
		if err != nil {
			return nil, err
		}
		tlsCfg := &tls.Config{
			MinVersion:           tls.VersionTLS13,
			RootCAs:              pool,
			ServerName:           cfg.ServerName,
			GetClientCertificate: credbundle.ClientLoader(cfg.ClientCredBundle),
		}
		return []grpc.DialOption{
			grpc.WithTransportCredentials(credentials.NewTLS(tlsCfg)),
		}, nil

	case ModeJWT:
		if cfg.TokenFile == "" {
			return nil, fmt.Errorf("ateapiauth: jwt mode requires TokenFile")
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
		return []grpc.DialOption{
			grpc.WithTransportCredentials(credentials.NewTLS(tlsCfg)),
			grpc.WithPerRPCCredentials(&fileTokenCreds{path: cfg.TokenFile}),
		}, nil

	default:
		return nil, fmt.Errorf("ateapiauth: unknown client mode %q", cfg.Mode)
	}
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

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

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

const (
	DefaultServiceAccountCAFile    = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"
	DefaultServiceAccountTokenFile = "/var/run/secrets/kubernetes.io/serviceaccount/token"
)

// ClientConfig configures how to dial the ateapi gRPC server.
//
//   - Mode=ModeMTLS: insecure TLS dial (InsecureSkipVerify=true). Client
//     identity is expected to come from mTLS credentials projected into
//     the pod (servicedns.podcert.ate.dev). No app-level credentials.
//   - Mode=ModeJWT: validates the server cert against CAFile, sends a Bearer
//     token from TokenFile as per-RPC credentials.
type ClientConfig struct {
	Mode Mode

	// CAFile is a PEM file containing CA certs that sign the server cert.
	// Required in all modes. Ignored for ModeMTLS until mTLS verification is
	// fully wired.
	CAFile string

	// ServerName overrides SNI / hostname verification. Optional.
	ServerName string

	// TokenFile is a path to a Kubernetes projected ServiceAccount token used
	// as a Bearer credential. Required for ModeJWT.
	TokenFile string
}

// DialOptions returns the grpc.DialOption set described by cfg, suitable to
// pass to grpc.NewClient.
func DialOptions(cfg ClientConfig) ([]grpc.DialOption, error) {
	if cfg.CAFile == "" {
		return nil, fmt.Errorf("ateapiauth: CAFile is required")
	}
	switch cfg.Mode {
	case "", ModeMTLS:
		tlsCfg := &tls.Config{InsecureSkipVerify: true} //nolint:gosec // explicit opt-in
		return []grpc.DialOption{
			grpc.WithTransportCredentials(credentials.NewTLS(tlsCfg)),
		}, nil

	case ModeJWT:
		if cfg.TokenFile == "" {
			return nil, fmt.Errorf("ateapiauth: jwt mode requires TokenFile")
		}
		caPEM, err := os.ReadFile(cfg.CAFile)
		if err != nil {
			return nil, fmt.Errorf("ateapiauth: reading CA file: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(caPEM) {
			return nil, fmt.Errorf("ateapiauth: no certificates found in CA file %q", cfg.CAFile)
		}
		tlsCfg := &tls.Config{
			MinVersion: tls.VersionTLS12,
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

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

// This file connects the egress gateway to the credential provider it resolves
// egress credential injections through: dialing it, reducing the configured
// provider prefix to the provider name the handler enforces, and the small
// validators the injection path shares. The gateway's atenet router wires
// DialProvider and ProviderName in when it builds the egress handler (see
// cmd/atenet/internal/router).

package egress

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"net/url"
	"strings"

	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/agent-substrate/substrate/internal/ateapiauth"
)

// credentialURIScheme is the only scheme a credential URI may carry.
const credentialURIScheme = "ate-secret"

// ProviderDialConfig configures the mTLS connection the egress gateway dials the
// credential provider with.
type ProviderDialConfig struct {
	// Address is the provider's gRPC dial target.
	Address string
	// CAFile is the CA the provider's serving certificate must chain to.
	// Required unless Insecure is set.
	CAFile string
	// ClientCert is the credential bundle presented to the provider as the
	// client certificate. Required unless Insecure is set.
	ClientCert string
	// ServerName is the SAN/SNI expected on the provider's serving certificate.
	ServerName string
	// Insecure dials the provider without TLS. Explicit opt-in for development
	// only: secrets would cross the network in the clear. Without it, a missing
	// CAFile or ClientCert is an error rather than a silent downgrade.
	Insecure bool
}

// DialProvider dials the credential provider over mTLS, with the same client
// auth the rest of atenet uses for ateapi. Plaintext requires the explicit
// Insecure opt-in; a missing CA or client credential bundle is otherwise an
// error. The caller owns the returned connection.
func DialProvider(ctx context.Context, cfg ProviderDialConfig) (*grpc.ClientConn, error) {
	statsOpt := grpc.WithStatsHandler(otelgrpc.NewClientHandler())

	if cfg.Insecure {
		slog.WarnContext(ctx, "dialing credential provider WITHOUT TLS (--credential-provider-insecure); development only")
		return grpc.NewClient(cfg.Address, grpc.WithTransportCredentials(insecure.NewCredentials()), statsOpt)
	}

	dialOpts, err := ateapiauth.DialOptions(ateapiauth.ClientConfig{
		CAFile:           cfg.CAFile,
		ServerName:       cfg.ServerName,
		ClientCredBundle: cfg.ClientCert,
	})
	if err != nil {
		return nil, fmt.Errorf("building credential-provider dial options: %w", err)
	}
	return grpc.NewClient(cfg.Address, append(dialOpts, statsOpt)...)
}

// ProviderName reduces a configured provider — a ate-secret:// prefix such
// as ate-secret://kubernetes.io — to the provider name (the URI host) the
// handler compares credential URIs against, so a URI naming another provider
// fails closed. An empty input returns an empty name, which disables the check
// (dev only).
func ProviderName(name string) (string, error) {
	if name == "" {
		return "", nil
	}
	return providerNameFromURI(name)
}

// providerNameFromURI returns the provider name of a ate-secret:// URI —
// the URI host, e.g. "kubernetes.io" in
// ate-secret://kubernetes.io/<namespace>/<secret>. The gateway uses it to
// confirm a URI targets the provider it is configured to serve.
func providerNameFromURI(raw string) (string, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return "", fmt.Errorf("parsing credential URI %q: %w", raw, err)
	}
	if u.Scheme != credentialURIScheme {
		return "", fmt.Errorf("credential URI %q: scheme is %q, want %q", raw, u.Scheme, credentialURIScheme)
	}
	if u.Host == "" {
		return "", fmt.Errorf("credential URI %q: missing provider name", raw)
	}
	return u.Host, nil
}

// validateInjectHeader rejects header names the egress gateway's ext_proc
// mutation rules (disallow_system + disallow_is_error) would turn into a hard
// request failure: the HTTP/2 pseudo-headers and Host. The egress-policy API
// validates header format on write, so this is a defensive backstop.
func validateInjectHeader(name string) error {
	if name == "" {
		return fmt.Errorf("credential injection header is required")
	}
	if strings.HasPrefix(name, ":") || strings.EqualFold(name, "host") {
		return fmt.Errorf("credential injection header %q is a system header the gateway forbids mutating", name)
	}
	return nil
}

// sanitizeSecret prepares resolved secret bytes for use as (part of) an HTTP
// header value. It trims a trailing newline — a Kubernetes Secret created from a
// file commonly carries one — then rejects an empty secret or one containing any
// control character, which Envoy would reject as an invalid header value and, in
// the case of CR/LF, would allow header injection.
func sanitizeSecret(secret []byte) ([]byte, error) {
	secret = bytes.TrimRight(secret, "\r\n")
	if len(secret) == 0 {
		return nil, fmt.Errorf("resolved credential is empty")
	}
	for _, b := range secret {
		if b < 0x20 || b == 0x7f {
			return nil, fmt.Errorf("resolved credential contains a control character")
		}
	}
	return secret, nil
}

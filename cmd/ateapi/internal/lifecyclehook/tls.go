// Copyright 2026 The Agent Substrate Authors
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

package lifecyclehook

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"strings"

	"go.ate.dev/substrate/internal/credbundle"
	"google.golang.org/grpc/credentials"
)

// TLSOptions provides the server-level client credential bundle path used when
// a hook target omits clientCredBundleFile under MTLS.
type TLSOptions struct {
	DefaultClientCredBundlePath string
}

// buildTargetTLSConfig constructs a TLS 1.3 client configuration for a single hook target.
func buildTargetTLSConfig(cfg HookTLSConfig, opts TLSOptions) (*tls.Config, error) {
	roots, err := loadRootCAPool(cfg)
	if err != nil {
		return nil, err
	}

	tlsCfg := &tls.Config{
		MinVersion: tls.VersionTLS13,
		ServerName: cfg.ServerName,
		NextProtos: []string{"h2"},
		RootCAs:    roots,
	}

	if cfg.ClientAuthMode == "" || cfg.ClientAuthMode == HookClientAuthModeMTLS {
		credPath := cfg.ClientCredBundleFile
		if credPath == "" {
			credPath = opts.DefaultClientCredBundlePath
		}
		if credPath == "" {
			return nil, fmt.Errorf("clientAuthMode is MTLS but no clientCredBundleFile or default client credential bundle path is configured")
		}
		loader := credbundle.ClientLoader(credPath)
		if _, err := loader(nil); err != nil {
			return nil, fmt.Errorf("loading client credential bundle %q: %w", credPath, err)
		}
		tlsCfg.GetClientCertificate = loader
	}

	return tlsCfg, nil
}

func loadRootCAPool(cfg HookTLSConfig) (*x509.CertPool, error) {
	var pemBytes []byte
	switch {
	case len(cfg.CABundle) > 0:
		pemBytes = cfg.CABundle
	case cfg.CACertFile != "":
		b, err := os.ReadFile(cfg.CACertFile)
		if err != nil {
			return nil, fmt.Errorf("reading caCertFile %q: %w", cfg.CACertFile, err)
		}
		pemBytes = b
	default:
		return nil, fmt.Errorf("no CA trust source configured")
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pemBytes) {
		return nil, fmt.Errorf("CA bundle contains no valid PEM certificates")
	}
	return pool, nil
}

func buildTransportCredentials(cfg HookTLSConfig, opts TLSOptions) (credentials.TransportCredentials, error) {
	tlsCfg, err := buildTargetTLSConfig(cfg, opts)
	if err != nil {
		return nil, err
	}
	return credentials.NewTLS(tlsCfg), nil
}

// normalizeDialTarget translates tcp://host:port into dns:///host:port so
// grpc.NewClient resolves the address with its default DNS resolver.
func normalizeDialTarget(addr string) string {
	if after, ok := strings.CutPrefix(addr, "tcp://"); ok {
		return "dns:///" + after
	}
	return addr
}

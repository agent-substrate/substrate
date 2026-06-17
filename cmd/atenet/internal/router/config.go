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

package router

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"time"

	"github.com/agent-substrate/substrate/internal/credbundle"
	"google.golang.org/grpc/credentials"
)

type authConfig struct {
	AteapiClientCertPath string
	AteapiCACertsPath    string
}

// routerConfig holds deployment setup and endpoint options for the router node instance.
type routerConfig struct {
	Standalone     bool
	Namespace      string
	Kubeconfig     string
	AteapiAddr     string
	HttpPort       int
	XdsPort        int
	ExtprocPort    int
	ExtprocAddr    string
	EnvoyImage     string
	TemplatesFile  string
	StatusPort     int
	HealthInterval time.Duration
	HttpsPort      int
	EnvoyCertPath  string
	LogLevel       string
	MetricsAddr    string
	Auth           authConfig
}

// ateapiTransportCreds builds the TLS credentials the router uses to dial
// ateapi.
func (cfg *routerConfig) apiTransportCredentials() (credentials.TransportCredentials, error) {
	tlsCfg, err := cfg.apiTLSConfig()
	if err != nil {
		return nil, err
	}
	return credentials.NewTLS(tlsCfg), nil
}

func (cfg *routerConfig) apiTLSConfig() (*tls.Config, error) {
	caBytes, err := os.ReadFile(cfg.Auth.AteapiCACertsPath)
	if err != nil {
		return nil, fmt.Errorf("error reading ateapi CA certs: %w", err)
	}
	rootCAs := x509.NewCertPool()
	if !rootCAs.AppendCertsFromPEM(caBytes) {
		return nil, fmt.Errorf("parse ateapi CA certs from %s", cfg.Auth.AteapiCACertsPath)
	}

	if _, err := os.Stat(cfg.Auth.AteapiClientCertPath); err != nil {
		return nil, fmt.Errorf("error reading ate apiserver client cert path from %q, error:%w",
			cfg.Auth.AteapiClientCertPath, err)
	}

	return &tls.Config{
		RootCAs:              rootCAs,
		GetClientCertificate: credbundle.ClientLoader(cfg.Auth.AteapiClientCertPath),
	}, nil
}

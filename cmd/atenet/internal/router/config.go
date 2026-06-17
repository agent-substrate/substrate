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
	"github.com/spf13/cobra"
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

func NewRouterCmd() *cobra.Command {
	var cfg routerConfig

	cmd := &cobra.Command{
		Use:   "router",
		Short: "Router components including xDS server and Envoy ExtProc gateway processing server",
		RunE: func(cmd *cobra.Command, args []string) error {
			srv, err := NewRouterServer(cfg)
			if err != nil {
				return fmt.Errorf("failed to create router server: %w", err)
			}
			srv.Cmd = cmd

			return srv.Run(cmd.Context())
		},
	}

	cmd.Flags().StringVar(&cfg.LogLevel, "log-level", "info", "Log level: debug, info, warn, error")
	cmd.Flags().StringVar(&cfg.MetricsAddr, "metrics-listen-addr", ":9090", "Address and port the prometheus metrics server should listen on.")
	cmd.Flags().BoolVar(&cfg.Standalone, "standalone", false, "Run in standalone mode, bypassing creation of managed deployment and services in Kubernetes cluster")
	cmd.Flags().StringVar(&cfg.Namespace, "namespace", "default", "Target operations namespace")
	cmd.Flags().StringVar(&cfg.Kubeconfig, "kubeconfig", "", "Absolute path to the kubeconfig configuration file")
	cmd.Flags().StringVar(&cfg.AteapiAddr, "ateapi-address", "api.ate-system.svc:443", "gRPC host address of the cluster ateapi Control instance")
	cmd.Flags().StringVar(&cfg.Auth.AteapiClientCertPath, "ateapi-client-cert", "/run/podidentity.podcert.ate.dev/credential-bundle.pem", "Path to the podidentity credential bundle the router presents as its client cert to ateapi.")
	cmd.Flags().StringVar(&cfg.Auth.AteapiCACertsPath, "ateapi-ca-certs", "/run/servicedns-ca.podcert.ate.dev/trust-bundle.pem", "Path to the servicedns trust bundle used to verify ateapi's serving cert.")
	cmd.Flags().IntVar(&cfg.HttpPort, "port-http", 8080, "TCP port for workload traffic entering through the Envoy Router")
	cmd.Flags().IntVar(&cfg.XdsPort, "port-xds", 18000, "TCP port listening for the xDS dynamic Envoy connections")
	cmd.Flags().IntVar(&cfg.ExtprocPort, "port-extproc", 50051, "Listen port for the Envoy dynamic External Processing (ext_proc) server")
	cmd.Flags().StringVar(&cfg.ExtprocAddr, "extproc-address", "127.0.0.1", "Host IP or address of the Envoy External Processing (ext_proc) server")
	cmd.Flags().StringVar(&cfg.EnvoyImage, "envoy-image", "envoyproxy/envoy:v1.30-latest", "Image URI used for dynamically launched router instances")
	cmd.Flags().StringVar(&cfg.TemplatesFile, "actor-templates-file", "", "Path to offline YAML configuration file listing ActorTemplates")
	cmd.Flags().IntVar(&cfg.StatusPort, "status-port", 4040, "Port to serve /statusz on (set <= 0 to disable serving status)")
	cmd.Flags().DurationVar(&cfg.HealthInterval, "health-interval", 1*time.Second, "Interval for checking health of dependent services")
	cmd.Flags().IntVar(&cfg.HttpsPort, "port-https", 8443, "TCP port for HTTPS workload traffic entering through the Envoy Router")
	cmd.Flags().StringVar(&cfg.EnvoyCertPath, "envoy-cert-path", "", "Path to the Envoy certificate file.")

	return cmd
}

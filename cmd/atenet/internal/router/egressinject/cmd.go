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

package egressinject

import (
	"time"

	"github.com/spf13/cobra"
)

type config struct {
	ProviderName    string
	ProviderAddress string
	ExtprocPort     int
	MetricsAddr     string
	LogLevel        string
	DrainGrace      time.Duration

	// Serving TLS (the egress gateway dials this server with mTLS + SAN pin).
	ServerCredBundle string
	ClientCAFile     string

	// Client TLS presented to the credential provider.
	ProviderCAFile     string
	ProviderClientCert string
	ProviderServerName string

	// ateapi Control connection: where each actor's egress policy is fetched.
	AteapiAddr       string
	AteapiCAFile     string
	AteapiClientCert string
	AteapiServerName string
}

// NewCmd builds the `atenet egress-inject` subcommand: the ext_proc server the
// egress gateway's MITM leg calls to inject credentials into outbound requests.
func NewCmd() *cobra.Command {
	var cfg config

	cmd := &cobra.Command{
		Use:   "egress-inject",
		Short: "ext_proc server that injects policy-selected credentials into egress requests on the MITM leg",
		RunE: func(cmd *cobra.Command, _ []string) error {
			return run(cmd.Context(), cfg)
		},
	}

	f := cmd.Flags()
	f.StringVar(&cfg.ProviderName, "credential-provider-name", "substrate-secret://kubernetes.io", "the credential-provider this injector serves, as a substrate-secret:// class prefix (e.g. substrate-secret://kubernetes.io); a policy credential URI of any other class is refused (empty disables the check, dev only)")
	f.StringVar(&cfg.ProviderAddress, "credential-provider-address", "", "address of the credential provider gRPC service; required")
	f.IntVar(&cfg.ExtprocPort, "port-extproc", 50051, "ext_proc gRPC listen port")
	f.StringVar(&cfg.MetricsAddr, "metrics-address", ":9090", "Prometheus/health HTTP listen address")
	f.StringVar(&cfg.LogLevel, "log-level", "info", "one of debug, info, warn, error")
	f.DurationVar(&cfg.DrainGrace, "drain-grace", 5*time.Second, "how long to wait for in-flight RPCs on shutdown before a hard stop")

	f.StringVar(&cfg.ServerCredBundle, "server-cred-bundle", "", "credential bundle (PEM key+chain) presented for serving TLS to the gateway; empty serves plaintext (dev only)")
	f.StringVar(&cfg.ClientCAFile, "client-ca-file", "", "CA the gateway's client certificate must chain to; empty accepts any client when TLS is on")

	f.StringVar(&cfg.ProviderCAFile, "provider-ca-file", "", "CA the credential provider's serving certificate must chain to; empty dials the provider plaintext (dev only)")
	f.StringVar(&cfg.ProviderClientCert, "provider-client-cert", "", "credential bundle presented to the credential provider")
	f.StringVar(&cfg.ProviderServerName, "provider-server-name", "", "expected SAN/SNI of the credential provider's serving certificate")

	f.StringVar(&cfg.AteapiAddr, "ateapi-address", "dns:///api.ate-system.svc:443", "gRPC dial target for the cluster ateapi Control instance, from which each actor's egress policy is fetched")
	f.StringVar(&cfg.AteapiCAFile, "ateapi-ca-file", "", "CA the ateapi serving certificate must chain to; required")
	f.StringVar(&cfg.AteapiClientCert, "ateapi-client-cert", "", "credential bundle presented to ateapi as the client certificate; required")
	f.StringVar(&cfg.AteapiServerName, "ateapi-server-name", "", "expected SAN/SNI of the ateapi serving certificate")

	return cmd
}

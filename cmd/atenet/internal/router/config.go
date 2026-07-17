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
	"time"
)

// authConfig holds the router's client-auth settings for dialing ateapi.
// AteapiCAFile always verifies ateapi's serving cert (the servicedns trust
// bundle in-cluster). By default the router presents AteapiClientCertPath
// (the podidentity credential bundle) as its client cert; with
// AteapiUseTokenAuth it sends a Bearer token from AteapiTokenFile instead and
// the cert path is ignored.
type authConfig struct {
	AteapiUseTokenAuth   bool
	AteapiCAFile         string
	AteapiClientCertPath string
	AteapiServerName     string
	AteapiTokenFile      string
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
	// OtlpCollectorAddress is the host:port of the OTLP gRPC collector that
	// Envoy reports tracing spans to. Empty disables Envoy-side tracing.
	OtlpCollectorAddress string

	Auth authConfig
}

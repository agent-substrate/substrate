// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package egresscapture

const (
	EnvCaptureEnabled               = "ATE_EGRESS_CAPTURE_ENABLED"
	EnvPEPAddress                   = "ATE_EGRESS_PEP_ADDRESS"
	EnvTunnelProtocol               = "ATE_EGRESS_TUNNEL_PROTOCOL"
	EnvConnectTLSServerName         = "ATE_EGRESS_CONNECT_TLS_SERVER_NAME"
	EnvConnectTLSCAFile             = "ATE_EGRESS_CONNECT_TLS_CA_FILE"
	EnvConnectTLSInsecureSkipVerify = "ATE_EGRESS_CONNECT_TLS_INSECURE_SKIP_VERIFY"
	DefaultTunnelProtocol           = TunnelProtocolConnect
	TunnelProtocolConnect           = "connect"
	TunnelProtocolPlaintext         = "plaintext"
	TunnelProtocolH2C               = "h2c"
	TunnelProtocolConnectTLS        = "connect-tls"
	TunnelProtocolHTTPSConnect      = "https-connect"
	TunnelProtocolTLSConnect        = "tls-connect"
	FutureTunnelProtocolHBONE       = "hbone"
)

var OptionalEnvNames = []string{
	EnvConnectTLSServerName,
	EnvConnectTLSCAFile,
	EnvConnectTLSInsecureSkipVerify,
}

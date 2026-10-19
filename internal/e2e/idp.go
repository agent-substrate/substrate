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

package e2e

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"
	"net/http"
	"testing"
	"time"

	"go.ate.dev/substrate/internal/ateclient"
	"go.ate.dev/substrate/internal/portforward"
)

// IDPHTTPClient returns a client that reaches ate-idp-server through a
// port-forward whatever host a URL names, and checks its certificate against
// the service-DNS signer's roots and the name its Service gives it. A verifier
// can then discover keys from an issuer that resolves only in the cluster.
func IDPHTTPClient(t *testing.T, ctx context.Context) *http.Client {
	t.Helper()
	clients := GetClients()
	config, err := ateclient.LoadKubeConfig(KubeConfig, KubeContext)
	if err != nil {
		t.Fatalf("loading kubeconfig: %v", err)
	}
	service := ResourceName("idp")
	port, stop, err := portforward.ServicePortForward(ctx, config, clients.K8s, SystemNamespace(), service, 443)
	if err != nil {
		t.Fatalf("port-forwarding to service %s: %v", service, err)
	}
	t.Cleanup(stop)

	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM([]byte(ServiceDNSTrustBundle(t, ctx, clients))) {
		t.Fatal("the service-DNS trust bundle holds no certificates to verify ate-idp-server with")
	}
	addr := fmt.Sprintf("127.0.0.1:%d", port)
	var dialer net.Dialer
	return &http.Client{
		Timeout: 30 * time.Second,
		Transport: &http.Transport{
			DialContext: func(ctx context.Context, network, _ string) (net.Conn, error) {
				return dialer.DialContext(ctx, network, addr)
			},
			TLSClientConfig: &tls.Config{
				RootCAs:    roots,
				ServerName: fmt.Sprintf("%s.%s.svc", service, SystemNamespace()),
			},
		},
	}
}

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

package ateclient

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"net/http"
	"sync"
	"time"

	"go.ate.dev/substrate/internal/installdefaults"
	"go.ate.dev/substrate/internal/podidentity"
	"go.ate.dev/substrate/internal/portforward"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

const (
	// routerHTTPSPort is atenet-router's HTTPS Service port.
	routerHTTPSPort = 443
	// routerCertTimeout bounds the wait for the podidentity signer.
	routerCertTimeout = 2 * time.Minute
	// routerCertRequester names this client on its PodCertificateRequests.
	routerCertRequester = "kubectl-ate"
)

// RouterServerName is the name on atenet-router's serving certificate.
func RouterServerName() string {
	return fmt.Sprintf("%s.%s.svc", installdefaults.RouterServiceName, systemNamespace())
}

// NewRouterTransport returns a transport that reaches atenet-router's HTTPS
// listener through a port-forward. It verifies the router's servicedns serving
// certificate and presents a podidentity certificate for the ingress client
// ServiceAccount, which the router allows by default.
//
// The certificate is requested before NewRouterTransport returns, so a caller
// without RBAC to create PodCertificateRequests fails here. It is requested
// again once half its lifetime has passed.
func NewRouterTransport(ctx context.Context, kubeconfigPath, k8sContext string) (*http.Transport, error) {
	config, err := LoadKubeConfig(kubeconfigPath, k8sContext)
	if err != nil {
		return nil, fmt.Errorf("failed to load kubeconfig: %w", err)
	}
	// Older clusters use the deprecated beta ClusterTrustBundle and
	// PodCertificateRequest APIs. Suppress their warnings when falling back
	// from v1.
	config.WarningHandlerWithContext = &rest.NoWarnings{}
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create k8s client: %w", err)
	}

	tlsCfg, err := serverTLSConfig(ctx, clientset, RouterServerName())
	if err != nil {
		return nil, err
	}
	certs := &routerClientCerts{clientset: clientset}
	if _, err := certs.get(ctx); err != nil {
		return nil, err
	}
	tlsCfg.GetClientCertificate = func(info *tls.CertificateRequestInfo) (*tls.Certificate, error) {
		return certs.get(info.Context())
	}

	return &http.Transport{
		// client-go tears down a whole port-forward when any one of its
		// streams errors, so each connection gets its own. That also picks
		// a ready router pod again after one goes away.
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			localPort, stop, err := portforward.ServicePortForward(ctx, config, clientset, systemNamespace(), installdefaults.RouterServiceName, routerHTTPSPort)
			if err != nil {
				return nil, err
			}
			var d net.Dialer
			conn, err := d.DialContext(ctx, "tcp", fmt.Sprintf("127.0.0.1:%d", localPort))
			if err != nil {
				stop()
				return nil, err
			}
			return &forwardedConn{Conn: conn, stop: stop}, nil
		},
		TLSClientConfig: tlsCfg,
		IdleConnTimeout: 90 * time.Second,
	}, nil
}

// routerClientCerts holds the current router client certificate.
type routerClientCerts struct {
	clientset kubernetes.Interface

	mu        sync.Mutex
	cert      *tls.Certificate
	refreshAt time.Time
}

func (c *routerClientCerts) get(ctx context.Context) (*tls.Certificate, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.cert != nil && time.Now().Before(c.refreshAt) {
		return c.cert, nil
	}
	namespace := systemNamespace()
	cert, err := podidentity.RequestCertificate(ctx, c.clientset, routerCertRequester, namespace, installdefaults.IngressClientServiceAccount, routerCertTimeout)
	if err != nil {
		return nil, fmt.Errorf("requesting a router client certificate for %s/%s: %w", namespace, installdefaults.IngressClientServiceAccount, err)
	}
	c.cert = &cert
	c.refreshAt = cert.Leaf.NotBefore.Add(cert.Leaf.NotAfter.Sub(cert.Leaf.NotBefore) / 2)
	return c.cert, nil
}

// forwardedConn stops its port-forward when closed.
type forwardedConn struct {
	net.Conn
	stop     func()
	stopOnce sync.Once
}

func (c *forwardedConn) Close() error {
	err := c.Conn.Close()
	c.stopOnce.Do(c.stop)
	return err
}

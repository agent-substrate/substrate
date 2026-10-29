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

package boomerutil

import (
	"crypto/tls"
	"fmt"
	"os"
	"sync"

	"go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/internal/ateapiauth"
	"go.ate.dev/substrate/internal/credbundle"
	_ "go.ate.dev/substrate/internal/k8sresolver"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

const (
	// DefaultRouterURL is the default HTTPS base URL for atenet-router.
	DefaultRouterURL = "https://atenet-router.ate-system.svc"

	// servicednsCAFile verifies ateapi's and atenet-router's servicedns serving
	// certs; podIdentityCredBundle is the benchmark's own pod certificate. Both
	// are projected in by benchmarking/locust/manifests/locust.yaml.
	servicednsCAFile      = "/run/servicedns-ca/ca.crt"
	podIdentityCredBundle = "/run/podidentity.podcert.ate.dev/credential-bundle.pem"

	// ateapiServerName is the DNS SAN on the apiserver's serving cert, which
	// is shorter than the endpoint the benchmark dials.
	ateapiServerName = "api.ate-system.svc"

	// routerServerName is the 3-label DNS SAN on atenet-router's servicedns
	// serving cert.
	routerServerName = "atenet-router.ate-system.svc"
)

var routerTLSConfig = sync.OnceValues(func() (*tls.Config, error) {
	return newRouterTLSConfig(servicednsCAFile, podIdentityCredBundle, routerServerName)
})

func newRouterTLSConfig(caFile, credBundle, serverName string) (*tls.Config, error) {
	roots, err := credbundle.ParsePool(caFile)
	if err != nil {
		return nil, fmt.Errorf("loading router CA file: %w", err)
	}
	if _, err := os.Stat(credBundle); err != nil {
		return nil, fmt.Errorf("checking router client credential bundle: %w", err)
	}
	return &tls.Config{
		MinVersion:           tls.VersionTLS13,
		RootCAs:              roots,
		ServerName:           serverName,
		GetClientCertificate: credbundle.ClientLoader(credBundle),
	}, nil
}

// RouterTLSConfig returns a mutual-TLS client configuration for dialing
// atenet-router.
func RouterTLSConfig() (*tls.Config, error) {
	cfg, err := routerTLSConfig()
	if err != nil {
		return nil, err
	}
	return cfg.Clone(), nil
}

// DialControl opens an authenticated gRPC connection to ateapi.
//
// ateapi rejects calls that carry no credential, so present the pod
// certificate — the same client auth the base install gives ate-controller
// and atenet-router. ClientCredBundle re-reads the bundle on every handshake,
// so rotations are picked up without a restart.
func DialControl(endpoint string) (*grpc.ClientConn, ateapi.ControlClient, error) {
	k8sConfig, err := rest.InClusterConfig()
	if err != nil {
		loadingRules := clientcmd.NewDefaultClientConfigLoadingRules()
		configOverrides := &clientcmd.ConfigOverrides{}
		k8sConfig, err = clientcmd.NewNonInteractiveDeferredLoadingClientConfig(loadingRules, configOverrides).ClientConfig()
		if err != nil {
			return nil, nil, fmt.Errorf("k8s config: %w", err)
		}
	}
	k8sClient, err := kubernetes.NewForConfig(k8sConfig)
	if err != nil {
		return nil, nil, fmt.Errorf("k8s client: %w", err)
	}

	dialOpts, err := ateapiauth.DialOptions(ateapiauth.ClientConfig{
		K8sClient:        k8sClient,
		CAFile:           servicednsCAFile,
		ClientCredBundle: podIdentityCredBundle,
		ServerName:       ateapiServerName,
	})
	if err != nil {
		return nil, nil, fmt.Errorf("ateapi dial options: %w", err)
	}
	dialOpts = append(dialOpts, grpc.WithStatsHandler(otelgrpc.NewClientHandler()))

	conn, err := grpc.NewClient(endpoint, dialOpts...)
	if err != nil {
		return nil, nil, fmt.Errorf("dial %s: %w", endpoint, err)
	}
	return conn, ateapi.NewControlClient(conn), nil
}

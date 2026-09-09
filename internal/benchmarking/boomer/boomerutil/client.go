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
	"fmt"

	"github.com/agent-substrate/substrate/internal/ateapiauth"
	_ "github.com/agent-substrate/substrate/internal/k8sresolver"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

const (
	// ateapiCAFile verifies ateapi's servicedns serving cert;
	// ateapiCredBundle is the benchmark's own pod certificate. Both are
	// projected in by benchmarking/locust/manifests/locust.yaml.
	ateapiCAFile     = "/run/servicedns-ca/ca.crt"
	ateapiCredBundle = "/run/podidentity.podcert.ate.dev/credential-bundle.pem"

	// ateapiServerName is the DNS SAN on the apiserver's serving cert, which
	// is shorter than the endpoint the benchmark dials.
	ateapiServerName = "api.ate-system.svc"
)

// DialControl opens an authenticated gRPC connection to ateapi.
//
// ateapi rejects calls that carry no credential, so present the pod
// certificate — the same client auth the base install gives ate-controller
// and atenet-router. ClientCredBundle re-reads the bundle on every handshake,
// so rotations are picked up without a restart.
func DialControl(endpoint string) (*grpc.ClientConn, ateapipb.ControlClient, error) {
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
		CAFile:           ateapiCAFile,
		ClientCredBundle: ateapiCredBundle,
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
	return conn, ateapipb.NewControlClient(conn), nil
}

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
	"fmt"

	"go.ate.dev/substrate/internal/ateclient"
	"go.ate.dev/substrate/internal/portforward"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

// ServicePortForward port-forwards port of the Service namespace/name in the
// cluster under test and returns the local port. Call stop to tear it down.
func ServicePortForward(ctx context.Context, namespace, name string, port int32) (localPort int, stop func(), err error) {
	config, clientset, err := kubeClient()
	if err != nil {
		return 0, nil, err
	}
	return portforward.ServicePortForward(ctx, config, clientset, namespace, name, port)
}

// servicePodPortForward is ServicePortForward to containerPort of a ready pod
// behind the Service, for a port the Service does not publish.
func servicePodPortForward(ctx context.Context, namespace, name string, containerPort int32) (localPort int, stop func(), err error) {
	config, clientset, err := kubeClient()
	if err != nil {
		return 0, nil, err
	}
	return portforward.ServicePodPortForward(ctx, config, clientset, namespace, name, containerPort)
}

// kubeClient connects to the cluster under test.
func kubeClient() (*rest.Config, kubernetes.Interface, error) {
	config, err := ateclient.LoadKubeConfig(KubeConfig, KubeContext)
	if err != nil {
		return nil, nil, fmt.Errorf("loading kubeconfig: %w", err)
	}
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, nil, fmt.Errorf("creating k8s client: %w", err)
	}
	return config, clientset, nil
}

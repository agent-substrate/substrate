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
	"time"

	"github.com/agent-substrate/substrate/internal/ateclient"
	"github.com/agent-substrate/substrate/pkg/client/clientset/versioned"
	apiextensionsclientset "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	"k8s.io/client-go/kubernetes"
)

type Clients struct {
	K8s          *kubernetes.Clientset
	CRD          *apiextensionsclientset.Clientset
	SubstrateK8s *versioned.Clientset
	SubstrateAPI *ateclient.Client
}

var sharedClients *Clients

// GetClients returns the shared E2E clients.
// It panics if the clients have not been initialized.
func GetClients() *Clients {
	if sharedClients == nil {
		panic("sharedClients not initialized. RunTestMain must be used to initialize clients.")
	}
	return sharedClients
}

func NewClients(ctx context.Context) (*Clients, error) {
	// Kube API
	config, err := ateclient.LoadConfig(KubeConfig, KubeContext)
	if err != nil {
		return nil, fmt.Errorf("LoadConfig error %q %s: %w", KubeConfig, KubeContext, err)
	}

	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, fmt.Errorf("NewForConfig: %w", err)
	}

	crdClient, err := apiextensionsclientset.NewForConfig(config)
	if err != nil {
		return nil, fmt.Errorf("apiextensions clientset: %w", err)
	}

	substrateClient, err := versioned.NewForConfig(config)
	if err != nil {
		return nil, fmt.Errorf("substrate clientset: %w", err)
	}

	// Establish port-forward tunnel and connect to ATE API
	connectCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	apiClient, err := ateclient.NewClient(connectCtx, KubeConfig, KubeContext, "", false)
	if err != nil {
		return nil, fmt.Errorf("NewClient: %w", err)
	}

	return &Clients{
		K8s:          clientset,
		CRD:          crdClient,
		SubstrateK8s: substrateClient,
		SubstrateAPI: apiClient,
	}, nil
}

func (c *Clients) Close() {
	c.SubstrateAPI.Close()
}

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
	"context"
	"fmt"

	v1alpha1 "github.com/agent-substrate/substrate/pkg/api/v1alpha1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// atStore defines an interface for retrieving a collection of ActorTemplates.
type atStore interface {
	readyTemplates(ctx context.Context) ([]*v1alpha1.ActorTemplate, error)
}

// k8sATStore implements the atStore interface using a Kubernetes client.
type k8sATStore struct {
	k8sClient client.Client
}

func newk8sATStore(k8sClient client.Client) *k8sATStore {
	return &k8sATStore{
		k8sClient: k8sClient,
	}
}

func (t *k8sATStore) readyTemplates(ctx context.Context) ([]*v1alpha1.ActorTemplate, error) {
	var atList v1alpha1.ActorTemplateList
	if err := t.k8sClient.List(ctx, &atList); err != nil {
		return nil, fmt.Errorf("failed to list ActorTemplates: %w", err)
	}

	var templates []*v1alpha1.ActorTemplate
	for i := range atList.Items {
		if atList.Items[i].Status.Phase != v1alpha1.PhaseReady {
			continue
		}
		templates = append(templates, &atList.Items[i])
	}
	return templates, nil
}

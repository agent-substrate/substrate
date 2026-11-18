// Copyright 2026 The Agent Substrate Authors
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

package controllers

import (
	"fmt"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/selection"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// CacheOptions limits the manager cache to the egressMITMCAPool Secret and to
// Deployments that carry workerPoolLabel. Other kinds are cached in full.
func CacheOptions(egressMITMCAPool types.NamespacedName) (cache.Options, error) {
	hasWorkerPoolLabel, err := labels.NewRequirement(workerPoolLabel, selection.Exists, nil)
	if err != nil {
		return cache.Options{}, fmt.Errorf("failed to select Deployments by label %q: %w", workerPoolLabel, err)
	}
	return cache.Options{
		ByObject: map[client.Object]cache.ByObject{
			&corev1.Secret{}: {
				Namespaces: map[string]cache.Config{
					egressMITMCAPool.Namespace: {
						FieldSelector: fields.OneTermEqualSelector("metadata.name", egressMITMCAPool.Name),
					},
				},
			},
			&appsv1.Deployment{}: {Label: labels.NewSelector().Add(*hasWorkerPoolLabel)},
		},
	}, nil
}

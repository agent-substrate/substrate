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

package controllers

import (
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/fields"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// CacheOptions scopes workload resources to watchNamespace. An empty namespace
// watches all workloads. The CA pool stays in the system namespace, and
// cluster-scoped resources such as ClusterTrustBundles remain cluster-scoped.
func CacheOptions(watchNamespace, systemNamespace string) cache.Options {
	pool := EgressMITMCAPoolRef(systemNamespace)
	opts := cache.Options{
		ByObject: map[client.Object]cache.ByObject{
			&corev1.Secret{}: {
				Namespaces: map[string]cache.Config{
					pool.Namespace: {
						FieldSelector: fields.OneTermEqualSelector("metadata.name", pool.Name),
					},
				},
			},
		},
	}
	if watchNamespace != "" {
		opts.DefaultNamespaces = map[string]cache.Config{watchNamespace: {}}
	}
	return opts
}

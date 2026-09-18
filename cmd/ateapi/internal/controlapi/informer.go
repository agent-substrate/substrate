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

package controlapi

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
)

const (
	ateletNamespace = "ate-system"
	byNode          = "by-node"
)

// AteletInformer creates a SharedInformerFactory and SharedIndexInformer for Atelet pods.
func AteletInformer(kc kubernetes.Interface) (informers.SharedInformerFactory, cache.SharedIndexInformer) {
	factory := informers.NewSharedInformerFactoryWithOptions(kc, 0,
		informers.WithNamespace(ateletNamespace),
		informers.WithTweakListOptions(func(options *metav1.ListOptions) {
			options.LabelSelector = `app in (atelet)`
		}),
	)
	ateletInformer := factory.Core().V1().Pods().Informer()
	ateletInformer.AddIndexers(cache.Indexers{
		byNode: func(obj any) ([]string, error) {
			pod := obj.(*corev1.Pod)
			return []string{pod.Spec.NodeName}, nil
		},
	})
	return factory, ateletInformer
}

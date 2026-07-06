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
	"time"

	"github.com/agent-substrate/substrate/internal/egress"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/dynamic/dynamicinformer"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
)

const (
	ateletNamespace    = "ate-system"
	byNamespaceAndName = "by-namespace-and-name"
	byWorkerPool       = "by-worker-pool"
	byNode             = "by-node"
	workerPodLabel     = "ate.dev/worker-pool"
)

// GatewayGVR is the Gateway API resource ate-api watches for egress PEPs.
var GatewayGVR = schema.GroupVersionResource{
	Group:    "gateway.networking.k8s.io",
	Version:  "v1",
	Resource: "gateways",
}

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

// WorkerPodInformer creates a SharedInformerFactory and SharedIndexInformer for Worker pods.
func WorkerPodInformer(kc kubernetes.Interface) (informers.SharedInformerFactory, cache.SharedIndexInformer) {
	factory := informers.NewSharedInformerFactoryWithOptions(kc, 5*time.Minute,
		informers.WithTweakListOptions(func(options *metav1.ListOptions) {
			options.LabelSelector = "ate.dev/worker-pool"
		}),
	)
	workerPodInformer := factory.Core().V1().Pods().Informer()
	workerPodInformer.AddIndexers(cache.Indexers{
		byNamespaceAndName: func(obj any) ([]string, error) {
			pod := obj.(*corev1.Pod)
			key := pod.ObjectMeta.Namespace + "/" + pod.ObjectMeta.Name
			return []string{key}, nil
		},
		byWorkerPool: func(obj any) ([]string, error) {
			pod := obj.(*corev1.Pod)
			workerPoolRef := pod.ObjectMeta.Namespace + "/" + pod.ObjectMeta.Labels["ate.dev/worker-pool"]
			return []string{workerPoolRef}, nil
		},
	})

	return factory, workerPodInformer
}

// GatewayPEPInformer watches labeled Gateways that ate-api can use as egress PEPs.
//
// The watch is cluster-wide and trusts Gateway labels: RBAC on Gateways is the
// security boundary (see docs/egress-capture.md, "Trust model"). If Gateway
// write access can't be restricted to the platform team, scope this watch to
// an allowlist of PEP namespaces instead.
func GatewayPEPInformer(dc dynamic.Interface) (dynamicinformer.DynamicSharedInformerFactory, cache.SharedIndexInformer) {
	factory := dynamicinformer.NewFilteredDynamicSharedInformerFactory(dc, 0, metav1.NamespaceAll, func(options *metav1.ListOptions) {
		options.LabelSelector = egress.LabelPEP
	})
	gatewayInformer := factory.ForResource(GatewayGVR).Informer()
	return factory, gatewayInformer
}

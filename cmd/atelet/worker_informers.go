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

package main

import (
	"go.ate.dev/substrate/pkg/client/clientset/versioned"
	"go.ate.dev/substrate/pkg/client/informers/externalversions"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kubeinformers "k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
)

func newWorkerInformers(ateClient versioned.Interface, k8sClient kubernetes.Interface, namespace, nodeName string) (externalversions.SharedInformerFactory, kubeinformers.SharedInformerFactory) {
	ateFactory := externalversions.NewSharedInformerFactoryWithOptions(ateClient, 0, externalversions.WithNamespace(namespace))
	// The node selector keeps LIST/WATCH costs proportional to this node's workers.
	podFactory := kubeinformers.NewSharedInformerFactoryWithOptions(k8sClient, 0,
		kubeinformers.WithNamespace(namespace),
		kubeinformers.WithTweakListOptions(func(o *metav1.ListOptions) {
			o.FieldSelector = "spec.nodeName=" + nodeName
			o.LabelSelector = workerPoolLabel
		}))
	return ateFactory, podFactory
}

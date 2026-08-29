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

package workersync

import (
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
)

// WorkerPodInformer creates a SharedInformerFactory and SharedIndexInformer for
// worker pods. The resync period re-delivers every cached pod as an update, so
// a registry record that drifted without a corresponding pod event is repaired
// on the next sweep.
func WorkerPodInformer(kc kubernetes.Interface) (informers.SharedInformerFactory, cache.SharedIndexInformer) {
	factory := informers.NewSharedInformerFactoryWithOptions(kc, 5*time.Minute,
		informers.WithTweakListOptions(func(options *metav1.ListOptions) {
			options.LabelSelector = workerPodLabel
		}),
	)
	return factory, factory.Core().V1().Pods().Informer()
}

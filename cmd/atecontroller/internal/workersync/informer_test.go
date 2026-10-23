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
	"context"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
)

func TestWorkerPodInformerNamespace(t *testing.T) {
	for _, namespace := range []string{"agent-workloads", ""} {
		t.Run("namespace="+namespace, func(t *testing.T) {
			client := fake.NewClientset()
			actions := make(chan k8stesting.Action, 10)
			client.PrependReactor("list", "pods", func(action k8stesting.Action) (bool, runtime.Object, error) {
				actions <- action
				return false, nil, nil
			})
			client.PrependWatchReactor("pods", func(action k8stesting.Action) (bool, watch.Interface, error) {
				actions <- action
				return false, nil, nil
			})
			factory, _ := WorkerPodInformer(client, namespace)
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			t.Cleanup(func() {
				cancel()
				factory.Shutdown()
			})
			factory.Start(ctx.Done())
			for _, verb := range []string{"list", "watch"} {
				select {
				case action := <-actions:
					if action.GetVerb() != verb || action.GetNamespace() != namespace {
						t.Fatalf("requested %s pods in namespace %q, want %s in %q", action.GetVerb(), action.GetNamespace(), verb, namespace)
					}
					var selector string
					switch action := action.(type) {
					case k8stesting.ListAction:
						selector = action.GetListRestrictions().Labels.String()
					case k8stesting.WatchAction:
						selector = action.GetWatchRestrictions().Labels.String()
					}
					if selector != workerPodLabel {
						t.Fatalf("%s selector = %q, want %q", verb, selector, workerPodLabel)
					}
				case <-ctx.Done():
					t.Fatalf("waiting for %s: %v", verb, ctx.Err())
				}
			}
		})
	}
}

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
	"context"
	"testing"
	"time"

	atev1 "go.ate.dev/substrate/api/infra/k8s/ate/v1"
	atefake "go.ate.dev/substrate/pkg/client/clientset/versioned/fake"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	k8swatch "k8s.io/apimachinery/pkg/watch"
	kubefake "k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/cache"
)

func TestWorkerInformersNamespace(t *testing.T) {
	for _, namespace := range []string{"workers", ""} {
		t.Run("namespace="+namespace, func(t *testing.T) {
			kube := kubefake.NewClientset(workerPod("local", "pool"), &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{Name: "other", Namespace: "other", Labels: map[string]string{workerPoolLabel: "pool"}},
			})
			ate := atefake.NewSimpleClientset(resolvedPool("pool", "config"),
				&atev1.WorkerPool{ObjectMeta: metav1.ObjectMeta{Name: "pool", Namespace: "other"}},
				&atev1.SandboxConfig{ObjectMeta: metav1.ObjectMeta{Name: "config"}},
			)
			actions := make(chan k8stesting.Action, 10)
			for _, client := range []*k8stesting.Fake{&kube.Fake, &ate.Fake} {
				client.PrependReactor("list", "*", func(action k8stesting.Action) (bool, runtime.Object, error) {
					actions <- action
					return false, nil, nil
				})
				client.PrependWatchReactor("*", func(action k8stesting.Action) (bool, k8swatch.Interface, error) {
					actions <- action
					return false, nil, nil
				})
			}
			ateFactory, podFactory := newWorkerInformers(ate, kube, namespace, "node-1")
			pods := podFactory.Core().V1().Pods().Informer()
			pools := ateFactory.Ate().V1().WorkerPools().Informer()
			configs := ateFactory.Ate().V1().SandboxConfigs().Informer()
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			t.Cleanup(func() {
				cancel()
				podFactory.Shutdown()
				ateFactory.Shutdown()
			})
			podFactory.Start(ctx.Done())
			ateFactory.Start(ctx.Done())
			seen := make(map[string]bool)
			for range 6 {
				select {
				case action := <-actions:
					resource := action.GetResource().Resource
					key := action.GetVerb() + " " + resource
					if seen[key] {
						t.Fatalf("duplicate request: %s", key)
					}
					seen[key] = true
					wantNamespace := namespace
					if resource == "sandboxconfigs" {
						wantNamespace = ""
					}
					if action.GetNamespace() != wantNamespace {
						t.Errorf("%s namespace = %q, want %q", key, action.GetNamespace(), wantNamespace)
					}
					if resource == "pods" {
						var fields, labels string
						switch action := action.(type) {
						case k8stesting.ListAction:
							fields = action.GetListRestrictions().Fields.String()
							labels = action.GetListRestrictions().Labels.String()
						case k8stesting.WatchAction:
							fields = action.GetWatchRestrictions().Fields.String()
							labels = action.GetWatchRestrictions().Labels.String()
						}
						if fields != "spec.nodeName=node-1" || labels != workerPoolLabel {
							t.Errorf("%s selectors = %q, %q, want node-1 and %s", key, fields, labels, workerPoolLabel)
						}
					}
				case <-ctx.Done():
					t.Fatalf("waiting for LIST/WATCH requests: %v (seen %v)", ctx.Err(), seen)
				}
			}
			if !cache.WaitForCacheSync(ctx.Done(), pods.HasSynced, pools.HasSynced, configs.HasSynced) {
				t.Fatal("worker caches did not sync")
			}
			want := 1
			if namespace == "" {
				want = 2
			}
			for name, informer := range map[string]cache.SharedIndexInformer{"Pods": pods, "WorkerPools": pools} {
				if got := len(informer.GetStore().List()); got != want {
					t.Errorf("%s cache has %d objects, want %d", name, got, want)
				}
			}
			if _, exists, err := configs.GetStore().GetByKey("config"); err != nil || !exists {
				t.Fatalf("cluster-scoped SandboxConfig missing: exists=%v, err=%v", exists, err)
			}
		})
	}
}

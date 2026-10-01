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
	"errors"
	"testing"

	atev1alpha1 "github.com/agent-substrate/substrate/pkg/api/v1alpha1"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
)

func TestSyncer_RejectsInvalidPodOwnership(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*corev1.Pod, *appsv1.ReplicaSet, *appsv1.Deployment, *atev1alpha1.WorkerPool)
	}{
		{"label only", func(p *corev1.Pod, _ *appsv1.ReplicaSet, _ *appsv1.Deployment, _ *atev1alpha1.WorkerPool) {
			p.OwnerReferences = nil
		}},
		{"empty pool label", func(p *corev1.Pod, _ *appsv1.ReplicaSet, _ *appsv1.Deployment, _ *atev1alpha1.WorkerPool) {
			p.Labels[workerPodLabel] = ""
		}},
		{"pod has a non-controller owner", func(p *corev1.Pod, _ *appsv1.ReplicaSet, _ *appsv1.Deployment, _ *atev1alpha1.WorkerPool) {
			p.OwnerReferences[0].Controller = nil
		}},
		{"pod owner has wrong kind", func(p *corev1.Pod, _ *appsv1.ReplicaSet, _ *appsv1.Deployment, _ *atev1alpha1.WorkerPool) {
			p.OwnerReferences[0].Kind = "Deployment"
		}},
		{"pod owner has wrong API version", func(p *corev1.Pod, _ *appsv1.ReplicaSet, _ *appsv1.Deployment, _ *atev1alpha1.WorkerPool) {
			p.OwnerReferences[0].APIVersion = "rogue.example/v1"
		}},
		{"pod owner has empty UID", func(p *corev1.Pod, _ *appsv1.ReplicaSet, _ *appsv1.Deployment, _ *atev1alpha1.WorkerPool) {
			p.OwnerReferences[0].UID = ""
		}},
		{"pod owner has empty name", func(p *corev1.Pod, _ *appsv1.ReplicaSet, _ *appsv1.Deployment, _ *atev1alpha1.WorkerPool) {
			p.OwnerReferences[0].Name = ""
		}},
		{"missing ReplicaSet", func(p *corev1.Pod, _ *appsv1.ReplicaSet, _ *appsv1.Deployment, _ *atev1alpha1.WorkerPool) {
			p.OwnerReferences[0].Name = "missing"
		}},
		{"recreated ReplicaSet", func(_ *corev1.Pod, rs *appsv1.ReplicaSet, _ *appsv1.Deployment, _ *atev1alpha1.WorkerPool) {
			rs.UID = "replacement"
		}},
		{"ReplicaSet in another namespace", func(_ *corev1.Pod, rs *appsv1.ReplicaSet, _ *appsv1.Deployment, _ *atev1alpha1.WorkerPool) {
			rs.Namespace = "other"
		}},
		{"ReplicaSet without owner", func(_ *corev1.Pod, rs *appsv1.ReplicaSet, _ *appsv1.Deployment, _ *atev1alpha1.WorkerPool) {
			rs.OwnerReferences = nil
		}},
		{"ReplicaSet has a non-controller owner", func(_ *corev1.Pod, rs *appsv1.ReplicaSet, _ *appsv1.Deployment, _ *atev1alpha1.WorkerPool) {
			*rs.OwnerReferences[0].Controller = false
		}},
		{"ReplicaSet owner has wrong kind", func(_ *corev1.Pod, rs *appsv1.ReplicaSet, _ *appsv1.Deployment, _ *atev1alpha1.WorkerPool) {
			rs.OwnerReferences[0].Kind = "StatefulSet"
		}},
		{"ReplicaSet owner has wrong API version", func(_ *corev1.Pod, rs *appsv1.ReplicaSet, _ *appsv1.Deployment, _ *atev1alpha1.WorkerPool) {
			rs.OwnerReferences[0].APIVersion = "rogue.example/v1"
		}},
		{"ReplicaSet owner has empty UID", func(_ *corev1.Pod, rs *appsv1.ReplicaSet, _ *appsv1.Deployment, _ *atev1alpha1.WorkerPool) {
			rs.OwnerReferences[0].UID = ""
		}},
		{"different Deployment", func(_ *corev1.Pod, rs *appsv1.ReplicaSet, dep *appsv1.Deployment, _ *atev1alpha1.WorkerPool) {
			dep.Name = "rogue"
			rs.OwnerReferences[0].Name = dep.Name
		}},
		{"missing Deployment", func(_ *corev1.Pod, _ *appsv1.ReplicaSet, dep *appsv1.Deployment, _ *atev1alpha1.WorkerPool) {
			dep.Name = "other"
		}},
		{"recreated Deployment", func(_ *corev1.Pod, _ *appsv1.ReplicaSet, dep *appsv1.Deployment, _ *atev1alpha1.WorkerPool) {
			dep.UID = "replacement"
		}},
		{"Deployment in another namespace", func(_ *corev1.Pod, _ *appsv1.ReplicaSet, dep *appsv1.Deployment, _ *atev1alpha1.WorkerPool) {
			dep.Namespace = "other"
		}},
		{"Deployment without owner", func(_ *corev1.Pod, _ *appsv1.ReplicaSet, dep *appsv1.Deployment, _ *atev1alpha1.WorkerPool) {
			dep.OwnerReferences = nil
		}},
		{"Deployment has a non-controller owner", func(_ *corev1.Pod, _ *appsv1.ReplicaSet, dep *appsv1.Deployment, _ *atev1alpha1.WorkerPool) {
			dep.OwnerReferences[0].Controller = nil
		}},
		{"Deployment owner has wrong kind", func(_ *corev1.Pod, _ *appsv1.ReplicaSet, dep *appsv1.Deployment, _ *atev1alpha1.WorkerPool) {
			dep.OwnerReferences[0].Kind = "ActorTemplate"
		}},
		{"Deployment owner has wrong API version", func(_ *corev1.Pod, _ *appsv1.ReplicaSet, dep *appsv1.Deployment, _ *atev1alpha1.WorkerPool) {
			dep.OwnerReferences[0].APIVersion = "rogue.example/v1"
		}},
		{"different WorkerPool", func(_ *corev1.Pod, _ *appsv1.ReplicaSet, dep *appsv1.Deployment, _ *atev1alpha1.WorkerPool) {
			dep.OwnerReferences[0].Name = "other"
		}},
		{"recreated WorkerPool", func(_ *corev1.Pod, _ *appsv1.ReplicaSet, _ *appsv1.Deployment, pool *atev1alpha1.WorkerPool) {
			pool.UID = "replacement"
		}},
		{"WorkerPool with empty UID", func(_ *corev1.Pod, _ *appsv1.ReplicaSet, dep *appsv1.Deployment, pool *atev1alpha1.WorkerPool) {
			pool.UID = ""
			dep.OwnerReferences[0].UID = ""
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			pool := workerPool("ns-ownership", "pool", "gvisor", nil)
			pod := workerPod(pool.Namespace, "worker", pool.Name, testPodUID, "10.0.0.1")
			rs, dep := workerReplicaSet(pool), workerDeployment(pool)
			tc.mutate(pod, rs, dep, pool)
			api := newFakeControl()
			s, pods, _ := setupReconcileTest(t, api, pool)
			kube := fake.NewClientset(rs, dep)
			s.apps = kube.AppsV1()
			key := seedPod(t, pods, pod)

			mustReconcile(t, t.Context(), s, key)
			if got := api.names(); len(got) != 0 {
				t.Fatalf("unowned pod registered as %v", got)
			}

			// Registrations from before ownership validation must stop receiving
			// new actors without inheriting labels or an epoch from a rogue pod.
			w := registeredWorker(pool.Namespace, pool.Name, pod.Name, testPodUID, "10.0.0.1")
			w.Epoch = 1
			w.Labels = map[string]string{"original": "true"}
			api.put(w)
			pod = withAteomRestarts(pod.DeepCopy(), 3)
			seedPod(t, pods, pod)
			mustReconcile(t, t.Context(), s, key)
			got := api.get(testPodUID)
			if got.GetStatus().GetState() != ateapipb.WorkerState_WORKER_STATE_DRAINING {
				t.Errorf("unowned worker state = %v, want DRAINING", got.GetStatus().GetState())
			}
			if got.GetEpoch() != 1 || got.GetLabels()["original"] != "true" {
				t.Errorf("unowned worker was updated: %v", got)
			}
		})
	}
}

func TestSyncer_OwnershipLookupRetries(t *testing.T) {
	for _, resource := range []string{"replicasets", "deployments"} {
		t.Run(resource, func(t *testing.T) {
			pool := workerPool("ns-ownership-retry", "pool", "gvisor", nil)
			api := newFakeControl()
			s, pods, _ := setupReconcileTest(t, api, pool)
			kube := fake.NewClientset(workerReplicaSet(pool), workerDeployment(pool))
			lookupErr := errors.New("API unavailable")
			kube.PrependReactor("get", resource, func(k8stesting.Action) (bool, runtime.Object, error) {
				return true, nil, lookupErr
			})
			s.apps = kube.AppsV1()
			key := seedPod(t, pods, workerPod(pool.Namespace, "worker", pool.Name, testPodUID, "10.0.0.1"))
			if err := s.reconcile(t.Context(), key); !errors.Is(err, lookupErr) {
				t.Fatalf("reconcile error = %v, want %v for retry", err, lookupErr)
			}
			if got := api.names(); len(got) != 0 {
				t.Fatalf("registered worker before ownership could be checked: %v", got)
			}
			kube.ReactionChain = kube.ReactionChain[1:]
			mustReconcile(t, t.Context(), s, key)
			if api.get(testPodUID) == nil {
				t.Fatal("worker was not registered after ownership lookup recovered")
			}
		})
	}
}

func TestSyncer_OwnershipDuringRollout(t *testing.T) {
	pool := workerPool("ns-ownership-rollout", "pool", "gvisor", nil)
	api := newFakeControl()
	s, pods, _ := setupReconcileTest(t, api, pool)
	for i, uid := range []string{testPodUID, otherPodUID} {
		rs := workerReplicaSet(pool)
		rs.Name += "-" + uid
		rs.UID = rs.UID + "-" + types.UID(uid)
		if _, err := s.apps.ReplicaSets(pool.Namespace).Create(t.Context(), rs, metav1.CreateOptions{}); err != nil {
			t.Fatal(err)
		}
		pod := workerPod(pool.Namespace, rs.Name+"-pod", pool.Name, uid, "10.0.0.1")
		pod.OwnerReferences = []metav1.OwnerReference{*metav1.NewControllerRef(rs, appsv1.SchemeGroupVersion.WithKind("ReplicaSet"))}
		mustReconcile(t, t.Context(), s, seedPod(t, pods, pod))
		if got := len(api.names()); got != i+1 {
			t.Fatalf("registered %d workers, want %d from distinct ReplicaSets", got, i+1)
		}
	}
}

func TestSyncer_UnownedTerminalPodIsNotDeleted(t *testing.T) {
	pool := workerPool("ns-ownership-terminal", "pool", "gvisor", nil)
	pod := workerPod(pool.Namespace, "rogue", pool.Name, testPodUID, "10.0.0.1")
	pod.OwnerReferences = nil
	pod.Status.Phase = corev1.PodFailed
	api := newFakeControl()
	s, pods, _ := setupReconcileTest(t, api, pool)
	kube := fake.NewClientset(pod)
	s.pods = kube.CoreV1()
	mustReconcile(t, t.Context(), s, seedPod(t, pods, pod))
	if _, err := kube.CoreV1().Pods(pool.Namespace).Get(t.Context(), pod.Name, metav1.GetOptions{}); err != nil {
		t.Fatalf("unowned terminal pod should be left alone: %v", err)
	}
}

// Copyright 2026 Google LLC
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
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	k8errors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/util/retry"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	"github.com/agent-substrate/substrate/internal/envtestbins"
	atev1alpha1 "github.com/agent-substrate/substrate/pkg/api/v1alpha1"
)

var (
	testEnv    *envtest.Environment
	cfg        *rest.Config
	k8sClient  client.Client
	testCtx    context.Context
	testCancel context.CancelFunc
)

func TestMain(m *testing.M) {
	binaryAssetsDirectory, err := envtestbins.BinaryAssetsDir()
	if err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(1)
	}

	testEnv = &envtest.Environment{
		CRDDirectoryPaths:     []string{"../../../../manifests/ate-install/generated"},
		BinaryAssetsDirectory: binaryAssetsDirectory,
	}

	cfg, err = testEnv.Start()
	if err != nil {
		fmt.Fprintf(os.Stderr, "envtest start failed: %v\n", err)
		os.Exit(1)
	}

	scheme := runtime.NewScheme()
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(atev1alpha1.AddToScheme(scheme))

	k8sClient, err = client.New(cfg, client.Options{Scheme: scheme})
	if err != nil {
		fmt.Fprintf(os.Stderr, "k8s client creation failed: %v\n", err)
		os.Exit(1)
	}

	mgr, err := ctrl.NewManager(cfg, ctrl.Options{
		Scheme:                 scheme,
		Metrics:                metricsserver.Options{BindAddress: "0"},
		HealthProbeBindAddress: "0",
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "manager creation failed: %v\n", err)
		os.Exit(1)
	}

	if err := (&WorkerPoolReconciler{
		Client: mgr.GetClient(),
		Scheme: mgr.GetScheme(),
	}).SetupWithManager(mgr); err != nil {
		fmt.Fprintf(os.Stderr, "controller setup failed: %v\n", err)
		os.Exit(1)
	}

	testCtx, testCancel = context.WithCancel(context.Background())
	go func() {
		_ = mgr.Start(testCtx)
	}()

	code := m.Run()

	testCancel()
	_ = testEnv.Stop()
	os.Exit(code)
}

// TestWorkerPoolCreatesDeployment verifies that creating a WorkerPool causes
// the controller to create a correctly-configured Deployment.
func TestWorkerPoolCreatesDeployment(t *testing.T) {
	wp := makeWorkerPool("test-create", "default", 3, "ateom:v1")
	if err := k8sClient.Create(testCtx, wp); err != nil {
		t.Fatalf("create WorkerPool: %v", err)
	}
	t.Cleanup(func() { k8sClient.Delete(testCtx, wp) }) //nolint:errcheck

	eventually(t, func(ctx context.Context) (bool, error) {
		dep, err := getDeployment(ctx, wp)
		if err != nil {
			return false, nil
		}
		if dep.Spec.Replicas == nil || *dep.Spec.Replicas != 3 {
			return false, nil
		}
		if len(dep.Spec.Template.Spec.Containers) == 0 {
			return false, nil
		}
		container := dep.Spec.Template.Spec.Containers[0]
		if container.Image != "ateom:v1" || container.Name != "ateom" {
			return false, nil
		}
		if dep.Spec.Template.Labels["ate.dev/worker-pool"] != wp.Name {
			return false, nil
		}
		if len(dep.OwnerReferences) == 0 || dep.OwnerReferences[0].Name != wp.Name {
			return false, nil
		}
		return len(dep.Spec.Template.Spec.Volumes) == 1 &&
			dep.Spec.Template.Spec.Volumes[0].Name == "run-ateom", nil
	})
}

// TestWorkerPoolReplicasUpdate verifies that changing spec.replicas on a
// WorkerPool propagates to the managed Deployment.
func TestWorkerPoolReplicasUpdate(t *testing.T) {
	wp := makeWorkerPool("test-replicas", "default", 2, "ateom:v1")
	if err := k8sClient.Create(testCtx, wp); err != nil {
		t.Fatalf("create WorkerPool: %v", err)
	}
	t.Cleanup(func() { k8sClient.Delete(testCtx, wp) }) //nolint:errcheck

	eventually(t, func(ctx context.Context) (bool, error) {
		_, err := getDeployment(ctx, wp)
		return err == nil, nil
	})

	if err := k8sClient.Get(testCtx, types.NamespacedName{Name: wp.Name, Namespace: wp.Namespace}, wp); err != nil {
		t.Fatalf("re-fetch WorkerPool: %v", err)
	}
	wp.Spec.Replicas = 5
	if err := k8sClient.Update(testCtx, wp); err != nil {
		t.Fatalf("update WorkerPool replicas: %v", err)
	}

	eventually(t, func(ctx context.Context) (bool, error) {
		dep, err := getDeployment(ctx, wp)
		if err != nil {
			return false, nil
		}
		return dep.Spec.Replicas != nil && *dep.Spec.Replicas == 5, nil
	})
}

// TestWorkerPoolImageUpdate verifies that changing spec.ateomImage on a
// WorkerPool propagates to the managed Deployment.
func TestWorkerPoolImageUpdate(t *testing.T) {
	wp := makeWorkerPool("test-image", "default", 1, "ateom:v1")
	if err := k8sClient.Create(testCtx, wp); err != nil {
		t.Fatalf("create WorkerPool: %v", err)
	}
	t.Cleanup(func() { k8sClient.Delete(testCtx, wp) }) //nolint:errcheck

	eventually(t, func(ctx context.Context) (bool, error) {
		_, err := getDeployment(ctx, wp)
		return err == nil, nil
	})

	if err := k8sClient.Get(testCtx, types.NamespacedName{Name: wp.Name, Namespace: wp.Namespace}, wp); err != nil {
		t.Fatalf("re-fetch WorkerPool: %v", err)
	}
	wp.Spec.AteomImage = "ateom:v2"
	if err := k8sClient.Update(testCtx, wp); err != nil {
		t.Fatalf("update WorkerPool image: %v", err)
	}

	eventually(t, func(ctx context.Context) (bool, error) {
		dep, err := getDeployment(ctx, wp)
		if err != nil || len(dep.Spec.Template.Spec.Containers) == 0 {
			return false, nil
		}
		return dep.Spec.Template.Spec.Containers[0].Image == "ateom:v2", nil
	})
}

// TestSSAPreservesUnownedFields verifies that SSA leaves fields set by other
// field managers untouched during reconciliation.
func TestSSAPreservesUnownedFields(t *testing.T) {
	wp := makeWorkerPool("test-ssa-unowned", "default", 2, "ateom:v1")
	if err := k8sClient.Create(testCtx, wp); err != nil {
		t.Fatalf("create WorkerPool: %v", err)
	}
	t.Cleanup(func() { k8sClient.Delete(testCtx, wp) }) //nolint:errcheck

	eventually(t, func(ctx context.Context) (bool, error) {
		_, err := getDeployment(ctx, wp)
		return err == nil, nil
	})

	// An external manager sets revisionHistoryLimit — a field the controller
	// never declares in its apply config.
	revisionHistoryLimit := int32(7)
	updateDeploymentSpec(t, wp, "set revisionHistoryLimit", func(dep *appsv1.Deployment) {
		dep.Spec.RevisionHistoryLimit = &revisionHistoryLimit
	})

	// The Deployment update triggers a reconcile via Owns(). Wait until the
	// reconcile has run (replicas still correct) and the field is still present.
	eventually(t, func(ctx context.Context) (bool, error) {
		d, err := getDeployment(ctx, wp)
		if err != nil {
			return false, nil
		}
		return d.Spec.Replicas != nil && *d.Spec.Replicas == 2 &&
			d.Spec.RevisionHistoryLimit != nil && *d.Spec.RevisionHistoryLimit == 7, nil
	})
}

// TestSSARevertsOwnedFields verifies that if an external actor changes a field
// owned by the workerpool-controller (e.g. replicas on the Deployment), the
// controller reverts it on the next reconcile.
func TestSSARevertsOwnedFields(t *testing.T) {
	wp := makeWorkerPool("test-ssa-owned", "default", 2, "ateom:v1")
	if err := k8sClient.Create(testCtx, wp); err != nil {
		t.Fatalf("create WorkerPool: %v", err)
	}
	t.Cleanup(func() { k8sClient.Delete(testCtx, wp) }) //nolint:errcheck

	eventually(t, func(ctx context.Context) (bool, error) {
		dep, err := getDeployment(ctx, wp)
		return err == nil && dep.Spec.Replicas != nil && *dep.Spec.Replicas == 2, nil
	})

	rogueReplicas := int32(99)
	updateDeploymentSpec(t, wp, "rogue update", func(dep *appsv1.Deployment) {
		dep.Spec.Replicas = &rogueReplicas
	})

	// The controller re-applies with ForceOwnership, reclaiming replicas.
	eventually(t, func(ctx context.Context) (bool, error) {
		d, err := getDeployment(ctx, wp)
		if err != nil {
			return false, nil
		}
		return d.Spec.Replicas != nil && *d.Spec.Replicas == 2, nil
	})
}

// TestDeletedDeploymentRecreated verifies that if the managed Deployment is
// deleted externally, the controller recreates it.
func TestDeletedDeploymentRecreated(t *testing.T) {
	wp := makeWorkerPool("test-recreate", "default", 2, "ateom:v1")
	if err := k8sClient.Create(testCtx, wp); err != nil {
		t.Fatalf("create WorkerPool: %v", err)
	}
	t.Cleanup(func() { k8sClient.Delete(testCtx, wp) }) //nolint:errcheck

	eventually(t, func(ctx context.Context) (bool, error) {
		_, err := getDeployment(ctx, wp)
		return err == nil, nil
	})

	dep, err := getDeployment(testCtx, wp)
	if err != nil {
		t.Fatalf("get Deployment: %v", err)
	}
	if err := k8sClient.Delete(testCtx, dep); err != nil {
		t.Fatalf("delete Deployment: %v", err)
	}

	eventually(t, func(ctx context.Context) (bool, error) {
		_, err := getDeployment(ctx, wp)
		return err == nil, nil
	})
}

// TestStatusReplicasPropagation verifies that the controller syncs the
// Deployment's status.replicas into WorkerPool.status.replicas.
func TestStatusReplicasPropagation(t *testing.T) {
	wp := makeWorkerPool("test-status", "default", 3, "ateom:v1")
	if err := k8sClient.Create(testCtx, wp); err != nil {
		t.Fatalf("create WorkerPool: %v", err)
	}
	t.Cleanup(func() { k8sClient.Delete(testCtx, wp) }) //nolint:errcheck

	eventually(t, func(ctx context.Context) (bool, error) {
		_, err := getDeployment(ctx, wp)
		return err == nil, nil
	})

	// Simulate the deployment controller reporting 3 running pods.
	updateDeploymentStatus(t, wp, "patch Deployment status", func(dep *appsv1.Deployment) {
		dep.Status.Replicas = 3
	})

	eventually(t, func(ctx context.Context) (bool, error) {
		current := &atev1alpha1.WorkerPool{}
		if err := k8sClient.Get(ctx, types.NamespacedName{Name: wp.Name, Namespace: wp.Namespace}, current); err != nil {
			return false, nil
		}
		return current.Status.Replicas == 3, nil
	})
}

func sampleWorkerPoolPodTemplate() *atev1alpha1.WorkerPoolPodTemplate {
	return &atev1alpha1.WorkerPoolPodTemplate{
		NodeSelector: map[string]string{
			"workload": "substrate",
		},
		Tolerations: []corev1.Toleration{{
			Key:      "nvidia.com/gpu",
			Operator: corev1.TolerationOpExists,
			Effect:   corev1.TaintEffectNoSchedule,
		}},
		PriorityClassName: "substrate-workers",
		NodeAffinity: &corev1.NodeAffinity{
			RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
				NodeSelectorTerms: []corev1.NodeSelectorTerm{{
					MatchExpressions: []corev1.NodeSelectorRequirement{{
						Key:      "workload",
						Operator: corev1.NodeSelectorOpIn,
						Values:   []string{"substrate"},
					}},
				}},
			},
		},
		Resources: &corev1.ResourceRequirements{
			Requests: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("500m"),
				corev1.ResourceMemory: resource.MustParse("1Gi"),
			},
			Limits: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("1"),
				corev1.ResourceMemory: resource.MustParse("2Gi"),
			},
		},
	}
}

// TestWorkerPoolPodTemplatePropagation verifies that template fields propagate
// to the managed Deployment pod template.
func TestWorkerPoolPodTemplatePropagation(t *testing.T) {
	wp := makeWorkerPool("test-template-propagate", "default", 1, "ateom:v1")
	wp.Spec.Template = sampleWorkerPoolPodTemplate()
	if err := k8sClient.Create(testCtx, wp); err != nil {
		t.Fatalf("create WorkerPool: %v", err)
	}
	t.Cleanup(func() { k8sClient.Delete(testCtx, wp) }) //nolint:errcheck

	eventually(t, func(ctx context.Context) (bool, error) {
		dep, err := getDeployment(ctx, wp)
		if err != nil || len(dep.Spec.Template.Spec.Containers) == 0 {
			return false, nil
		}
		podSpec := dep.Spec.Template.Spec
		container := podSpec.Containers[0]

		if podSpec.NodeSelector["workload"] != "substrate" {
			return false, nil
		}
		if len(podSpec.Tolerations) != 1 || podSpec.Tolerations[0].Key != "nvidia.com/gpu" {
			return false, nil
		}
		if podSpec.PriorityClassName != "substrate-workers" {
			return false, nil
		}
		if podSpec.Affinity == nil || podSpec.Affinity.NodeAffinity == nil {
			return false, nil
		}
		return container.Resources.Requests.Cpu().String() == "500m" &&
			container.Resources.Requests.Memory().String() == "1Gi" &&
			container.Resources.Limits.Cpu().String() == "1" &&
			container.Resources.Limits.Memory().String() == "2Gi", nil
	})
}

// TestWorkerPoolPodTemplateUpdate verifies that changing template fields on a
// WorkerPool propagates to the managed Deployment.
func TestWorkerPoolPodTemplateUpdate(t *testing.T) {
	wp := makeWorkerPool("test-template-update", "default", 1, "ateom:v1")
	wp.Spec.Template = sampleWorkerPoolPodTemplate()
	if err := k8sClient.Create(testCtx, wp); err != nil {
		t.Fatalf("create WorkerPool: %v", err)
	}
	t.Cleanup(func() { k8sClient.Delete(testCtx, wp) }) //nolint:errcheck

	eventually(t, func(ctx context.Context) (bool, error) {
		dep, err := getDeployment(ctx, wp)
		return err == nil && dep.Spec.Template.Spec.NodeSelector["workload"] == "substrate", nil
	})

	if err := k8sClient.Get(testCtx, types.NamespacedName{Name: wp.Name, Namespace: wp.Namespace}, wp); err != nil {
		t.Fatalf("re-fetch WorkerPool: %v", err)
	}
	wp.Spec.Template.NodeSelector = map[string]string{"workload": "updated"}
	if err := k8sClient.Update(testCtx, wp); err != nil {
		t.Fatalf("update WorkerPool template: %v", err)
	}

	eventually(t, func(ctx context.Context) (bool, error) {
		dep, err := getDeployment(ctx, wp)
		if err != nil || len(dep.Spec.Template.Spec.Containers) == 0 {
			return false, nil
		}
		podSpec := dep.Spec.Template.Spec
		return podSpec.NodeSelector["workload"] == "updated" &&
			podSpec.Containers[0].Resources.Requests.Cpu().String() == "500m", nil
	})
}

// TestWorkerPoolPodTemplateClear verifies that clearing template.nodeSelector
// removes it from the managed Deployment.
func TestWorkerPoolPodTemplateClear(t *testing.T) {
	wp := makeWorkerPool("test-template-clear", "default", 1, "ateom:v1")
	wp.Spec.Template = sampleWorkerPoolPodTemplate()
	if err := k8sClient.Create(testCtx, wp); err != nil {
		t.Fatalf("create WorkerPool: %v", err)
	}
	t.Cleanup(func() { k8sClient.Delete(testCtx, wp) }) //nolint:errcheck

	eventually(t, func(ctx context.Context) (bool, error) {
		dep, err := getDeployment(ctx, wp)
		return err == nil && dep.Spec.Template.Spec.NodeSelector["workload"] == "substrate", nil
	})

	if err := k8sClient.Get(testCtx, types.NamespacedName{Name: wp.Name, Namespace: wp.Namespace}, wp); err != nil {
		t.Fatalf("re-fetch WorkerPool: %v", err)
	}
	wp.Spec.Template.NodeSelector = nil
	if err := k8sClient.Update(testCtx, wp); err != nil {
		t.Fatalf("clear WorkerPool nodeSelector: %v", err)
	}

	eventually(t, func(ctx context.Context) (bool, error) {
		dep, err := getDeployment(ctx, wp)
		if err != nil {
			return false, nil
		}
		return len(dep.Spec.Template.Spec.NodeSelector) == 0, nil
	})
}

// TestWorkerPoolPodTemplateClearAll verifies that removing spec.template clears
// all pod template fields owned by the workerpool-controller.
func TestWorkerPoolPodTemplateClearAll(t *testing.T) {
	wp := makeWorkerPool("test-template-clear-all", "default", 1, "ateom:v1")
	wp.Spec.Template = sampleWorkerPoolPodTemplate()
	if err := k8sClient.Create(testCtx, wp); err != nil {
		t.Fatalf("create WorkerPool: %v", err)
	}
	t.Cleanup(func() { k8sClient.Delete(testCtx, wp) }) //nolint:errcheck

	eventually(t, func(ctx context.Context) (bool, error) {
		dep, err := getDeployment(ctx, wp)
		if err != nil || len(dep.Spec.Template.Spec.Containers) == 0 {
			return false, nil
		}
		podSpec := dep.Spec.Template.Spec
		container := podSpec.Containers[0]
		return podSpec.NodeSelector["workload"] == "substrate" &&
			len(podSpec.Tolerations) == 1 &&
			podSpec.PriorityClassName == "substrate-workers" &&
			podSpec.Affinity != nil &&
			podSpec.Affinity.NodeAffinity != nil &&
			container.Resources.Requests.Cpu().String() == "500m", nil
	})

	if err := k8sClient.Get(testCtx, types.NamespacedName{Name: wp.Name, Namespace: wp.Namespace}, wp); err != nil {
		t.Fatalf("re-fetch WorkerPool: %v", err)
	}
	wp.Spec.Template = nil
	if err := k8sClient.Update(testCtx, wp); err != nil {
		t.Fatalf("clear WorkerPool template: %v", err)
	}

	eventually(t, func(ctx context.Context) (bool, error) {
		dep, err := getDeployment(ctx, wp)
		if err != nil || len(dep.Spec.Template.Spec.Containers) == 0 {
			return false, nil
		}
		podSpec := dep.Spec.Template.Spec
		container := podSpec.Containers[0]
		return len(podSpec.NodeSelector) == 0 &&
			len(podSpec.Tolerations) == 0 &&
			podSpec.PriorityClassName == "" &&
			(podSpec.Affinity == nil || podSpec.Affinity.NodeAffinity == nil) &&
			len(container.Resources.Limits) == 0 &&
			len(container.Resources.Requests) == 0, nil
	})
}

// TestSSARevertsOwnedPodTemplateFields verifies that if an external actor
// changes pod template fields owned by the workerpool-controller, the
// controller reverts them on the next reconcile.
func TestSSARevertsOwnedPodTemplateFields(t *testing.T) {
	wp := makeWorkerPool("test-ssa-template", "default", 1, "ateom:v1")
	wp.Spec.Template = sampleWorkerPoolPodTemplate()
	if err := k8sClient.Create(testCtx, wp); err != nil {
		t.Fatalf("create WorkerPool: %v", err)
	}
	t.Cleanup(func() { k8sClient.Delete(testCtx, wp) }) //nolint:errcheck

	eventually(t, func(ctx context.Context) (bool, error) {
		dep, err := getDeployment(ctx, wp)
		return err == nil && dep.Spec.Template.Spec.NodeSelector["workload"] == "substrate", nil
	})

	updateDeploymentSpec(t, wp, "rogue update", func(dep *appsv1.Deployment) {
		dep.Spec.Template.Spec.NodeSelector = map[string]string{"workload": "rogue"}
	})

	eventually(t, func(ctx context.Context) (bool, error) {
		dep, err := getDeployment(ctx, wp)
		if err != nil {
			return false, nil
		}
		return dep.Spec.Template.Spec.NodeSelector["workload"] == "substrate", nil
	})
}

// TestReplicasValidationRejectsNegative verifies that the API server rejects a
// WorkerPool whose spec.replicas is negative.
func TestReplicasValidationRejectsNegative(t *testing.T) {
	wp := makeWorkerPool("test-neg-replicas", "default", -1, "ateom:v1")
	err := k8sClient.Create(testCtx, wp)
	if err == nil {
		t.Cleanup(func() { k8sClient.Delete(testCtx, wp) }) //nolint:errcheck
		t.Fatal("expected creation with negative replicas to fail, but it succeeded")
	}
	if !k8errors.IsInvalid(err) {
		t.Fatalf("expected Invalid error, got: %v", err)
	}
}

// --- helpers ---

func makeWorkerPool(name, ns string, replicas int32, image string) *atev1alpha1.WorkerPool {
	return &atev1alpha1.WorkerPool{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Spec: atev1alpha1.WorkerPoolSpec{
			Replicas:   replicas,
			AteomImage: image,
		},
	}
}

func getDeployment(ctx context.Context, wp *atev1alpha1.WorkerPool) (*appsv1.Deployment, error) {
	dep := &appsv1.Deployment{}
	err := k8sClient.Get(ctx, types.NamespacedName{
		Name:      wp.Name,
		Namespace: wp.Namespace,
	}, dep)
	return dep, err
}

// updateDeployment re-fetches the managed Deployment, applies mutate, and
// persists it via update, retrying on optimistic-concurrency conflicts. The
// controller reconciles the Deployment concurrently (bumping its
// resourceVersion on every SSA apply), so a plain get-then-update flakes with
// "the object has been modified" under load.
func updateDeployment(t *testing.T, wp *atev1alpha1.WorkerPool, action string, mutate func(*appsv1.Deployment), update func(*appsv1.Deployment) error) {
	t.Helper()
	err := retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		dep, err := getDeployment(testCtx, wp)
		if err != nil {
			return err
		}
		mutate(dep)
		return update(dep)
	})
	if err != nil {
		t.Fatalf("%s: %v", action, err)
	}
}

func updateDeploymentSpec(t *testing.T, wp *atev1alpha1.WorkerPool, action string, mutate func(*appsv1.Deployment)) {
	t.Helper()
	updateDeployment(t, wp, action, mutate, func(dep *appsv1.Deployment) error {
		return k8sClient.Update(testCtx, dep)
	})
}

// updateDeploymentStatus is updateDeploymentSpec for the status subresource.
func updateDeploymentStatus(t *testing.T, wp *atev1alpha1.WorkerPool, action string, mutate func(*appsv1.Deployment)) {
	t.Helper()
	updateDeployment(t, wp, action, mutate, func(dep *appsv1.Deployment) error {
		return k8sClient.Status().Update(testCtx, dep)
	})
}

// eventually polls condition every 100ms until it returns true or 15s elapses.
func eventually(t *testing.T, condition func(ctx context.Context) (bool, error)) {
	t.Helper()
	if err := wait.PollUntilContextTimeout(context.Background(), 100*time.Millisecond, 15*time.Second, true, condition); err != nil {
		t.Fatalf("condition not met within timeout: %v", err)
	}
}

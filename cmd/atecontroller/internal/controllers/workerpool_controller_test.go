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
	"os/exec"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	k8errors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

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
	cmd := exec.Command("bash", "../../../../hack/run-tool.sh", "setup-envtest", "use", "--print", "path")
	out, err := cmd.Output()
	if err != nil {
		fmt.Fprintf(os.Stderr, "setup-envtest failed: %v\n", err)
		os.Exit(1)
	}
	binaryAssetsDirectory := strings.TrimSpace(string(out))

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

	dep, err := getDeployment(testCtx, wp)
	if err != nil {
		t.Fatalf("get Deployment: %v", err)
	}

	// An external manager sets revisionHistoryLimit — a field the controller
	// never declares in its apply config.
	revisionHistoryLimit := int32(7)
	dep.Spec.RevisionHistoryLimit = &revisionHistoryLimit
	if err := k8sClient.Update(testCtx, dep); err != nil {
		t.Fatalf("set revisionHistoryLimit: %v", err)
	}

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

	dep, err := getDeployment(testCtx, wp)
	if err != nil {
		t.Fatalf("get Deployment: %v", err)
	}
	rogueReplicas := int32(99)
	dep.Spec.Replicas = &rogueReplicas
	if err := k8sClient.Update(testCtx, dep); err != nil {
		t.Fatalf("rogue update: %v", err)
	}

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

	dep, err := getDeployment(testCtx, wp)
	if err != nil {
		t.Fatalf("get Deployment: %v", err)
	}

	// Simulate the deployment controller reporting 3 running pods.
	dep.Status.Replicas = 3
	if err := k8sClient.Status().Update(testCtx, dep); err != nil {
		t.Fatalf("patch Deployment status: %v", err)
	}

	eventually(t, func(ctx context.Context) (bool, error) {
		current := &atev1alpha1.WorkerPool{}
		if err := k8sClient.Get(ctx, types.NamespacedName{Name: wp.Name, Namespace: wp.Namespace}, current); err != nil {
			return false, nil
		}
		return current.Status.Replicas == 3, nil
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
		Name:      deploymentName(wp.Name),
		Namespace: wp.Namespace,
	}, dep)
	return dep, err
}

// eventually polls condition every 100ms until it returns true or 15s elapses.
func eventually(t *testing.T, condition func(ctx context.Context) (bool, error)) {
	t.Helper()
	if err := wait.PollUntilContextTimeout(context.Background(), 100*time.Millisecond, 15*time.Second, true, condition); err != nil {
		t.Fatalf("condition not met within timeout: %v", err)
	}
}

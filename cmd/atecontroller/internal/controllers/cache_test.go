// Copyright 2026 The Agent Substrate Authors
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
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	k8errors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	appsv1ac "k8s.io/client-go/applyconfigurations/apps/v1"
	corev1ac "k8s.io/client-go/applyconfigurations/core/v1"
	metav1ac "k8s.io/client-go/applyconfigurations/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// cachedClient reads from the manager cache, as the reconcilers do. TestMain sets it.
var cachedClient client.Client

// TestCacheOptionsScope checks the Secret and Deployment rules of CacheOptions.
func TestCacheOptionsScope(t *testing.T) {
	t.Parallel()

	ref := types.NamespacedName{Namespace: "ate-system", Name: "pool"}
	opts, err := CacheOptions(ref)
	if err != nil {
		t.Fatalf("CacheOptions: %v", err)
	}

	var secrets, deployments cache.ByObject
	hasSecrets, hasDeployments := false, false
	for obj, by := range opts.ByObject {
		switch obj.(type) {
		case *corev1.Secret:
			secrets, hasSecrets = by, true
		case *appsv1.Deployment:
			deployments, hasDeployments = by, true
		}
	}
	if !hasSecrets {
		t.Fatal("no cache scope for Secrets")
	}
	scope, ok := secrets.Namespaces[ref.Namespace]
	if !ok || len(secrets.Namespaces) != 1 {
		t.Fatalf("Secret cache namespaces = %v, want only %q", secrets.Namespaces, ref.Namespace)
	}
	if got := scope.FieldSelector.String(); got != "metadata.name="+ref.Name {
		t.Errorf("Secret field selector = %q, want %q", got, "metadata.name="+ref.Name)
	}

	if !hasDeployments || deployments.Label == nil {
		t.Fatal("no label-selected cache scope for Deployments")
	}
	if !deployments.Label.Matches(labels.Set{"ate.dev/worker-pool": "pool"}) {
		t.Errorf("Deployment label selector %q does not match the label ate.dev/worker-pool", deployments.Label)
	}
	if deployments.Label.Matches(labels.Set{}) {
		t.Errorf("Deployment label selector %q matches a Deployment with no labels", deployments.Label)
	}
}

// TestDeploymentCacheScope checks that the manager cache holds a pool's
// Deployment and no Deployment without the ate.dev/worker-pool label.
func TestDeploymentCacheScope(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	const label = "ate.dev/worker-pool"
	wp := makeWorkerPool("cache-scope", "default", 2, "worker:v1")

	// The bystander is created first, so a cache of every Deployment holds it
	// before it holds the labeled Deployment of the pool.
	bystander := applyDeploymentWithoutLabels(t, "cache-scope-bystander", wp.Namespace, map[string]string{"app": "bystander"}, "someone-else")
	applyDeploymentWithoutLabels(t, wp.Name, wp.Namespace, map[string]string{label: wp.Name}, workerPoolFieldOwner)

	if err := k8sClient.Create(ctx, wp); err != nil {
		t.Fatalf("create WorkerPool: %v", err)
	}
	deleteOnCleanup(t, wp)

	eventually(t, func(ctx context.Context) (bool, error) {
		dep := &appsv1.Deployment{}
		if err := cachedClient.Get(ctx, client.ObjectKeyFromObject(wp), dep); err != nil {
			return false, nil
		}
		return dep.Labels[label] == wp.Name && metav1.IsControlledBy(dep, wp), nil
	})

	err := cachedClient.Get(ctx, client.ObjectKeyFromObject(bystander), &appsv1.Deployment{})
	if !k8errors.IsNotFound(err) {
		t.Errorf("cached read of a Deployment without %s: err = %v, want NotFound", label, err)
	}
	if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(bystander), &appsv1.Deployment{}); err != nil {
		t.Fatalf("live read of the bystander Deployment: %v", err)
	}

	dep, err := getDeployment(ctx, wp)
	if err != nil {
		t.Fatalf("get Deployment: %v", err)
	}
	strip := client.RawPatch(types.MergePatchType, []byte(`{"metadata":{"labels":{"`+label+`":null}}}`))
	if err := k8sClient.Patch(ctx, dep, strip); err != nil {
		t.Fatalf("strip %s: %v", label, err)
	}
	if _, has := dep.Labels[label]; has {
		t.Fatalf("label %s survived the patch", label)
	}
	eventually(t, func(ctx context.Context) (bool, error) {
		dep, err := getDeployment(ctx, wp)
		return err == nil && dep.Labels[label] == wp.Name, nil
	})
}

// applyDeploymentWithoutLabels applies a Deployment whose metadata carries no
// labels, and deletes it on cleanup.
func applyDeploymentWithoutLabels(t *testing.T, name, namespace string, podLabels map[string]string, fieldOwner string) *appsv1.Deployment {
	t.Helper()
	ac := appsv1ac.Deployment(name, namespace).
		WithSpec(appsv1ac.DeploymentSpec().
			WithSelector(metav1ac.LabelSelector().WithMatchLabels(podLabels)).
			WithTemplate(corev1ac.PodTemplateSpec().
				WithLabels(podLabels).
				WithSpec(corev1ac.PodSpec().
					WithContainers(corev1ac.Container().WithName("worker").WithImage("worker:v0")))))
	if err := k8sClient.Apply(t.Context(), ac, client.FieldOwner(fieldOwner)); err != nil {
		t.Fatalf("apply Deployment %s: %v", name, err)
	}
	dep := &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace}}
	deleteOnCleanup(t, dep)
	return dep
}

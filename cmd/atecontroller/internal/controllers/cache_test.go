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

package controllers

import (
	"context"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/cmd/atecontroller/internal/workersync"
	atev1alpha1 "github.com/agent-substrate/substrate/pkg/api/v1alpha1"
	"github.com/agent-substrate/substrate/pkg/client/clientset/versioned"
	"github.com/agent-substrate/substrate/pkg/client/informers/externalversions"
	appsv1 "k8s.io/api/apps/v1"
	certsv1beta1 "k8s.io/api/certificates/v1beta1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// The real API server rejects cluster-wide requests for namespaced resources.
// Workload caches must still sync, while the system Secret and cluster-scoped
// configuration keep their independent scopes.
func TestNamespaceScopedCachesWithRBAC(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	create := func(obj client.Object) {
		t.Helper()
		if err := k8sClient.Create(ctx, obj); err != nil {
			t.Fatal(err)
		}
		deleteOnCleanup(t, obj)
	}
	newNamespace := func(prefix string) string {
		t.Helper()
		namespace := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{GenerateName: prefix}}
		create(namespace)
		return namespace.Name
	}
	workloadNamespace := newNamespace("scoped-workloads-")
	systemNamespace := newNamespace("scoped-system-")
	otherNamespace := newNamespace("scoped-other-")
	username := workloadNamespace + "-reader"
	create(&rbacv1.Role{
		ObjectMeta: metav1.ObjectMeta{Name: "workloads", Namespace: workloadNamespace},
		Rules: []rbacv1.PolicyRule{
			{APIGroups: []string{""}, Resources: []string{"pods"}, Verbs: []string{"get", "list", "watch"}},
			{APIGroups: []string{"apps"}, Resources: []string{"deployments"}, Verbs: []string{"get", "list", "watch"}},
			{APIGroups: []string{"networking.k8s.io"}, Resources: []string{"networkpolicies"}, Verbs: []string{"get", "list", "watch"}},
			{APIGroups: []string{"ate.dev"}, Resources: []string{"workerpools"}, Verbs: []string{"get", "list", "watch"}},
		},
	})
	pool := EgressMITMCAPoolRef(systemNamespace)
	create(&rbacv1.Role{
		ObjectMeta: metav1.ObjectMeta{Name: "ca-pool", Namespace: systemNamespace},
		Rules:      []rbacv1.PolicyRule{{APIGroups: []string{""}, Resources: []string{"secrets"}, ResourceNames: []string{pool.Name}, Verbs: []string{"get", "list", "watch"}}},
	})
	for namespace, role := range map[string]string{workloadNamespace: "workloads", systemNamespace: "ca-pool"} {
		create(&rbacv1.RoleBinding{
			ObjectMeta: metav1.ObjectMeta{Name: role, Namespace: namespace},
			Subjects:   []rbacv1.Subject{{Kind: "User", Name: username, APIGroup: rbacv1.GroupName}},
			RoleRef:    rbacv1.RoleRef{Kind: "Role", Name: role, APIGroup: rbacv1.GroupName},
		})
	}
	create(&rbacv1.ClusterRole{
		ObjectMeta: metav1.ObjectMeta{Name: username},
		Rules: []rbacv1.PolicyRule{
			{APIGroups: []string{"ate.dev"}, Resources: []string{"sandboxconfigs", "csidriverconfigs"}, Verbs: []string{"get", "list", "watch"}},
			{APIGroups: []string{"certificates.k8s.io"}, Resources: []string{"clustertrustbundles"}, Verbs: []string{"get", "list", "watch"}},
		},
	})
	create(&rbacv1.ClusterRoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: username},
		Subjects:   []rbacv1.Subject{{Kind: "User", Name: username, APIGroup: rbacv1.GroupName}},
		RoleRef:    rbacv1.RoleRef{Kind: "ClusterRole", Name: username, APIGroup: rbacv1.GroupName},
	})
	create(&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: pool.Name, Namespace: systemNamespace}})
	_, roots := caPoolSecret(t, "scoped-root")
	bundle := &certsv1beta1.ClusterTrustBundle{
		ObjectMeta: metav1.ObjectMeta{Name: username},
		Spec:       certsv1beta1.ClusterTrustBundleSpec{TrustBundle: rootPEM(t, roots)},
	}
	create(bundle)
	for _, namespace := range []string{workloadNamespace, otherNamespace} {
		create(&corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: "worker", Namespace: namespace, Labels: map[string]string{"ate.dev/worker-pool": "pool"}},
			Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "main", Image: "test.invalid/worker"}}},
		})
	}
	restricted := rest.CopyConfig(cfg)
	restricted.Impersonate = rest.ImpersonationConfig{UserName: username, Groups: []string{"system:authenticated"}}
	kc := kubernetes.NewForConfigOrDie(restricted)
	if err := wait.PollUntilContextCancel(ctx, 20*time.Millisecond, true, func(ctx context.Context) (bool, error) {
		_, err := kc.CoreV1().Pods(workloadNamespace).List(ctx, metav1.ListOptions{})
		if apierrors.IsForbidden(err) {
			return false, nil
		}
		return err == nil, err
	}); err != nil {
		t.Fatalf("waiting for RoleBinding: %v", err)
	}
	if _, err := kc.CoreV1().Pods("").List(ctx, metav1.ListOptions{}); !apierrors.IsForbidden(err) {
		t.Fatalf("cluster-wide Pod list = %v, want Forbidden", err)
	}
	ac := versioned.NewForConfigOrDie(restricted)
	if _, err := ac.ApiV1alpha1().WorkerPools("").List(ctx, metav1.ListOptions{}); !apierrors.IsForbidden(err) {
		t.Fatalf("cluster-wide WorkerPool list = %v, want Forbidden", err)
	}

	workerFactory, workerInformer := workersync.WorkerPodInformer(kc, workloadNamespace)
	ateFactory := externalversions.NewSharedInformerFactoryWithOptions(ac, 0, externalversions.WithNamespace(workloadNamespace))
	ateFactory.Api().V1alpha1().WorkerPools().Informer()
	ateFactory.Api().V1alpha1().SandboxConfigs().Informer()
	ateFactory.Api().V1alpha1().CSIDriverConfigs().Informer()
	workerFactory.Start(ctx.Done())
	ateFactory.Start(ctx.Done())
	t.Cleanup(func() {
		cancel()
		workerFactory.Shutdown()
		ateFactory.Shutdown()
	})
	for _, synced := range workerFactory.WaitForCacheSync(ctx.Done()) {
		if !synced {
			t.Fatal("Worker Pod cache did not sync with namespace Role")
		}
	}
	for kind, synced := range ateFactory.WaitForCacheSync(ctx.Done()) {
		if !synced {
			t.Fatalf("%v cache did not sync", kind)
		}
	}
	if got := workerInformer.GetStore().List(); len(got) != 1 || got[0].(*corev1.Pod).Namespace != workloadNamespace {
		t.Fatalf("worker cache = %v, want only the watched namespace", got)
	}
	// Exercise the watch after the initial list has synced.
	var updated corev1.Pod
	key := types.NamespacedName{Namespace: workloadNamespace, Name: "worker"}
	if err := k8sClient.Get(ctx, key, &updated); err != nil {
		t.Fatal(err)
	}
	updated.Labels["watch-observed"] = "updated"
	if err := k8sClient.Update(ctx, &updated); err != nil {
		t.Fatal(err)
	}
	if err := wait.PollUntilContextCancel(ctx, 20*time.Millisecond, true, func(context.Context) (bool, error) {
		obj, exists, err := workerInformer.GetIndexer().GetByKey(key.String())
		return exists && obj.(*corev1.Pod).Labels["watch-observed"] == "updated", err
	}); err != nil {
		t.Fatalf("namespace-scoped watch did not deliver Pod update: %v", err)
	}

	for _, namespace := range []string{workloadNamespace, ""} {
		t.Run("manager-namespace="+namespace, func(t *testing.T) {
			config := restricted
			if namespace == "" {
				config = cfg
			}
			opts := CacheOptions(namespace, systemNamespace)
			opts.Scheme = runtime.NewScheme()
			if err := clientgoscheme.AddToScheme(opts.Scheme); err != nil {
				t.Fatal(err)
			}
			if err := atev1alpha1.AddToScheme(opts.Scheme); err != nil {
				t.Fatal(err)
			}
			c, err := cache.New(config, opts)
			if err != nil {
				t.Fatal(err)
			}
			for _, obj := range []client.Object{&corev1.Pod{}, &atev1alpha1.WorkerPool{}, &appsv1.Deployment{}, &networkingv1.NetworkPolicy{}, &corev1.Secret{}, &certsv1beta1.ClusterTrustBundle{}} {
				if _, err := c.GetInformer(ctx, obj, cache.BlockUntilSynced(false)); err != nil {
					t.Fatal(err)
				}
			}
			cacheCtx, stop := context.WithCancel(ctx)
			done := make(chan error, 1)
			go func() { done <- c.Start(cacheCtx) }()
			t.Cleanup(func() {
				stop()
				if err := <-done; err != nil {
					t.Errorf("cache stopped: %v", err)
				}
			})
			if !c.WaitForCacheSync(ctx) {
				t.Fatal("manager cache did not sync")
			}
			var secret corev1.Secret
			if err := c.Get(ctx, pool, &secret); err != nil {
				t.Fatalf("system CA pool: %v", err)
			}
			var cachedBundle certsv1beta1.ClusterTrustBundle
			if err := c.Get(ctx, types.NamespacedName{Name: bundle.Name}, &cachedBundle); err != nil {
				t.Fatalf("cluster trust bundle: %v", err)
			}
			if cachedBundle.Spec.TrustBundle != bundle.Spec.TrustBundle {
				t.Fatal("cluster trust bundle cache did not contain the expected certificate")
			}
			var pods corev1.PodList
			if err := c.List(ctx, &pods, client.MatchingLabels{"ate.dev/worker-pool": "pool"}); err != nil {
				t.Fatal(err)
			}
			want := 1
			if namespace == "" {
				want = 2
			}
			if len(pods.Items) != want {
				t.Fatalf("cached %d Pods, want %d", len(pods.Items), want)
			}
			if namespace != "" {
				var pod corev1.Pod
				if err := c.Get(ctx, types.NamespacedName{Namespace: otherNamespace, Name: "worker"}, &pod); err == nil {
					t.Fatal("manager cache exposed a Pod outside the watched namespace")
				}
			}
		})
	}
}

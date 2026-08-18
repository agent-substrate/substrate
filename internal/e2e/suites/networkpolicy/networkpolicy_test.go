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

package networkpolicy

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/agent-substrate/substrate/internal/e2e"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/api/v1alpha1"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

const (
	ateSystemNamespace  = "ate-system"
	atenetRouterAppName = "atenet-router"
)

func TestNetworkPolicyLifecycleAndReconciliation(t *testing.T) {
	nsObj := e2e.CreateNamespace(t)
	ctx := context.Background()
	clients := e2e.GetClients()

	wpName := "test-wp-netpol"
	wp := &v1alpha1.WorkerPool{
		ObjectMeta: metav1.ObjectMeta{
			Name:      wpName,
			Namespace: nsObj.Name,
		},
		Spec: v1alpha1.WorkerPoolSpec{
			Replicas:   1,
			AteomImage: "ateom:v1",
		},
	}

	t.Logf("Creating WorkerPool %s/%s...", nsObj.Name, wpName)
	if _, err := clients.SubstrateK8s.ApiV1alpha1().WorkerPools(nsObj.Name).Create(ctx, wp, metav1.CreateOptions{}); err != nil {
		t.Fatalf("failed to create WorkerPool: %v", err)
	}

	expectedNpName := resources.NetworkPolicyName(wpName)
	t.Logf("Waiting for NetworkPolicy %s to be created...", expectedNpName)

	var np *networkingv1.NetworkPolicy
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		var err error
		np, err = clients.K8s.NetworkingV1().NetworkPolicies(nsObj.Name).Get(ctx, expectedNpName, metav1.GetOptions{})
		if err == nil {
			break
		}
		time.Sleep(1 * time.Second)
	}

	if np == nil {
		t.Fatalf("timed out waiting for NetworkPolicy %s to be created in namespace %s", expectedNpName, nsObj.Name)
	}
	t.Logf("Successfully retrieved NetworkPolicy %s", expectedNpName)

	// Validate OwnerReference and Labels
	if len(np.OwnerReferences) == 0 || np.OwnerReferences[0].Name != wpName {
		t.Errorf("expected OwnerReference to %q, got %v", wpName, np.OwnerReferences)
	}
	if np.Labels == nil || np.Labels["ate.dev/worker-pool"] != wpName {
		t.Errorf("expected label ate.dev/worker-pool=%q, got %v", wpName, np.Labels)
	}

	// Validate PodSelector
	if np.Spec.PodSelector.MatchLabels == nil || np.Spec.PodSelector.MatchLabels["ate.dev/worker-pool"] != wpName {
		t.Errorf("expected PodSelector match label ate.dev/worker-pool=%q, got %v", wpName, np.Spec.PodSelector.MatchLabels)
	}

	// Validate Ingress Rule
	if len(np.Spec.Ingress) != 1 {
		t.Fatalf("expected exactly 1 ingress rule, got %d", len(np.Spec.Ingress))
	}
	ingressRule := np.Spec.Ingress[0]
	if len(ingressRule.From) != 1 {
		t.Fatalf("expected exactly 1 ingress from peer, got %d", len(ingressRule.From))
	}
	fromPeer := ingressRule.From[0]
	if fromPeer.NamespaceSelector == nil || fromPeer.NamespaceSelector.MatchLabels["kubernetes.io/metadata.name"] != ateSystemNamespace {
		t.Errorf("expected namespace selector for %s, got %v", ateSystemNamespace, fromPeer.NamespaceSelector)
	}
	if fromPeer.PodSelector == nil || fromPeer.PodSelector.MatchLabels["app"] != atenetRouterAppName {
		t.Errorf("expected pod selector for %s, got %v", atenetRouterAppName, fromPeer.PodSelector)
	}

	// Validate Egress Rule is unmanaged (empty)
	if len(np.Spec.Egress) != 0 {
		t.Errorf("expected Egress to be empty/unmanaged, got %v", np.Spec.Egress)
	}

	// Verify Garbage Collection upon deletion
	t.Logf("Deleting WorkerPool %s/%s and verifying NetworkPolicy GC...", nsObj.Name, wpName)
	if err := clients.SubstrateK8s.ApiV1alpha1().WorkerPools(nsObj.Name).Delete(ctx, wpName, metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
		t.Fatalf("failed to delete WorkerPool: %v", err)
	}

	gcDeadline := time.Now().Add(60 * time.Second)
	gcSuccess := false
	for time.Now().Before(gcDeadline) {
		_, err := clients.K8s.NetworkingV1().NetworkPolicies(nsObj.Name).Get(ctx, expectedNpName, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			gcSuccess = true
			break
		}
		time.Sleep(2 * time.Second)
	}

	if !gcSuccess {
		t.Fatalf("timed out waiting for NetworkPolicy %s to be garbage collected after WorkerPool deletion", expectedNpName)
	}
	t.Logf("NetworkPolicy %s was successfully garbage collected", expectedNpName)
}

func TestNetworkPolicyDataPlaneEnforcement(t *testing.T) {
	nsObj := e2e.CreateNamespace(t)
	ctx := context.Background()
	clients := e2e.GetClients()

	// Setup WorkerPool and ActorTemplate from the standard counter demo
	wp, at := setupDemoCounterTemplate(ctx, t, clients, nsObj.Name)

	// Create Atespace
	if _, err := clients.SubstrateAPI.CreateAtespace(ctx, &ateapipb.CreateAtespaceRequest{
		Atespace: &ateapipb.Atespace{Metadata: &ateapipb.ResourceMetadata{Name: nsObj.Name}},
	}); err != nil {
		t.Fatalf("failed to create atespace: %v", err)
	}

	// Create and Resume Actor
	actorName := "netpol-dataplane-" + nsObj.Name
	t.Logf("Creating Actor %q in Atespace %q...", actorName, nsObj.Name)
	if _, err := clients.SubstrateAPI.CreateActor(ctx, &ateapipb.CreateActorRequest{Actor: &ateapipb.Actor{
		Metadata:               &ateapipb.ResourceMetadata{Atespace: nsObj.Name, Name: actorName},
		ActorTemplateNamespace: nsObj.Name,
		ActorTemplateName:      at.Name,
	}}); err != nil {
		t.Fatalf("failed to create Actor: %v", err)
	}
	defer func() {
		_, _ = clients.SubstrateAPI.SuspendActor(ctx, &ateapipb.SuspendActorRequest{
			Actor: &ateapipb.ObjectRef{Atespace: nsObj.Name, Name: actorName},
		})
		_, _ = clients.SubstrateAPI.DeleteActor(ctx, &ateapipb.DeleteActorRequest{
			Actor: &ateapipb.ObjectRef{Atespace: nsObj.Name, Name: actorName},
		})
	}()

	t.Logf("Resuming Actor %q...", actorName)
	if _, err := clients.SubstrateAPI.ResumeActor(ctx, &ateapipb.ResumeActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: nsObj.Name, Name: actorName},
	}); err != nil {
		t.Fatalf("failed to resume Actor: %v", err)
	}
	waitForActorRunning(ctx, t, clients, nsObj.Name, actorName)

	// === Positive Data Plane Verification (Authorized Ingress) ===
	t.Log("=== Verifying authorized ingress via atenet-router ===")
	rc, err := e2e.NewRouterClient(ctx)
	if err != nil {
		t.Fatalf("NewRouterClient: %v", err)
	}
	defer rc.Close()

	var resp *http.Response
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		resp, err = rc.Get(ctx, resources.ActorRef{Atespace: nsObj.Name, Name: actorName}, "/")
		if err == nil && resp.StatusCode == http.StatusOK {
			break
		}
		if resp != nil {
			resp.Body.Close()
		}
		time.Sleep(1 * time.Second)
	}

	if err != nil {
		t.Fatalf("Positive Data Plane Verification failed: GET / via atenet-router returned error: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("Positive Data Plane Verification failed: GET / via atenet-router status %d, body %q", resp.StatusCode, body)
	}
	body, _ := io.ReadAll(resp.Body)
	t.Logf("Positive Data Plane Verification PASSED: Successfully accessed actor %q via authorized atenet-router (HTTP %d): %s", actorName, resp.StatusCode, strings.TrimSpace(string(body)))

	// === Negative Data Plane Verification (Unauthorized Ingress Blocking) ===
	t.Log("=== Verifying unauthorized ingress blocking ===")
	rogueNsObj := e2e.CreateNamespace(t)

	// Find the IP address of the worker pod backing our WorkerPool
	pods, err := clients.K8s.CoreV1().Pods(nsObj.Name).List(ctx, metav1.ListOptions{
		LabelSelector: fmt.Sprintf("ate.dev/worker-pool=%s", wp.Name),
	})
	if err != nil || len(pods.Items) == 0 {
		t.Fatalf("failed to list worker pods for worker pool %q: %v", wp.Name, err)
	}
	var workerIP string
	for i := range pods.Items {
		if pods.Items[i].Status.Phase == corev1.PodRunning && pods.Items[i].Status.PodIP != "" {
			workerIP = pods.Items[i].Status.PodIP
			t.Logf("Target worker pod %s/%s has IP %s", nsObj.Name, pods.Items[i].Name, workerIP)
			break
		}
	}
	if workerIP == "" {
		t.Fatalf("no running worker pod with IP found for worker pool %q", wp.Name)
	}

	// Deploy an unauthorized probe pod in an external test namespace
	probePod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "unauthorized-probe",
			Namespace: rogueNsObj.Name,
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Name:    "probe",
					Image:   "busybox@sha256:1487d0af5f52b4ba31c7e465126ee2123fe3f2305d638e7827681e7cf6c83d5e",
					Command: []string{"/bin/sleep", "3600"},
				},
			},
			RestartPolicy: corev1.RestartPolicyNever,
		},
	}
	t.Logf("Creating unauthorized probe pod %s/%s...", rogueNsObj.Name, probePod.Name)
	if _, err := clients.K8s.CoreV1().Pods(rogueNsObj.Name).Create(ctx, probePod, metav1.CreateOptions{}); err != nil {
		t.Fatalf("failed to create probe pod: %v", err)
	}
	waitForPodRunning(ctx, t, clients, rogueNsObj.Name, probePod.Name)

	// Attempt direct connection to worker pod IP from unauthorized probe pod
	execArgs := []string{"exec", "-n", rogueNsObj.Name, probePod.Name}
	if e2e.KubeContext != "" {
		execArgs = append([]string{"--context=" + e2e.KubeContext}, execArgs...)
	}
	if e2e.KubeConfig != "" {
		execArgs = append([]string{"--kubeconfig=" + e2e.KubeConfig}, execArgs...)
	}
	execArgs = append(execArgs, "--", "sh", "-c", fmt.Sprintf("wget -T 3 -q -O - http://%s:8080/ || nc -z -w 3 %s 8080", workerIP, workerIP))

	t.Logf("Executing unauthorized connection attempt from %s/%s to %s:8080...", rogueNsObj.Name, probePod.Name, workerIP)
	cmd := exec.Command("kubectl", execArgs...)
	output, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("Negative Data Plane Verification failed: unauthorized probe pod successfully connected to worker pod %s:8080 (output: %s). NetworkPolicy did not block the connection!", workerIP, string(output))
	}
	t.Logf("Negative Data Plane Verification PASSED: Unauthorized connection attempt from %s/%s to %s:8080 was blocked as expected (err: %v)", rogueNsObj.Name, probePod.Name, workerIP, err)
}

func setupDemoCounterTemplate(ctx context.Context, t *testing.T, clients *e2e.Clients, ns string) (*v1alpha1.WorkerPool, *v1alpha1.ActorTemplate) {
	t.Helper()
	srcNS := "ate-demo-counter"
	if v := os.Getenv("E2E_TEMPLATE_NAMESPACE"); v != "" {
		srcNS = v
	}
	srcName := "counter"
	if v := os.Getenv("E2E_TEMPLATE_NAME"); v != "" {
		srcName = v
	}

	existingWp, err := clients.SubstrateK8s.ApiV1alpha1().WorkerPools(srcNS).Get(ctx, srcName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("failed to get existing WorkerPool %s/%s: %v", srcNS, srcName, err)
	}

	existingAt, err := clients.SubstrateK8s.ApiV1alpha1().ActorTemplates(srcNS).Get(ctx, srcName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("failed to get existing ActorTemplate %s/%s: %v", srcNS, srcName, err)
	}

	wp := &v1alpha1.WorkerPool{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "counter",
			Namespace: ns,
			Labels:    map[string]string{"netpol-test": ns},
		},
		Spec: v1alpha1.WorkerPoolSpec{
			Replicas:          1,
			AteomImage:        existingWp.Spec.AteomImage,
			SandboxClass:      existingWp.Spec.SandboxClass,
			SandboxConfigName: existingWp.Spec.SandboxConfigName,
		},
	}
	if _, err := clients.SubstrateK8s.ApiV1alpha1().WorkerPools(ns).Create(ctx, wp, metav1.CreateOptions{}); err != nil {
		t.Fatalf("failed to create WorkerPool: %v", err)
	}

	at := &v1alpha1.ActorTemplate{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "counter",
			Namespace: ns,
		},
		Spec: v1alpha1.ActorTemplateSpec{
			WorkerSelector: &metav1.LabelSelector{
				MatchLabels: map[string]string{"netpol-test": ns},
			},
			SandboxClass:    existingAt.Spec.SandboxClass,
			Containers:      existingAt.Spec.Containers,
			SnapshotsConfig: existingAt.Spec.SnapshotsConfig,
			Volumes:         existingAt.Spec.Volumes,
		},
	}
	if _, err := clients.SubstrateK8s.ApiV1alpha1().ActorTemplates(ns).Create(ctx, at, metav1.CreateOptions{}); err != nil {
		t.Fatalf("failed to create ActorTemplate: %v", err)
	}

	t.Logf("Waiting for ActorTemplate %s/%s to be Ready...", ns, at.Name)
	tmplTimeout := 90 * time.Second
	if v := os.Getenv("E2E_TEMPLATE_READY_TIMEOUT"); v != "" {
		d, perr := time.ParseDuration(v)
		if perr != nil {
			t.Fatalf("invalid E2E_TEMPLATE_READY_TIMEOUT %q: %v", v, perr)
		}
		tmplTimeout = d
	}
	tmplCtx, tmplCancel := context.WithTimeout(ctx, tmplTimeout)
	defer tmplCancel()
	var lastPhase v1alpha1.PhaseType
	for {
		curAt, err := clients.SubstrateK8s.ApiV1alpha1().ActorTemplates(ns).Get(tmplCtx, at.Name, metav1.GetOptions{})
		if err == nil {
			lastPhase = curAt.Status.Phase
			if lastPhase == v1alpha1.PhaseReady {
				t.Logf("ActorTemplate %s/%s is Ready with golden snapshot %q", ns, at.Name, curAt.Status.GoldenSnapshot)
				break
			}
			if lastPhase == v1alpha1.PhaseFailed {
				t.Fatalf("ActorTemplate %s/%s transitioned to PhaseFailed!", ns, at.Name)
			}
		}
		select {
		case <-tmplCtx.Done():
			t.Fatalf("Timed out waiting for ActorTemplate %q to be Ready after %v (last phase: %s, err: %v)", at.Name, tmplTimeout, lastPhase, err)
		case <-time.After(1 * time.Second):
		}
	}

	return wp, at
}

func waitForActorRunning(ctx context.Context, t *testing.T, clients *e2e.Clients, atespace, actorName string) {
	t.Helper()
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := clients.SubstrateAPI.GetActor(ctx, &ateapipb.GetActorRequest{
			Actor: &ateapipb.ObjectRef{Atespace: atespace, Name: actorName},
		})
		if err == nil && resp.GetStatus().GetState() == ateapipb.ActorState_ACTOR_STATE_RUNNING {
			t.Logf("Actor %q reached RUNNING state", actorName)
			return
		}
		time.Sleep(1 * time.Second)
	}
	t.Fatalf("timed out waiting for actor %q to reach RUNNING state", actorName)
}

func waitForPodRunning(ctx context.Context, t *testing.T, clients *e2e.Clients, namespace, podName string) {
	t.Helper()
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		pod, err := clients.K8s.CoreV1().Pods(namespace).Get(ctx, podName, metav1.GetOptions{})
		if err == nil && pod.Status.Phase == corev1.PodRunning {
			t.Logf("Pod %s/%s is RUNNING", namespace, podName)
			return
		}
		time.Sleep(1 * time.Second)
	}
	t.Fatalf("timed out waiting for pod %s/%s to reach RUNNING status", namespace, podName)
}

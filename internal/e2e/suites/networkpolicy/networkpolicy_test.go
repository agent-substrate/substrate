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
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	"go.ate.dev/substrate/api/ateapi/v1"
	atev1 "go.ate.dev/substrate/api/infra/k8s/ate/v1"
	"go.ate.dev/substrate/internal/ateclient"
	"go.ate.dev/substrate/internal/e2e"
	"go.ate.dev/substrate/internal/portforward"
	"go.ate.dev/substrate/internal/resources"
)

const (
	atenetRouterAppName = "atenet-router"
)

func TestNetworkPolicyLifecycleAndReconciliation(t *testing.T) {
	nsObj := e2e.CreateNamespace(t)
	ctx := context.Background()
	clients := e2e.GetClients()

	wpName := "test-wp-netpol"
	wp := &atev1.WorkerPool{
		ObjectMeta: metav1.ObjectMeta{
			Name:      wpName,
			Namespace: nsObj.Name,
		},
		Spec: atev1.WorkerPoolSpec{
			Replicas:       1,
			WorkerImage:    "worker:v1",
			SandboxClasses: []atev1.WorkerPoolSandboxClass{{Name: atev1.SandboxClassGvisor}},
		},
	}

	t.Logf("Creating WorkerPool %s/%s...", nsObj.Name, wpName)
	if _, err := clients.SubstrateK8s.AteV1().WorkerPools(nsObj.Name).Create(ctx, wp, metav1.CreateOptions{}); err != nil {
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
	if fromPeer.NamespaceSelector == nil || fromPeer.NamespaceSelector.MatchLabels["kubernetes.io/metadata.name"] != e2e.SystemNamespace() {
		t.Errorf("expected namespace selector for %s, got %v", e2e.SystemNamespace(), fromPeer.NamespaceSelector)
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
	if err := clients.SubstrateK8s.AteV1().WorkerPools(nsObj.Name).Delete(ctx, wpName, metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
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

	// Setup WorkerPool and ActorTemplate from the substrate counter demo (the
	// template's atespace, named after the test namespace, is created there).
	poolName, at := setupDemoCounterTemplate(ctx, t, clients, nsObj.Name)

	// Create and Resume Actor
	actorName := "netpol-dataplane-" + nsObj.Name
	t.Logf("Creating Actor %q in Atespace %q...", actorName, nsObj.Name)
	if _, err := clients.SubstrateAPI.CreateActor(ctx, &ateapi.CreateActorRequest{Actor: &ateapi.Actor{
		Metadata:      &ateapi.ResourceMetadata{Atespace: nsObj.Name, Name: actorName},
		ActorTemplate: e2e.TemplateRef(at),
	}}); err != nil {
		t.Fatalf("failed to create Actor: %v", err)
	}
	defer func() {
		_, _ = clients.SubstrateAPI.SuspendActor(ctx, &ateapi.SuspendActorRequest{
			Actor: &ateapi.ObjectRef{Atespace: nsObj.Name, Name: actorName},
		})
		_, _ = clients.SubstrateAPI.DeleteActor(ctx, &ateapi.DeleteActorRequest{
			Actor: &ateapi.ObjectRef{Atespace: nsObj.Name, Name: actorName},
		})
	}()

	t.Logf("Resuming Actor %q...", actorName)
	if _, err := e2e.ResumeActorAwaitCapacity(t, ctx, clients, &ateapi.ResumeActorRequest{
		Actor: &ateapi.ObjectRef{Atespace: nsObj.Name, Name: actorName},
	}); err != nil {
		t.Fatalf("failed to resume Actor: %v", err)
	}
	waitForActorRunning(ctx, t, clients, nsObj.Name, actorName)

	// === Positive Data Plane Verification (Authorized Ingress) ===
	t.Log("=== Verifying authorized ingress via atenet-router ===")
	rc, err := e2e.NewRouterClient(t, ctx)
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

	// Find the IP address of the worker pod backing our WorkerPool
	pods, err := clients.K8s.CoreV1().Pods(nsObj.Name).List(ctx, metav1.ListOptions{
		LabelSelector: fmt.Sprintf("ate.dev/worker-pool=%s", poolName),
	})
	if err != nil || len(pods.Items) == 0 {
		t.Fatalf("failed to list worker pods for worker pool %q: %v", poolName, err)
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
		t.Fatalf("no running worker pod with IP found for worker pool %q", poolName)
	}

	// The probe runs in a new namespace, which the policy does not allow in.
	// The control pod runs in the worker's namespace, but no policy selects
	// it. If the probe can reach the control pod, a failed dial to the worker
	// is caused by the policy, not by a probe that cannot connect at all.
	probe := e2e.DeployServerPod(t, ctx, testserverPod("unauthorized-probe", ""))
	control := e2e.DeployServerPod(t, ctx, testserverPod("netpol-control", nsObj.Name))
	controlPod, err := clients.K8s.CoreV1().Pods(control.Namespace).Get(ctx, control.Name, metav1.GetOptions{})
	if err != nil || controlPod.Status.PodIP == "" {
		t.Fatalf("failed to get the IP of control pod %s/%s: %v", control.Namespace, control.Name, err)
	}
	pc := newProbeClient(t, ctx, probe)

	controlAddress := net.JoinHostPort(controlPod.Status.PodIP, strconv.Itoa(testserverPort))
	t.Logf("Dialing control pod %s/%s at %s from probe pod %s/%s...", control.Namespace, control.Name, controlAddress, probe.Namespace, probe.Name)
	var result dialResult
	controlDeadline := time.Now().Add(30 * time.Second)
	for {
		result = pc.dial(t, ctx, controlAddress)
		if result.OK || time.Now().After(controlDeadline) {
			break
		}
		time.Sleep(1 * time.Second)
	}
	if !result.OK {
		t.Fatalf("Probe control failed: probe pod %s/%s cannot reach control pod %s, which no NetworkPolicy selects (%s), so a blocked dial to the worker would prove nothing", probe.Namespace, probe.Name, controlAddress, result.Error)
	}

	workerAddress := net.JoinHostPort(workerIP, workerReadyzPort)
	t.Logf("Attempting unauthorized connection from %s/%s to worker %s...", probe.Namespace, probe.Name, workerAddress)
	result = pc.dial(t, ctx, workerAddress)
	if result.OK {
		t.Fatalf("Negative Data Plane Verification failed: unauthorized probe pod %s/%s successfully connected to worker pod %s. NetworkPolicy did not block the connection!", probe.Namespace, probe.Name, workerAddress)
	}
	t.Logf("Negative Data Plane Verification PASSED: Unauthorized connection attempt from %s/%s to %s was blocked as expected (%s)", probe.Namespace, probe.Name, workerAddress, result.Error)
}

const (
	// testserverPort is the port the probe and control pods listen on.
	testserverPort = 8080
	// workerReadyzPort is the port of worker's /readyz server.
	workerReadyzPort = "8080"
)

// testserverPod describes a testserver http pod, used for both the probe,
// which serves /dial, and the control, which is the dial target. An empty
// namespace makes DeployServerPod create a new one.
func testserverPod(name, namespace string) e2e.ServerPod {
	return e2e.ServerPod{
		Name:       name,
		Namespace:  namespace,
		ImportPath: "go.ate.dev/substrate/internal/e2e/fixtures/testserver",
		Args:       []string{"http"},
		Port:       testserverPort,
	}
}

// dialResult mirrors testserver's /dial response.
type dialResult struct {
	Address string `json:"address"`
	OK      bool   `json:"ok"`
	Error   string `json:"error"`
}

// probeClient calls a testserver pod's /dial over a port-forward, so the
// connection is made from that pod.
type probeClient struct {
	baseURL string
	http    *http.Client
}

func newProbeClient(t *testing.T, ctx context.Context, probe e2e.Server) *probeClient {
	t.Helper()
	config, err := ateclient.LoadKubeConfig(e2e.KubeConfig, e2e.KubeContext)
	if err != nil {
		t.Fatalf("loading kubeconfig: %v", err)
	}
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		t.Fatalf("creating k8s client for port-forward: %v", err)
	}
	localPort, stop, err := portforward.ServicePortForward(ctx, config, clientset, probe.Namespace, probe.Name, testserverPort)
	if err != nil {
		t.Fatalf("port-forwarding %s/%s: %v", probe.Namespace, probe.Name, err)
	}
	t.Cleanup(stop)
	return &probeClient{
		baseURL: fmt.Sprintf("http://127.0.0.1:%d", localPort),
		http:    &http.Client{Timeout: 30 * time.Second},
	}
}

// dial asks the probe to open a TCP connection to address. A refused or
// timed-out connection comes back as a result with OK false. Anything else
// going wrong fails the test, so a broken probe is not mistaken for a blocked
// connection.
func (c *probeClient) dial(t *testing.T, ctx context.Context, address string) dialResult {
	t.Helper()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+"/dial?address="+url.QueryEscape(address), nil)
	if err != nil {
		t.Fatalf("building probe request for %s: %v", address, err)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		t.Fatalf("calling probe /dial for %s: %v", address, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<10))
		t.Fatalf("probe /dial for %s returned %d: %s", address, resp.StatusCode, strings.TrimSpace(string(body)))
	}
	var out dialResult
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decoding probe /dial response for %s: %v", address, err)
	}
	return out
}

// setupDemoCounterTemplate provisions the per-test WorkerPool and substrate
// ActorTemplate from the substrate counter demo, returning the pool name and
// the template. The template lives in an atespace named after the test's k8s
// namespace, so its name needs no per-test suffix. SnapshotConfig is copied
// from the source, as the CRD-era setup did.
func setupDemoCounterTemplate(ctx context.Context, t *testing.T, clients *e2e.Clients, ns string) (string, *ateapi.ActorTemplate) {
	t.Helper()
	const poolName = "counter"
	at := e2e.CreateSubstrateCounterTemplate(ctx, t, clients, ns, e2e.SubstrateTemplateOptions{
		Atespace:     ns,
		Name:         "counter",
		PoolName:     poolName,
		PoolReplicas: 1,
		Labels:       map[string]string{"netpol-test": ns},
	})
	return poolName, at
}

func waitForActorRunning(ctx context.Context, t *testing.T, clients *e2e.Clients, atespace, actorName string) {
	t.Helper()
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := clients.SubstrateAPI.GetActor(ctx, &ateapi.GetActorRequest{
			Actor: &ateapi.ObjectRef{Atespace: atespace, Name: actorName},
		})
		if err == nil && resp.GetStatus().GetState() == ateapi.ActorState_ACTOR_STATE_RUNNING {
			t.Logf("Actor %q reached RUNNING state", actorName)
			return
		}
		time.Sleep(1 * time.Second)
	}
	t.Fatalf("timed out waiting for actor %q to reach RUNNING state", actorName)
}

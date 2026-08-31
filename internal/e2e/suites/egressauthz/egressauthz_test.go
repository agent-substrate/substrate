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

// Package egressauthz e2e-tests the egress gateway's front door: the two ways
// it refuses a caller that is not a running actor.
//
// Both tests are negative, and that is the whole of the package on purpose.
//   - TestGatewayRefusesANonActorWorkload needs a credential no test process
//     can mint. The probe's podidentity certificate is issued by kubelet from
//     a real signer, so presenting it proves the gateway's downstream
//     trusted_ca is the actor-identity CA and not merely some substrate
//     anchor. Get that wrong and every workload in the cluster can open a
//     tunnel.
//   - TestGatewayRefusesAnUnknownActor presents a cryptographically perfect
//     credential and is denied only by the control-plane lookup, so it proves
//     Envoy actually calls ext_proc on the CONNECT and honors a deny. A
//     gateway that authorizes on the certificate alone passes every other
//     test in the repo.
package egressauthz

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	"github.com/agent-substrate/substrate/internal/ateclient"
	"github.com/agent-substrate/substrate/internal/e2e"
	"github.com/agent-substrate/substrate/internal/portforward"
)

const (
	// Where the gateway's CA lives, fixed by hack/install-ate.sh.
	egressNamespace = "ate-system"

	probeName = "egressprobe"
)

func TestGatewayRefusesANonActorWorkload(t *testing.T) {
	ctx := context.Background()

	probe := sharedProbe(t, ctx)

	const sni = "podidentity.example.com"
	result := probe.handshakeAs(t, ctx, sni, podIdentityCredentialPath)
	if result.OK {
		t.Fatalf("the gateway opened a tunnel for the probe's podidentity credential; its front door is accepting workloads that are not actors")
	}
	if result.Stage != stageGatewayTLS {
		t.Fatalf("podidentity credential failed at stage %q, want %q -- the probe was stopped by something other than the front door, so this test is not checking it: %s",
			result.Stage, stageGatewayTLS, result.Error)
	}
	t.Logf("gateway refused the non-actor credential at its front door as expected: %s", result.Error)
}

// TestGatewayRefusesAnUnknownActor covers the check that only ext_proc can
// make. The credential here is cryptographically perfect -- signed by the real
// actor-identity CA, correct extension, correct purpose -- so Envoy completes
// the handshake, and the CONNECT is denied only because the control plane has
// no such actor. Without this, nothing distinguishes a gateway that authorizes
// on the certificate alone from one that authorizes on control-plane state, and
// the difference is whether a deleted actor's credential still works.
func TestGatewayRefusesAnUnknownActor(t *testing.T) {
	ctx := context.Background()

	probe := sharedProbe(t, ctx)

	const sni = "unknown.example.com"
	result := probe.handshakeAs(t, ctx, sni, unknownActorCredentialPath)
	if result.OK {
		t.Fatalf("the gateway tunneled for an actor the control plane has never heard of; the ext_proc identity check is not running")
	}
	// A 403 on the CONNECT, not a TLS failure: the certificate was accepted and
	// the identity it carries was rejected. A failure at stageGatewayTLS would
	// mean the request never reached ext_proc, and the denial would prove
	// nothing about the control-plane lookup.
	if result.Stage != stageConnect || result.ConnectStatus != http.StatusForbidden {
		t.Fatalf("unknown actor was refused at stage %q with CONNECT status %d, want %q and %d -- something other than the ext_proc identity check turned it away: %s",
			result.Stage, result.ConnectStatus, stageConnect, http.StatusForbidden, result.Error)
	}
	t.Logf("gateway denied the unknown actor at CONNECT as expected: %s", result.Error)
}

// probeClient talks to the in-cluster egress probe over a port-forward.
//
// The SNI each test passes is decorative: it would name the tunneled
// handshake, and neither test is allowed to open one. It travels only so the
// probe's logs and errors say which case they came from.
type probeClient struct {
	baseURL string
	http    *http.Client
}

var (
	probeOnce sync.Once
	probeVal  *probeClient
	probeErr  error
)

// sharedProbe returns the one probe pod the whole suite uses.
func sharedProbe(t *testing.T, ctx context.Context) *probeClient {
	t.Helper()
	probeOnce.Do(func() {
		// startProbe reports failures through t, which unwinds this goroutine
		// without returning. Leave something behind so the tests that run
		// afterwards fail pointing at the first one instead of dereferencing
		// nil.
		defer func() {
			if probeVal == nil && probeErr == nil {
				probeErr = errors.New("setup did not complete; see the failure reported by the first test that needed the probe")
			}
		}()
		probeVal = startProbe(t, ctx)
	})
	if probeErr != nil {
		t.Fatalf("starting the shared egress probe: %v", probeErr)
	}
	return probeVal
}

// startProbe creates the probe's namespace, mints its credentials there, builds
// and deploys the probe, waits for it to be ready, and returns a client for it.
func startProbe(t *testing.T, ctx context.Context) *probeClient {
	t.Helper()
	if _, err := e2e.CheckEnv("KO_DOCKER_REPO"); err != nil {
		t.Fatalf("CheckEnv failed: %v", err)
	}
	ns := e2e.CreateNamespace(t).Name

	provisionProbeCredentials(t, ctx, ns)
	root, err := e2e.FindRepoRoot()
	if err != nil {
		t.Fatalf("FindRepoRoot: %v", err)
	}

	tmpl, err := os.ReadFile(filepath.Join(root, "internal/e2e/fixtures/testserver/egressprobe.yaml.tmpl"))
	if err != nil {
		t.Fatalf("reading egressprobe manifest template: %v", err)
	}
	manifest := filepath.Join(t.TempDir(), "egressprobe.yaml")
	rendered := strings.ReplaceAll(string(tmpl), "${NAMESPACE}", ns)
	if err := os.WriteFile(manifest, []byte(rendered), 0o644); err != nil {
		t.Fatalf("writing rendered egressprobe manifest: %v", err)
	}

	applyArgs := []string{"ko", "apply", "-f", manifest}
	if e2e.KubeContext != "" {
		applyArgs = append(applyArgs, "--", "--context="+e2e.KubeContext)
	}
	e2e.RunCmdWithEnv(t, []string{"KO_CONFIG_PATH=" + root}, filepath.Join(root, "hack/run-tool.sh"), applyArgs...)

	waitForProbeReady(t, ctx, ns)

	config, err := ateclient.LoadConfig(e2e.KubeConfig, e2e.KubeContext)
	if err != nil {
		t.Fatalf("loading kubeconfig: %v", err)
	}
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		t.Fatalf("creating k8s client for port-forward: %v", err)
	}
	localPort, stop, err := portforward.ServicePortForward(ctx, config, clientset, ns, probeName, 8080)
	if err != nil {
		t.Fatalf("port-forwarding %s/%s: %v", ns, probeName, err)
	}
	e2e.RegisterSuiteCleanup(stop)

	return &probeClient{
		baseURL: fmt.Sprintf("http://127.0.0.1:%d", localPort),
		http:    &http.Client{Timeout: 90 * time.Second},
	}
}

func waitForProbeReady(t *testing.T, ctx context.Context, ns string) {
	t.Helper()
	const timeout = 3 * time.Minute
	deadline := time.Now().Add(timeout)
	var lastState string
	for time.Now().Before(deadline) {
		pod, err := e2e.GetClients().K8s.CoreV1().Pods(ns).Get(ctx, probeName, metav1.GetOptions{})
		switch {
		case err != nil:
			lastState = err.Error()
		case portforward.IsPodReady(pod):
			t.Logf("probe pod %s/%s is ready", ns, probeName)
			return
		default:
			lastState = describeProbeState(pod)
		}
		time.Sleep(2 * time.Second)
	}
	t.Fatalf("timed out after %v waiting for probe pod %s/%s to become ready: %s", timeout, ns, probeName, lastState)
}

func describeProbeState(pod *corev1.Pod) string {
	parts := []string{"phase=" + string(pod.Status.Phase)}
	for _, cs := range pod.Status.ContainerStatuses {
		switch {
		case cs.State.Waiting != nil:
			parts = append(parts, fmt.Sprintf("%s waiting: %s: %s", cs.Name, cs.State.Waiting.Reason, cs.State.Waiting.Message))
		case cs.State.Terminated != nil:
			parts = append(parts, fmt.Sprintf("%s terminated: %s: %s", cs.Name, cs.State.Terminated.Reason, cs.State.Terminated.Message))
		default:
			parts = append(parts, fmt.Sprintf("%s running, ready=%t", cs.Name, cs.Ready))
		}
	}
	return strings.Join(parts, "; ")
}

// handshakeAs asks the probe to complete one CONNECT plus inner TLS handshake
// for sni, presenting the credential at the given path. Every caller names one:
// which credential is presented is the variable both tests turn. A refusal
// comes back as a result with OK false, not as an error, because refusal is the
// outcome under test.
func (c *probeClient) handshakeAs(t *testing.T, ctx context.Context, sni, credential string) handshakeResult {
	t.Helper()
	endpoint := c.baseURL + "/handshake?sni=" + url.QueryEscape(sni) +
		"&credential-bundle=" + url.QueryEscape(credential)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		t.Fatalf("building probe request for %q: %v", sni, err)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		t.Fatalf("calling probe for %q: %v", sni, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<10))
		t.Fatalf("probe returned %d for %q: %s", resp.StatusCode, sni, strings.TrimSpace(string(body)))
	}
	var out handshakeResult
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decoding probe response for %q: %v", sni, err)
	}
	return out
}

// Stages a handshake can fail at, mirroring the probe's stage constants. Only
// the two the tests assert on are named here; the rest arrive as whatever
// string the probe sent and are printed in the failure.
const (
	stageGatewayTLS = "gateway_tls"
	stageConnect    = "connect"
)

// handshakeResult mirrors the probe's response body. It is duplicated rather
// than imported because the probe is package main.
type handshakeResult struct {
	SNI        string `json:"sni"`
	Credential string `json:"credential"`
	OK         bool   `json:"ok"`
	// Stage is where a failed handshake stopped. Asserting on it rather than
	// on Error is what keeps "the front door refused the certificate" and "the
	// door opened and ext_proc said no" from being the same test: they are
	// different hops, and their messages are only incidentally different.
	Stage string `json:"stage"`
	// ConnectStatus is the status the gateway answered the CONNECT with, set
	// only when Stage is stageConnect.
	ConnectStatus int    `json:"connect_status"`
	Error         string `json:"error"`
	ChainPEM      string `json:"chain_pem"`
}

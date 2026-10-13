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
//
// This file covers security invariants that require TWO actors running at the
// same time on distinct worker pods (replicas: 2 in the WorkerPool) to verify
// space invariance.
package security

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/internal/e2e"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	markerConcurrentA = dataMount + "/concurrent-marker-a"
	markerConcurrentB = dataMount + "/concurrent-marker-b"
)

type fetchResponse struct {
	Status string `json:"status"`
	Body   string `json:"body"`
	Error  string `json:"error"`
}

// TestConcurrentActorIsolation tests space invariants between two actors
// running concurrently on two distinct worker pods.
func TestConcurrentActorIsolation(t *testing.T) {
	env, err := e2e.CheckEnv("BUCKET_NAME", "KO_DOCKER_REPO")
	if err != nil {
		t.Fatalf("CheckEnv failed: %v", err)
	}
	ctx := context.Background()
	clients := e2e.GetClients()

	ns := deployConcurrentAtespaces(t, ctx, clients, env["BUCKET_NAME"])

	rc, err := e2e.NewRouterClient(ctx)
	if err != nil {
		t.Fatalf("NewRouterClient: %v", err)
	}
	defer rc.Close()

	// Both actors share the same name ("actor-a") in their respective
	// atespaces so the router must disambiguate live concurrent actors by
	// (atespace, actor) rather than actor name alone.
	const actorName = "actor-a"

	createAndResumeActor(t, ctx, clients, ns.a, actorName)
	createAndResumeActor(t, ctx, clients, ns.b, actorName)

	refA := resources.ActorRef{Atespace: ns.a, Name: actorName}
	refB := resources.ActorRef{Atespace: ns.b, Name: actorName}

	uidA := actorUID(t, ctx, clients, ns.a, actorName)
	uidB := actorUID(t, ctx, clients, ns.b, actorName)
	if uidA == uidB {
		t.Fatalf("actors %s and %s were assigned the same UID %q", refA, refB, uidA)
	}

	workerA := assignedWorker(t, ctx, clients, ns.a, actorName)
	workerB := assignedWorker(t, ctx, clients, ns.b, actorName)

	if workerA == workerB {
		t.Fatalf("actors landed on the same worker (%s); pool has replicas: 2, want distinct workers to test space isolation", workerA)
	}
	t.Logf("actor %s (uid %s) running on worker %s; actor %s (uid %s) running on worker %s",
		refA, uidA, workerA, refB, uidB, workerB)

	// NW-6 [space]: while two identically named actors are concurrently live in
	// different atespaces on distinct workers, the ingress router must deliver
	// each request to the actor in the atespace named in the request.
	t.Run("RouterDeliversOnlyToNamedActorInSpace", func(t *testing.T) {
		t.Log("The router must deliver a request only to the actor and atespace named in the request (T-14)")

		gotA := whoami(t, ctx, rc, refA)
		if gotA.Atespace != ns.a || gotA.File != actorName || gotA.UID != uidA {
			t.Fatalf("router delivered %s /whoami = %+v, want atespace=%q file=%q uid=%q",
				refA, gotA, ns.a, actorName, uidA)
		}

		gotB := whoami(t, ctx, rc, refB)
		if gotB.Atespace != ns.b || gotB.File != actorName || gotB.UID != uidB {
			t.Fatalf("router delivered %s /whoami = %+v, want atespace=%q file=%q uid=%q",
				refB, gotB, ns.b, actorName, uidB)
		}

		// Parallel concurrent routing: fire simultaneous requests to both
		// same-named actors to verify no crosstalk under concurrent traffic.
		const iterations = 10
		errs := make(chan string, iterations*2)
		var wg sync.WaitGroup
		for i := range iterations {
			wg.Add(2)
			go func(iter int) {
				defer wg.Done()
				got, err := tryWhoami(ctx, rc, refA)
				if err != nil {
					errs <- fmt.Sprintf("iter %d: parallel whoami for %s failed: %v", iter, refA, err)
					return
				}
				if got.Atespace != ns.a || got.File != actorName || got.UID != uidA {
					errs <- fmt.Sprintf("iter %d: parallel whoami for %s delivered wrong actor %+v, want atespace=%q file=%q uid=%q",
						iter, refA, got, ns.a, actorName, uidA)
				}
			}(i)
			go func(iter int) {
				defer wg.Done()
				got, err := tryWhoami(ctx, rc, refB)
				if err != nil {
					errs <- fmt.Sprintf("iter %d: parallel whoami for %s failed: %v", iter, refB, err)
					return
				}
				if got.Atespace != ns.b || got.File != actorName || got.UID != uidB {
					errs <- fmt.Sprintf("iter %d: parallel whoami for %s delivered wrong actor %+v, want atespace=%q file=%q uid=%q",
						iter, refB, got, ns.b, actorName, uidB)
				}
			}(i)
		}
		wg.Wait()
		close(errs)
		for msg := range errs {
			t.Errorf("%s", msg)
		}
	})

	// NW-2 [space]: an actor must not open a connection directly to another
	// actor across the pod network. The worker pool's NetworkPolicy admits
	// ingress only from atenet-router.
	t.Run("DirectActorToActorTrafficBlocked", func(t *testing.T) {
		t.Log("An actor must not open a connection directly to another actor (T-17)")

		// Actors have egress denied by default. Grant actor A an allow-all
		// EgressPolicy and verify with a positive control that actor A can
		// reach an in-cluster HTTP server through egress, so that failure to
		// reach worker B is attributable to the worker NetworkPolicy rather
		// than egress being off.
		e2e.EnsureEgressPolicy(t, ctx, clients, &ateapipb.ObjectRef{Atespace: ns.a, Name: actorName}, e2e.EgressAllowAll()...)

		controlTarget := e2e.DeployServerPod(t, ctx, e2e.ServerPod{
			Name:       "egress-control",
			ImportPath: "github.com/agent-substrate/substrate/internal/e2e/fixtures/testserver",
			Args:       []string{"http"},
			Port:       80,
			TargetPort: 8080,
		})
		controlURL := fmt.Sprintf("http://%s/healthz", controlTarget.Address())
		controlResp := probeFetchUntilReached(t, ctx, rc, refA, controlURL)
		if controlResp.Error != "" || controlResp.Status != "200" {
			t.Fatalf("control: actor %s egress fetch to %s = %+v, want status 200 and no error — actor egress is not working, so the worker-pod dial check below would be vacuous",
				refA, controlURL, controlResp)
		}

		// Positive control on actor B: confirm worker B is live and serving
		// actor B through the authorized ingress path (atenet-router).
		if gotB := whoami(t, ctx, rc, refB); gotB.UID != uidB {
			t.Fatalf("control: actor %s /whoami through router = %+v, want uid=%q", refB, gotB, uidB)
		}

		workerPodIP_B := getWorkerPodIP(t, ctx, clients, ns.b, actorName)
		t.Logf("discovered worker B pod IP: %s", workerPodIP_B)

		// Worker pods listen on 443 (atunnel HTTPS), 8443 (atunnel CONNECT),
		// and 8080 (ateom /readyz); see buildDeploymentApplyConfig in
		// cmd/atecontroller/internal/controllers/workerpool_apply.go.
		// Dial all three over cleartext HTTP so atenet-egress matches
		// EgressAllowAll()'s all-ports HttpRule on the IP literal and attempts
		// a TCP connection to workerPodIP_B:<port> on the pod network (an
		// https:// request to an IP literal carries no SNI per RFC 6066 and
		// would be rejected at atenet-egress before any packet is sent toward
		// worker B).
		for _, port := range []int{443, 8443, 8080} {
			targetURL := fmt.Sprintf("http://%s:%d/readyz", workerPodIP_B, port)
			got := probeFetch(t, ctx, rc, refA, targetURL)
			if !isBlockedWorkerDial(got) {
				t.Errorf("actor %s reached worker B at %s (status %q, body %q, error %q) — NetworkPolicy did not block traffic to worker B across the pod network",
					refA, targetURL, got.Status, got.Body, got.Error)
			} else {
				t.Logf("actor %s dial to %s blocked as expected (status=%q error=%q body=%q)",
					refA, targetURL, got.Status, got.Error, got.Body)
			}
		}
	})

	// ST-1 [space]: while two actors run concurrently in space, their
	// durable-dir mounts must be strictly isolated.
	t.Run("ConcurrentDurableMountIsolation", func(t *testing.T) {
		t.Log("Two concurrent actors must not read or write each other's durable-dir volume")

		writeFile(t, ctx, rc, refA, markerConcurrentA)
		writeFile(t, ctx, rc, refB, markerConcurrentB)

		if got := readFile(t, ctx, rc, refA, markerConcurrentA); got.Content != writtenByProbe {
			t.Fatalf("control: actor %s reading its OWN %s = %q (error %q), want %q",
				refA, markerConcurrentA, got.Content, got.Error, writtenByProbe)
		}
		if got := readFile(t, ctx, rc, refB, markerConcurrentB); got.Content != writtenByProbe {
			t.Fatalf("control: actor %s reading its OWN %s = %q (error %q), want %q",
				refB, markerConcurrentB, got.Content, got.Error, writtenByProbe)
		}

		if got := readFile(t, ctx, rc, refA, markerConcurrentB); got.Error == "" {
			t.Errorf("actor %s read %s, belonging to concurrent actor %s: content %q — durable-dir volume isolation failed in space",
				refA, markerConcurrentB, refB, got.Content)
		}
		if got := readFile(t, ctx, rc, refB, markerConcurrentA); got.Error == "" {
			t.Errorf("actor %s read %s, belonging to concurrent actor %s: content %q — durable-dir volume isolation failed in space",
				refB, markerConcurrentA, refA, got.Content)
		}
	})
}

func deployConcurrentAtespaces(t *testing.T, ctx context.Context, clients *e2e.Clients, bucket string) atespaces {
	t.Helper()
	a, _ := e2e.DeploySubstrateFixture(t, ctx, clients, e2e.SubstrateFixtureManifests{
		Pool:     "internal/e2e/fixtures/security/concurrent-a-pool.yaml.tmpl",
		Template: "internal/e2e/fixtures/security/concurrent-template.yaml.tmpl",
	}, bucket, "sec-conc-a", nil)
	b, _ := e2e.DeploySubstrateFixture(t, ctx, clients, e2e.SubstrateFixtureManifests{
		Pool:     "internal/e2e/fixtures/security/concurrent-b-namespace.yaml.tmpl",
		Template: "internal/e2e/fixtures/security/concurrent-template.yaml.tmpl",
	}, bucket, "sec-conc-b", nil)
	if a == b {
		t.Fatalf("both fixtures rendered to atespace %q", a)
	}
	return atespaces{a: a, b: b}
}

func getWorkerPodIP(t *testing.T, ctx context.Context, clients *e2e.Clients, atespace, actorName string) string {
	t.Helper()
	actor, err := clients.SubstrateAPI.GetActor(ctx, &ateapipb.GetActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: atespace, Name: actorName},
	})
	if err != nil {
		t.Fatalf("GetActor %s/%s: %v", atespace, actorName, err)
	}
	wa := actor.GetStatus().GetWorkerAssignment()
	if wa == nil || wa.GetWorkerPod() == "" || wa.GetWorkerNamespace() == "" {
		t.Fatalf("actor %s/%s has no worker assignment: %+v", atespace, actorName, actor.GetStatus())
	}
	pod, err := clients.K8s.CoreV1().Pods(wa.GetWorkerNamespace()).Get(ctx, wa.GetWorkerPod(), metav1.GetOptions{})
	if err != nil {
		t.Fatalf("getting worker pod %s/%s: %v", wa.GetWorkerNamespace(), wa.GetWorkerPod(), err)
	}
	if pod.Status.PodIP == "" {
		t.Fatalf("worker pod %s/%s has no PodIP", wa.GetWorkerNamespace(), wa.GetWorkerPod())
	}
	return pod.Status.PodIP
}

// tryWhoami calls the probe's /whoami endpoint and returns an error instead of
// calling t.Fatalf, making it safe to call from child goroutines.
func tryWhoami(ctx context.Context, rc *e2e.RouterClient, actor resources.ActorRef) (whoamiResponse, error) {
	resp, err := rc.Get(ctx, actor, "/whoami")
	if err != nil {
		return whoamiResponse{}, fmt.Errorf("GET /whoami for %s: %w", actor, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return whoamiResponse{}, fmt.Errorf("GET /whoami for %s: status %d, body %q", actor, resp.StatusCode, body)
	}
	var out whoamiResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return whoamiResponse{}, fmt.Errorf("decoding /whoami for %s: %w", actor, err)
	}
	return out, nil
}

// probeFetch asks actor's probe to fetch targetURL using system roots (the
// security fixture template does not mount the MITM egress trust bundle).
func probeFetch(t *testing.T, ctx context.Context, rc *e2e.RouterClient, actor resources.ActorRef, targetURL string) fetchResponse {
	t.Helper()
	endpoint := "/fetch?roots=system&url=" + url.QueryEscape(targetURL)
	resp, err := rc.Get(ctx, actor, endpoint)
	if err != nil {
		t.Fatalf("GET %s for %s: %v", endpoint, actor, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("GET %s for %s: status %d, body %q", endpoint, actor, resp.StatusCode, body)
	}
	var out fetchResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decoding %s for %s: %v", endpoint, actor, err)
	}
	return out
}

// probeFetchUntilReached retries probeFetch until it returns 200 OK or the
// 30s deadline expires, riding out initial service endpoint or policy
// propagation.
func probeFetchUntilReached(t *testing.T, ctx context.Context, rc *e2e.RouterClient, actor resources.ActorRef, targetURL string) fetchResponse {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		got := probeFetch(t, ctx, rc, actor, targetURL)
		if (got.Error == "" && got.Status == "200") || time.Now().After(deadline) {
			return got
		}
		time.Sleep(time.Second)
	}
}

// isBlockedWorkerDial reports whether a probeFetch result shows that the
// outbound connection to the worker pod was blocked before reaching any
// listener on the worker.
//
// Actor TCP egress traverses atunnel -> atenet-egress, where Envoy terminates
// the downstream HTTP connection and dials the target IP:port upstream. When
// the worker's NetworkPolicy drops the SYN packets from atenet-egress, Envoy's
// upstream dial times out and returns HTTP 503 ("connection timeout" /
// "upstream connect error") or a transport error (got.Error != ""). By
// contrast, if the packet reaches the worker pod, port 8080 (/readyz) returns
// 200 and ports 443/8443 (atunnel TLS listeners) return 400 ("Client sent an
// HTTP request to an HTTPS server").
func isBlockedWorkerDial(got fetchResponse) bool {
	if got.Error != "" {
		return true
	}
	if (got.Status == "502" || got.Status == "503" || got.Status == "504") &&
		(strings.Contains(got.Body, "connection timeout") || strings.Contains(got.Body, "upstream connect error")) {
		return true
	}
	return false
}

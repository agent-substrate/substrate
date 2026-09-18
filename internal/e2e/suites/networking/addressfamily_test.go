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

package networking

import (
	"context"
	"encoding/json"
	"net/http"
	"net/netip"
	"testing"

	"github.com/agent-substrate/substrate/internal/e2e"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// netInfo mirrors the counter demo's /netinfo response.
type netInfo struct {
	Interfaces []struct {
		Name  string   `json:"name"`
		CIDRs []string `json:"cidrs"`
	} `json:"interfaces"`
	IPv4 []string `json:"ipv4"`
	IPv6 []string `json:"ipv6"`
}

// TestActorAddressFamilies asserts that an actor gets exactly the address
// families its worker pod has.
//
// It is written as an equivalence rather than "expect IPv6" so it means
// something on every cluster. On a dual-stack one it is the positive check that
// the actor was actually given IPv6; on the IPv4-only clusters CI runs today it
// asserts the opposite -- that the actor was *not* given an address its pod
// cannot route -- which guards the gate in ateomnet.SetupActorNetwork and the
// dangerous direction of the micro-VM restore reconcile, where a dual-stack
// golden lands on an IPv4-only pod.
//
// Both sandbox classes run this via E2E_SANDBOX_CLASS, and they reach the
// answer by completely different routes: gVisor's runsc adopts the interior
// netns wholesale, while the micro-VM guest has to be told over the kata-agent
// channel. Only the actor can report what it ended up with, which is what
// /netinfo is for.
func TestActorAddressFamilies(t *testing.T) {
	ctx := context.Background()
	actorName, actor := createAndResumeSubstrateActor(t, ctx, "family", e2e.SubstrateCounterFixture())

	assignment := actor.GetStatus().GetWorkerAssignment()
	if assignment.GetWorkerNamespace() == "" || assignment.GetWorkerPod() == "" {
		t.Skipf("resumed Actor has no worker pod assignment, so there is nothing to compare against: %+v", actor)
	}
	podV4, podV6 := workerPodFamilies(t, ctx, assignment)

	router := mustRouterClient(t, ctx)
	defer router.Close()
	actorRef := resources.ActorRef{Atespace: networkingAtespace, Name: actorName}
	body := waitForRouteReady(t, "Actor /netinfo", func() (*http.Response, error) {
		return router.Get(ctx, actorRef, "/netinfo")
	})

	var info netInfo
	if err := json.Unmarshal([]byte(body), &info); err != nil {
		t.Fatalf("parsing /netinfo response %q: %v", body, err)
	}
	for _, iface := range info.Interfaces {
		t.Logf("actor interface %s: %v", iface.Name, iface.CIDRs)
	}

	// An actor with no IPv4 has failed at something far more basic than address
	// families, and would otherwise let the IPv6 legs pass vacuously.
	if len(info.IPv4) == 0 {
		t.Errorf("actor has no IPv4 address; interfaces: %+v", info.Interfaces)
	}
	if got, want := len(info.IPv6) > 0, len(podV6) > 0; got != want {
		t.Errorf("actor has IPv6 = %v, but its worker pod %s/%s has IPv6 = %v\n\tpod IPv4: %v\n\tpod IPv6: %v\n\tactor IPv4: %v\n\tactor IPv6: %v\n\tactor interfaces: %+v",
			got, assignment.GetWorkerNamespace(), assignment.GetWorkerPod(), want,
			podV4, podV6, info.IPv4, info.IPv6, info.Interfaces)
	}
}

// workerPodFamilies splits the assigned worker pod's addresses by family. Pod
// IPs are already the global addresses the CNI handed out, so unlike the
// actor's own interfaces there is no link-local to filter -- but they are
// classified by parsing rather than by counting Status.PodIPs, because a
// single-stack pod on a dual-stack cluster still has exactly one entry.
func workerPodFamilies(t *testing.T, ctx context.Context, assignment *ateapipb.WorkerAssignment) (v4, v6 []string) {
	t.Helper()
	pod, err := e2e.GetClients().K8s.CoreV1().Pods(assignment.GetWorkerNamespace()).
		Get(ctx, assignment.GetWorkerPod(), metav1.GetOptions{})
	if err != nil {
		t.Fatalf("getting worker pod %s/%s: %v", assignment.GetWorkerNamespace(), assignment.GetWorkerPod(), err)
	}
	for _, podIP := range pod.Status.PodIPs {
		addr, err := netip.ParseAddr(podIP.IP)
		if err != nil {
			t.Fatalf("worker pod %s/%s has unparseable IP %q: %v", pod.Namespace, pod.Name, podIP.IP, err)
		}
		if addr.Unmap().Is4() {
			v4 = append(v4, addr.String())
		} else {
			v6 = append(v6, addr.String())
		}
	}
	t.Logf("worker pod %s/%s has IPv4 %v, IPv6 %v", pod.Namespace, pod.Name, v4, v6)
	return v4, v6
}

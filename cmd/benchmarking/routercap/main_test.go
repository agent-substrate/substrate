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

// Tests for flag parsing and for the runner configuration the binary assembles.

package main

import (
	"context"
	"io"
	"strings"
	"testing"

	"github.com/agent-substrate/substrate/internal/benchmarking/routercap"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func TestParsePortRange(t *testing.T) {
	cases := []struct {
		name          string
		in            string
		wantLow, high int
		wantSource    string
		wantErr       bool
	}{
		// The sysctl's own output is tab-separated; accepting it directly means
		// run.sh can pass what it read rather than reformatting it and getting
		// the reformatting wrong.
		{name: "SysctlForm", in: "32768\t60999", wantLow: 32768, high: 60999, wantSource: routercap.PortRangeMeasured},
		{name: "DashForm", in: "32768-60999", wantLow: 32768, high: 60999, wantSource: routercap.PortRangeMeasured},
		{name: "SpaceForm", in: "1024 65535", wantLow: 1024, high: 65535, wantSource: routercap.PortRangeMeasured},
		// Unset must say "assumed", not quietly look like a measurement.
		{name: "Empty", in: "", wantLow: 32768, high: 60999, wantSource: routercap.PortRangeAssumed},
		{name: "OneNumber", in: "32768", wantErr: true},
		{name: "Reversed", in: "60999-32768", wantErr: true},
		{name: "NotNumbers", in: "low-high", wantErr: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parsePortRange(tc.in)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("parsePortRange(%q) = %+v, want an error", tc.in, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("parsePortRange(%q): %v", tc.in, err)
			}
			if got.Low != tc.wantLow || got.High != tc.high || got.Source != tc.wantSource {
				t.Errorf("parsePortRange(%q) = %+v, want %d-%d from %q", tc.in, got, tc.wantLow, tc.high, tc.wantSource)
			}
		})
	}
}

func TestCheckConcurrencyRefusesAMislabeledArm(t *testing.T) {
	// Envoy left to its own devices sizes worker threads from the node's core
	// count, so a 10-core arm can run 176 event loops and measure CFS
	// throttling. Labeling that series "10 cores" is worse than no series.
	const body = "envoy_server_concurrency{} 40\n"
	c := &routercap.EnvoyClient{Fetch: func(context.Context) (io.ReadCloser, error) {
		return io.NopCloser(strings.NewReader(body)), nil
	}}

	if err := checkConcurrency(context.Background(), c, 40); err != nil {
		t.Errorf("matching arm rejected: %v", err)
	}
	err := checkConcurrency(context.Background(), c, 10)
	if err == nil {
		t.Fatal("a 10-core arm running 40 Envoy workers was accepted")
	}
	if !strings.Contains(err.Error(), "40") || !strings.Contains(err.Error(), "10") {
		t.Errorf("error %q does not name both the observed and the intended concurrency", err)
	}
	// --arm unset is the smoke-test path: nothing was patched, so there is
	// nothing to disagree with.
	if err := checkConcurrency(context.Background(), c, 0); err != nil {
		t.Errorf("unset arm rejected: %v", err)
	}
}

func TestResolveOutputDir(t *testing.T) {
	t.Run("ExplicitWins", func(t *testing.T) {
		c := &config{outputDir: "/tmp/here", dest: "/tmp/root", name: "routercap", tag: "t1", arm: 40}
		got, err := c.resolveOutputDir()
		if err != nil || got != "/tmp/here" {
			t.Errorf("got (%q, %v), want /tmp/here", got, err)
		}
	})
	t.Run("DestBuildsTheArmPath", func(t *testing.T) {
		c := &config{dest: "/tmp/root", name: "routercap", tag: "t1", arm: 40}
		got, err := c.resolveOutputDir()
		if err != nil || got != "/tmp/root/routercap/t1/arm-40c" {
			t.Errorf("got (%q, %v), want /tmp/root/routercap/t1/arm-40c", got, err)
		}
	})
	t.Run("RemoteDestIsRefusedNotIgnored", func(t *testing.T) {
		// Honoring a gs:// --dest by writing somewhere local would lose the
		// run. Upload is not wired yet, so say so.
		c := &config{dest: "gs://bucket/results", name: "routercap", arm: 40}
		if _, err := c.resolveOutputDir(); err == nil {
			t.Fatal("a remote --dest was silently accepted")
		}
	})
	t.Run("NeitherIsAnError", func(t *testing.T) {
		if _, err := (&config{}).resolveOutputDir(); err == nil {
			t.Fatal("a run with nowhere to write was accepted")
		}
	})
	t.Run("StdoutIsAWholeOutput", func(t *testing.T) {
		// How the in-cluster Job reports: nothing can read files back out of a
		// distroless container.
		got, err := (&config{recordsToStdout: true}).resolveOutputDir()
		if err != nil || got != "" {
			t.Errorf("got (%q, %v), want no directory and no error", got, err)
		}
	})
}

func testPod(ns, name, node string, containers ...string) *corev1.Pod {
	p := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: ns, Name: name,
			Labels: map[string]string{"ate.dev/worker-pool": "benchmark-ateom", "app": name},
		},
		Status: corev1.PodStatus{Phase: corev1.PodRunning, PodIP: "10.0.0.1"},
		Spec:   corev1.PodSpec{NodeName: node},
	}
	for _, c := range containers {
		p.Spec.Containers = append(p.Spec.Containers, corev1.Container{Name: c})
	}
	return p
}

func TestResolveTargetsCoversEveryNodeTheRunWatches(t *testing.T) {
	// A node missing from the scrape list leaves its guard permanently silent,
	// which reads exactly like the guard passing.
	router := routercap.PodRef{
		Namespace: "ate-system", Name: "atenet-router-7d9", IP: "10.0.0.5", Node: "router-node",
		Containers: []string{"envoy", "atenet-router"},
	}
	cs := fake.NewSimpleClientset(
		testPod("ate-system", "atenet-router-7d9", "router-node", "envoy", "atenet-router"),
		testPod("ate-system", "ate-api-server-1", "system-node", "ate-api-server"),
		testPod("benchmark-workloads", "worker-1", "worker-node-a", "ateom"),
		testPod("benchmark-workloads", "worker-2", "worker-node-b", "ateom"),
	)
	cfg := &config{
		routerNamespace:  "ate-system",
		workerNamespace:  "benchmark-workloads",
		workerSelector:   "ate.dev/worker-pool",
		loadgenPod:       "routercap-runner-abc",
		loadgenNS:        "benchmarking",
		loadgenNode:      "loadgen-node",
		loadgenContainer: "routercap",
		guards:           routercap.DefaultGuardConfig(),
	}
	placement := map[string]string{}

	targets, nodes, err := resolveTargets(context.Background(), cs, cfg, []routercap.PodRef{router}, placement)
	if err != nil {
		t.Fatalf("resolveTargets: %v", err)
	}

	byRole := map[string]int{}
	for _, tg := range targets {
		byRole[tg.Role]++
	}
	want := map[string]int{
		routercap.RoleEnvoy:        1,
		routercap.RoleSidecar:      1,
		routercap.RoleLoadgen:      1,
		routercap.RoleControlPlane: 1, // the api-server; the router pod itself is excluded
		routercap.RoleWorker:       2,
	}
	for role, n := range want {
		if byRole[role] != n {
			t.Errorf("role %s has %d targets, want %d", role, byRole[role], n)
		}
	}

	set := map[string]bool{}
	for _, n := range nodes {
		set[n] = true
	}
	for _, n := range []string{"router-node", "system-node", "worker-node-a", "worker-node-b", "loadgen-node"} {
		if !set[n] {
			t.Errorf("node %s is not scraped, so every container on it is invisible", n)
		}
	}

	// Counted from the cluster, not configured: the per-worker connection-rate
	// guard divides a cluster-wide rate by this, so a stale flag would move the
	// threshold without anyone noticing.
	if cfg.guards.WorkerPods != 2 {
		t.Errorf("guard worker pods = %d, want the 2 that were found", cfg.guards.WorkerPods)
	}
	if got := placement[routercap.RoleWorker]; got != "worker-node-a,worker-node-b" {
		t.Errorf("worker placement = %q, want both nodes recorded", got)
	}
}

func TestResolveTargetsWithoutTheDownwardAPI(t *testing.T) {
	// Run outside a pod with no --loadgen-* flags: the loadgen target is
	// absent, and setup turns that into a startup failure rather than a
	// silently disabled guard.
	router := routercap.PodRef{
		Namespace: "ate-system", Name: "r", IP: "10.0.0.5", Node: "router-node",
		Containers: []string{"envoy", "atenet-router"},
	}
	cs := fake.NewSimpleClientset(testPod("ate-system", "r", "router-node", "envoy", "atenet-router"))
	cfg := &config{routerNamespace: "ate-system", workerNamespace: "benchmark-workloads", guards: routercap.DefaultGuardConfig()}

	targets, _, err := resolveTargets(context.Background(), cs, cfg, []routercap.PodRef{router}, map[string]string{})
	if err != nil {
		t.Fatalf("resolveTargets: %v", err)
	}
	if hasRole(targets, routercap.RoleLoadgen) {
		t.Error("a loadgen target was invented from nothing")
	}
}

// Copyright 2026 The Agent Substrate Authors
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

package steps

import (
	"reflect"
	"slices"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	"k8s.io/apimachinery/pkg/labels"

	"go.ate.dev/substrate/cmd/ate-setup/internal/config"
)

func loadWorkloads(t *testing.T, cfg config.Config) (systemInstall, map[string][]corev1.Container) {
	t.Helper()
	in := loadSystemInstall(t, &cfg)
	workloads := make(map[string][]corev1.Container, len(in.pods))
	for _, pod := range in.pods {
		workloads[pod.workload] = pod.spec.Containers
	}
	return in, workloads
}

func findPolicyByName(t *testing.T, policies []networkingv1.NetworkPolicy, name string) networkingv1.NetworkPolicy {
	t.Helper()
	idx := slices.IndexFunc(policies, func(np networkingv1.NetworkPolicy) bool { return np.Name == name })
	if idx < 0 {
		t.Fatalf("NetworkPolicy/%s not found", name)
	}
	return policies[idx]
}

func findContainerByName(t *testing.T, workloads map[string][]corev1.Container, workload, container string) corev1.Container {
	t.Helper()
	cs, ok := workloads[workload]
	if !ok {
		t.Fatalf("workload %s not found", workload)
	}
	idx := slices.IndexFunc(cs, func(c corev1.Container) bool { return c.Name == container })
	if idx < 0 {
		t.Fatalf("container %q not found in %s", container, workload)
	}
	return cs[idx]
}

// checkPprofContainer asserts whether c exposes TCP port pprof:6060 and the
// expected pprof/contention environment variables without duplicates or extras.
func checkPprofContainer(t *testing.T, workload string, c corev1.Container, wantPort bool, wantEnv map[string]string) {
	t.Helper()
	hasPort := slices.ContainsFunc(c.Ports, func(p corev1.ContainerPort) bool {
		return p.Name == "pprof" && p.ContainerPort == 6060 && (p.Protocol == "" || p.Protocol == corev1.ProtocolTCP)
	})
	if wantPort && !hasPort {
		t.Errorf("%s container %q missing TCP port pprof:6060 (got %+v)", workload, c.Name, c.Ports)
	}
	if !wantPort {
		for _, p := range c.Ports {
			if p.Name == "pprof" || p.ContainerPort == 6060 {
				t.Errorf("%s container %q unexpectedly exposes port %+v", workload, c.Name, p)
			}
		}
	}

	envs := make(map[string]string, len(c.Env))
	for _, e := range c.Env {
		if _, dup := envs[e.Name]; dup {
			t.Errorf("%s container %q has duplicate env var %q", workload, c.Name, e.Name)
		}
		envs[e.Name] = e.Value
		isPprof := strings.HasPrefix(e.Name, "ATE_PPROF_") ||
			strings.HasPrefix(e.Name, "ATE_WORKER_PPROF") ||
			e.Name == "GRPC_GO_SERVER_GOROUTINE_LABELS"
		if _, expected := wantEnv[e.Name]; isPprof && !expected {
			t.Errorf("%s container %q has unexpected env var %s=%q", workload, c.Name, e.Name, e.Value)
		}
	}
	for k, want := range wantEnv {
		if got, ok := envs[k]; !ok {
			t.Errorf("%s container %q missing env var %s (want %q)", workload, c.Name, k, want)
		} else if got != want {
			t.Errorf("%s container %q env %s = %q, want %q", workload, c.Name, k, got, want)
		}
	}
}

func checkPprofTargets(t *testing.T, in systemInstall, workloads map[string][]corev1.Container, cfg config.Config, wantPprof, wantContention bool) {
	t.Helper()
	obs := findPolicyByName(t, in.policies, "ate-system-observability")
	credPod := in.pod(t, "k8s-credential-provider")
	if mustSelector(t, &obs.Spec.PodSelector).Matches(labels.Set(credPod.labels)) {
		t.Errorf("ate-system-observability selects k8s-credential-provider")
	}
	for _, c := range credPod.spec.Containers {
		checkPprofContainer(t, credPod.workload, c, false, nil)
	}

	grpcEnv := map[string]string{}
	ctrlEnv := map[string]string{}
	if wantPprof {
		grpcEnv["ATE_PPROF_LISTEN_ADDR"] = ":6060"
		grpcEnv["GRPC_GO_SERVER_GOROUTINE_LABELS"] = "grpc.method=true"
		ctrlEnv["ATE_PPROF_LISTEN_ADDR"] = ":6060"
		ctrlEnv["ATE_WORKER_PPROF"] = "true"
	}
	if wantContention {
		grpcEnv["ATE_PPROF_MUTEX_FRACTION"] = "10"
		grpcEnv["ATE_PPROF_BLOCK_RATE"] = "1000000"
		ctrlEnv["ATE_PPROF_MUTEX_FRACTION"] = "10"
		ctrlEnv["ATE_PPROF_BLOCK_RATE"] = "1000000"
		ctrlEnv["ATE_WORKER_PPROF_MUTEX_FRACTION"] = "10"
		ctrlEnv["ATE_WORKER_PPROF_BLOCK_RATE"] = "1000000"
	}

	targets := []struct {
		workload, container string
		wantEnv             map[string]string
		skip                bool
	}{
		{"Deployment/ate-api-server", "ate-api-server", grpcEnv, false},
		{"Deployment/ate-controller", "ate-controller", ctrlEnv, false},
		{"DaemonSet/atelet", "atelet", grpcEnv, cfg.FakeDataPlane},
		{"Deployment/atenet-router", "atenet-router", grpcEnv, cfg.Router == config.RouterAgentgateway},
		{"Deployment/atenet-egress", "ext-proc", grpcEnv, cfg.Router == config.RouterAgentgateway},
	}
	for _, tgt := range targets {
		if tgt.skip {
			cs := workloads[tgt.workload]
			if len(cs) == 0 {
				t.Errorf("workload %s has 0 containers, want >= 1", tgt.workload)
				continue
			}
			if strings.HasPrefix(tgt.workload, "Deployment/atenet-") && (len(cs) != 1 || cs[0].Name != "agentgateway") {
				t.Errorf("%s containers = %+v, want single agentgateway container", tgt.workload, cs)
			}
			for _, c := range cs {
				checkPprofContainer(t, tgt.workload, c, false, nil)
			}
			continue
		}
		checkPprofContainer(t, tgt.workload, findContainerByName(t, workloads, tgt.workload, tgt.container), wantPprof, tgt.wantEnv)
	}
}

func checkPprofBaselinePreserved(t *testing.T, got, baseline map[string][]corev1.Container) {
	t.Helper()
	for workload, baseContainers := range baseline {
		for _, bc := range baseContainers {
			gc := findContainerByName(t, got, workload, bc.Name)
			for _, bp := range bc.Ports {
				if !slices.Contains(gc.Ports, bp) {
					t.Errorf("%s container %q dropped baseline port %+v", workload, bc.Name, bp)
				}
			}
			for _, be := range bc.Env {
				if !slices.ContainsFunc(gc.Env, func(ge corev1.EnvVar) bool { return reflect.DeepEqual(ge, be) }) {
					t.Errorf("%s container %q dropped baseline env var %q", workload, bc.Name, be.Name)
				}
			}
		}
	}
}

// TestPprofComponents verifies pprof and pprof-contention port/env placement,
// excluded workloads, and baseline port/env preservation under --pprof.
func TestPprofComponents(t *testing.T) {
	cases := []struct {
		name           string
		cfg            config.Config
		wantPprof      bool
		wantContention bool
		checkBaseline  bool
	}{
		{"gke-envoy", config.Config{Router: config.RouterEnvoy, Pprof: config.PprofOn}, true, false, true},
		{"gke-envoy-contention", config.Config{Router: config.RouterEnvoy, Pprof: config.PprofContention}, true, true, false},
		{"gke-agentgateway-contention", config.Config{Router: config.RouterAgentgateway, Pprof: config.PprofContention}, true, true, false},
		{"gke-envoy-fake-contention", config.Config{Router: config.RouterEnvoy, FakeDataPlane: true, Pprof: config.PprofContention}, true, true, false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			in, workloads := loadWorkloads(t, tc.cfg)
			checkPprofTargets(t, in, workloads, tc.cfg, tc.wantPprof, tc.wantContention)
			if tc.checkBaseline {
				baseCfg := tc.cfg
				baseCfg.Pprof = config.PprofOff
				_, baseline := loadWorkloads(t, baseCfg)
				checkPprofBaselinePreserved(t, workloads, baseline)
			}
		})
	}
}

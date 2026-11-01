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

package steps

import (
	"errors"
	"os"
	"slices"
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/kubernetes/fake"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"

	atev1 "go.ate.dev/substrate/api/infra/k8s/ate/v1"
	"go.ate.dev/substrate/cmd/ate-setup/internal/config"
	"go.ate.dev/substrate/cmd/ate-setup/internal/kube"
)

// deploy_locust.sh takes these as flags only, so anything not forwarded here is
// silently lost. The expected lists are what deploy_benchmarks in the shell
// installer assembled.
func TestDeployLocustArgs(t *testing.T) {
	opts := BenchmarkOptions{WorkerCount: 4, SandboxClass: config.SandboxClassGvisor}

	for _, tc := range []struct {
		name         string
		otlpEndpoint string
		actorMemory  string
		want         []string
	}{
		{
			name: "neither configured",
			want: []string{"--deploy", "--worker-count", "4", "--sandbox-class", "gvisor"},
		},
		{
			name:         "otlp endpoint only",
			otlpEndpoint: "http://otel-collector.ate-system.svc:4317",
			want: []string{"--deploy", "--worker-count", "4", "--sandbox-class", "gvisor",
				"--otlp-endpoint", "http://otel-collector.ate-system.svc:4317"},
		},
		{
			name:        "actor memory only",
			actorMemory: "256Mi",
			want: []string{"--deploy", "--worker-count", "4", "--sandbox-class", "gvisor",
				"--actor-memory", "256Mi"},
		},
		{
			name:         "both configured",
			otlpEndpoint: "http://otel-collector.ate-system.svc:4317",
			actorMemory:  "256Mi",
			want: []string{"--deploy", "--worker-count", "4", "--sandbox-class", "gvisor",
				"--otlp-endpoint", "http://otel-collector.ate-system.svc:4317",
				"--actor-memory", "256Mi"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := deployLocustArgs(opts, tc.otlpEndpoint, tc.actorMemory)
			if !slices.Equal(got, tc.want) {
				t.Errorf("deployLocustArgs() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestDeployBenchmarksRequiresMicroVMSandboxConfig(t *testing.T) {
	controller := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Namespace: NamespaceAteSystem, Name: "ate-controller"},
	}
	microvmSandboxConfig := &unstructured.Unstructured{}
	microvmSandboxConfig.SetGroupVersionKind(sandboxConfigGVK)
	microvmSandboxConfig.SetName(microvmSandboxConfigName)

	newEnv := func(t *testing.T, dyn ...runtime.Object) *Env {
		t.Helper()
		cs := fake.NewClientset(controller)
		cs.Fake.Resources = []*metav1.APIResourceList{
			{
				GroupVersion: atev1.GroupVersion.String(),
				APIResources: []metav1.APIResource{{Name: "sandboxconfigs", Kind: "SandboxConfig", Namespaced: false}},
			},
		}
		dc := dynamicfake.NewSimpleDynamicClient(clientgoscheme.Scheme, dyn...)
		return &Env{
			Cfg:  &config.Config{Root: t.TempDir()},
			Kube: kube.NewFromClients(nil, cs, dc, cs.Discovery()),
		}
	}

	t.Run("microvm without sandboxconfig fails with install hint", func(t *testing.T) {
		e := newEnv(t)
		err := e.DeployBenchmarks(t.Context(), BenchmarkOptions{WorkerCount: 1, SandboxClass: config.SandboxClassMicrovm})
		if err == nil || !strings.Contains(err.Error(), "deploy sandboxconfig --sandbox-classes=microvm") {
			t.Fatalf("DeployBenchmarks() = %v, want missing sandboxconfig/microvm hint", err)
		}
	})

	t.Run("microvm with sandboxconfig passes preflight", func(t *testing.T) {
		e := newEnv(t, microvmSandboxConfig)
		err := e.DeployBenchmarks(t.Context(), BenchmarkOptions{WorkerCount: 1, SandboxClass: config.SandboxClassMicrovm})
		if !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("DeployBenchmarks() = %v, want script ErrNotExist after passing preflight", err)
		}
	})
}

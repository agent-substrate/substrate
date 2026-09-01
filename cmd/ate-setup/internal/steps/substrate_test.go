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
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"google.golang.org/grpc"

	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/config"
	"github.com/agent-substrate/substrate/internal/ateclient"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

// TestActorTemplateFromManifestDemos parses the protojson ActorTemplate
// manifests that only the shell installers deploy; the manifests of demos
// registered with ate-setup are covered by the demos package's render test.
// Substitutions mirror what the deploying script performs.
func TestActorTemplateFromManifestDemos(t *testing.T) {
	root, err := config.RepoRoot()
	if err != nil {
		t.Fatalf("RepoRoot: %v", err)
	}

	// The values benchmarking/workloads/deploy.sh substitutes, beyond the
	// ${BUCKET_NAME} every installer provides.
	benchmarkValues := map[string]string{
		"${ACTOR_MEMORY}":        "256Mi",
		"${OTLP_ENDPOINT}":       "http://otel-collector.otel-system:4317",
		"${SANDBOX_CLASS_ENUM}":  "SANDBOX_CLASS_GVISOR",
		"${SANDBOX_CONFIG_NAME}": "gvisor-default",
	}

	tests := []struct {
		relPath string
		values  map[string]string
		want    resources.ActorTemplateRef
	}{
		{
			relPath: "demos/counter/counter-microvm-csi-test-template.yaml",
			want:    resources.ActorTemplateRef{Atespace: "ate-demo-counter-microvm-csi", Name: "counter-microvm-csi"},
		},
		{
			relPath: "demos/jupyter/jupyter-template.yaml.tmpl",
			want:    resources.ActorTemplateRef{Atespace: "ate-demo-jupyter", Name: "jupyter"},
		},
		{
			relPath: "demos/sandbox/manual-test-multi-template.yaml",
			want:    resources.ActorTemplateRef{Atespace: "ate-manual-test-multi", Name: "sandbox-template"},
		},
		{
			relPath: "benchmarking/workloads/manifests/sleep-template.yaml.tmpl",
			values:  benchmarkValues,
			want:    resources.ActorTemplateRef{Atespace: "benchmark-workloads", Name: "sleep"},
		},
		{
			relPath: "benchmarking/workloads/manifests/glutton-template.yaml.tmpl",
			values:  benchmarkValues,
			want:    resources.ActorTemplateRef{Atespace: "benchmark-workloads", Name: "glutton"},
		},
		{
			relPath: "benchmarking/workloads/manifests/glutton-durdir-data-template.yaml.tmpl",
			values:  benchmarkValues,
			want:    resources.ActorTemplateRef{Atespace: "benchmark-workloads", Name: "glutton-durdir-data"},
		},
		{
			relPath: "benchmarking/workloads/manifests/glutton-durdir-full-template.yaml.tmpl",
			values:  benchmarkValues,
			want:    resources.ActorTemplateRef{Atespace: "benchmark-workloads", Name: "glutton-durdir-full"},
		},
		{
			relPath: "benchmarking/workloads/manifests/usermem-template.yaml.tmpl",
			values:  benchmarkValues,
			want:    resources.ActorTemplateRef{Atespace: "benchmark-workloads", Name: "usermem"},
		},
		{
			relPath: "benchmarking/workloads/manifests/kernelmem-template.yaml.tmpl",
			values:  benchmarkValues,
			want:    resources.ActorTemplateRef{Atespace: "benchmark-workloads", Name: "kernelmem"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.relPath, func(t *testing.T) {
			data, err := os.ReadFile(filepath.Join(root, tc.relPath))
			if err != nil {
				t.Fatalf("ReadFile: %v", err)
			}
			manifest := strings.ReplaceAll(string(data), "${BUCKET_NAME}", "ate-snapshots")
			for placeholder, value := range tc.values {
				manifest = strings.ReplaceAll(manifest, placeholder, value)
			}
			if i := strings.Index(manifest, "${"); i >= 0 {
				t.Fatalf("unsubstituted placeholder remains: %s", manifest[i:min(i+40, len(manifest))])
			}
			template, err := ActorTemplateFromManifest([]byte(manifest))
			if err != nil {
				t.Fatalf("ActorTemplateFromManifest: %v", err)
			}
			if got := resources.ActorTemplateRefFromActorTemplate(template); got != tc.want {
				t.Errorf("manifest names template %s, want %s", got, tc.want)
			}
		})
	}
}

func TestActorTemplateFromManifestErrors(t *testing.T) {
	tests := []struct {
		name     string
		manifest string
	}{
		{name: "empty", manifest: ""},
		{name: "not yaml", manifest: ":\t:"},
		// Strict parsing: a typo must fail rather than silently drop the field.
		{name: "unknown field", manifest: "metadata:\n  atespace: a\n  name: t\nsnapshotConfig: {}\n"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := ActorTemplateFromManifest([]byte(tc.manifest)); err == nil {
				t.Error("ActorTemplateFromManifest succeeded, want error")
			}
		})
	}
}

// failingControlClient fails every GetActorTemplate with a fixed error.
type failingControlClient struct {
	ateapipb.ControlClient
	err error
}

func (f failingControlClient) GetActorTemplate(ctx context.Context, in *ateapipb.GetActorTemplateRequest, opts ...grpc.CallOption) (*ateapipb.ActorTemplate, error) {
	return nil, f.err
}

// TestWaitActorTemplateGoldenTimeoutIncludesLastError: a wait that spent its
// budget on failing Gets must surface the RPC error, not just "timed out".
func TestWaitActorTemplateGoldenTimeoutIncludesLastError(t *testing.T) {
	getErr := errors.New("connection refused")
	client := &ateclient.Client{ControlClient: failingControlClient{err: getErr}}
	ref := resources.ActorTemplateRef{Atespace: "ate-demo", Name: "counter"}

	err := WaitActorTemplateGolden(context.Background(), client, ref, 0)
	if err == nil {
		t.Fatal("WaitActorTemplateGolden succeeded, want timeout error")
	}
	if !errors.Is(err, getErr) {
		t.Errorf("WaitActorTemplateGolden error = %v, want it to wrap %v", err, getErr)
	}
}

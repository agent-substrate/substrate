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

package cmd

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/testing/protocmp"
)

const counterTemplateManifest = `metadata:
  atespace: ate-demo-counter
  name: counter
workerSelector:
  matchLabels:
    workload: counter
containers:
- name: counter
  image: ko://github.com/agent-substrate/substrate/demos/counter
  command: ["/ko-app/counter", "--extra-port=9090"]
  readyz:
    httpGet:
      path: /readyz
      port: 80
  volumeMounts:
  - name: data
    mountPath: /home/counter
resources:
  limits:
  - name: cpu
    quantity: "1"
  - name: memory
    quantity: 512Mi
snapshotsConfig:
  onPause: SNAPSHOT_CONTENT_SCOPE_FULL
  onCommit: SNAPSHOT_CONTENT_SCOPE_FULL
  storageLocation: gs://ate-snapshots/ate-demo-counter/
sandboxConfig:
  sandboxClass: SANDBOX_CLASS_GVISOR
  configName: gvisor-default
volumes:
- name: data
  type: DurableDir
  durableDir: {}
`

func TestActorTemplateFromManifest(t *testing.T) {
	got, err := actorTemplateFromManifest([]byte(counterTemplateManifest))
	if err != nil {
		t.Fatalf("actorTemplateFromManifest: %v", err)
	}

	want := &ateapipb.ActorTemplate{
		Metadata:       &ateapipb.ResourceMetadata{Atespace: "ate-demo-counter", Name: "counter"},
		WorkerSelector: &ateapipb.Selector{MatchLabels: map[string]string{"workload": "counter"}},
		Containers: []*ateapipb.Container{{
			Name:    "counter",
			Image:   "ko://github.com/agent-substrate/substrate/demos/counter",
			Command: []string{"/ko-app/counter", "--extra-port=9090"},
			Readyz: &ateapipb.ContainerReadyz{
				HttpGet: &ateapipb.HTTPGetAction{Path: "/readyz", Port: 80},
			},
			VolumeMounts: []*ateapipb.VolumeMount{{Name: "data", MountPath: "/home/counter"}},
		}},
		Resources: &ateapipb.Resources{Limits: []*ateapipb.Limits{
			{Name: "cpu", Quantity: "1"},
			{Name: "memory", Quantity: "512Mi"},
		}},
		SnapshotsConfig: &ateapipb.SnapshotsConfig{
			OnPause:         ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_FULL,
			OnCommit:        ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_FULL,
			StorageLocation: "gs://ate-snapshots/ate-demo-counter/",
		},
		SandboxConfig: &ateapipb.SandboxConfig{
			SandboxClass: ateapipb.SandboxClass_SANDBOX_CLASS_GVISOR,
			ConfigName:   "gvisor-default",
		},
		Volumes: []*ateapipb.Volume{{
			Name:       "data",
			Type:       "DurableDir",
			DurableDir: &ateapipb.DurableDirVolumeSource{},
		}},
	}
	if diff := cmp.Diff(want, got, protocmp.Transform()); diff != "" {
		t.Errorf("template mismatch (-want +got):\n%s", diff)
	}
}

func TestActorTemplateFromManifest_SnakeCase(t *testing.T) {
	// protojson accepts the proto field names as well as the json names.
	manifest := `metadata:
  atespace: ate-demo-counter
  name: counter
snapshots_config:
  on_pause: SNAPSHOT_CONTENT_SCOPE_FULL
  storage_location: gs://ate-snapshots/ate-demo-counter/
sandbox_config:
  sandbox_class: SANDBOX_CLASS_MICROVM
  config_name: microvm
`
	got, err := actorTemplateFromManifest([]byte(manifest))
	if err != nil {
		t.Fatalf("actorTemplateFromManifest: %v", err)
	}
	if got.GetSnapshotsConfig().GetStorageLocation() != "gs://ate-snapshots/ate-demo-counter/" {
		t.Errorf("storage_location = %q", got.GetSnapshotsConfig().GetStorageLocation())
	}
	if got.GetSandboxConfig().GetSandboxClass() != ateapipb.SandboxClass_SANDBOX_CLASS_MICROVM {
		t.Errorf("sandbox_class = %v", got.GetSandboxConfig().GetSandboxClass())
	}
}

func TestActorTemplateFromManifest_Errors(t *testing.T) {
	tests := []struct {
		name     string
		manifest string
	}{
		{name: "empty", manifest: ""},
		{name: "unknown field", manifest: "metadata: {atespace: a, name: n}\nsandboxClass: gvisor\n"},
		{name: "bad enum", manifest: "sandboxConfig: {sandboxClass: gvisor}\n"},
		{name: "crd shape", manifest: "apiVersion: ate.dev/v1alpha1\nkind: ActorTemplate\nmetadata: {name: counter}\n"},
		{name: "not yaml", manifest: "\t{"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got, err := actorTemplateFromManifest([]byte(test.manifest)); err == nil {
				t.Fatalf("actorTemplateFromManifest succeeded: %v", got)
			}
		})
	}
}

// The counter demo's substrate template manifests must stay parseable by
// `create actortemplate -f`; this pins them to the parser.
func TestActorTemplateFromManifest_DemoManifests(t *testing.T) {
	tests := []struct {
		manifest string
		atespace string
		name     string
		class    ateapipb.SandboxClass
	}{
		{
			manifest: "counter-template.yaml.tmpl",
			atespace: "ate-demo-counter",
			name:     "counter",
			class:    ateapipb.SandboxClass_SANDBOX_CLASS_GVISOR,
		},
		{
			manifest: "counter-microvm-template.yaml.tmpl",
			atespace: "ate-demo-counter-microvm",
			name:     "counter-microvm",
			class:    ateapipb.SandboxClass_SANDBOX_CLASS_MICROVM,
		},
	}
	for _, test := range tests {
		t.Run(test.manifest, func(t *testing.T) {
			data, err := os.ReadFile(filepath.Join("../../../..", "demos", "counter", test.manifest))
			if err != nil {
				t.Fatalf("reading demo manifest: %v", err)
			}
			// The install scripts substitute the bucket and drop the unused
			// optional placeholder lines before applying.
			rendered := strings.ReplaceAll(string(data), "${BUCKET_NAME}", "ate-snapshots")
			var kept []string
			for _, line := range strings.Split(rendered, "\n") {
				if strings.Contains(line, "${") {
					continue
				}
				kept = append(kept, line)
			}
			rendered = strings.Join(kept, "\n")

			got, err := actorTemplateFromManifest([]byte(rendered))
			if err != nil {
				t.Fatalf("actorTemplateFromManifest: %v", err)
			}
			if got.GetMetadata().GetAtespace() != test.atespace || got.GetMetadata().GetName() != test.name {
				t.Errorf("metadata = %s/%s, want %s/%s",
					got.GetMetadata().GetAtespace(), got.GetMetadata().GetName(), test.atespace, test.name)
			}
			if got.GetSandboxConfig().GetSandboxClass() != test.class {
				t.Errorf("sandbox class = %v, want %v", got.GetSandboxConfig().GetSandboxClass(), test.class)
			}
			if len(got.GetContainers()) == 0 || got.GetSnapshotsConfig().GetStorageLocation() == "" {
				t.Errorf("missing required fields: %v", got)
			}
		})
	}
}

func TestReadFileOrStdin(t *testing.T) {
	data, err := readFileOrStdin(strings.NewReader("metadata: {name: n}"), "-")
	if err != nil || string(data) != "metadata: {name: n}" {
		t.Fatalf("readFileOrStdin(-) = (%q, %v)", data, err)
	}
	if _, err := readFileOrStdin(nil, "/does/not/exist.yaml"); err == nil {
		t.Fatal("readFileOrStdin on a missing file succeeded")
	}
}

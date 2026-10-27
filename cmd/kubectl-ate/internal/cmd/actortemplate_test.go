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

	"github.com/google/go-cmp/cmp"
	"go.ate.dev/substrate/api/ateapi/v1"
	"google.golang.org/protobuf/testing/protocmp"
)

func TestActorTemplateCommandArgs(t *testing.T) {
	runCommandArgsTests(t, []commandArgsTest{
		{name: "list", command: getActorTemplatesCmd},
		{name: "get", command: getActorTemplatesCmd, args: []string{"counter"}},
		{name: "get multiple", command: getActorTemplatesCmd, args: []string{"counter", "counter-microvm"}},
	})
}

const counterTemplateManifest = `metadata:
  atespace: ate-demo-counter
  name: counter
workerSelector:
  matchLabels:
    workload: counter
containers:
- name: counter
  image: ko://go.ate.dev/substrate/demos/counter
  command: ["/ko-app/counter", "--extra-port=9090"]
  wakeupProbe:
    httpGet:
      path: /readyz
      port: 80
  volumeMounts:
  - name: data
    mountPath: /home/counter
resources:
  limits:
    cpuMillicores: 1000
    memoryBytes: 536870912
snapshotConfig:
  preferredFidelity: SNAPSHOT_FIDELITY_MEMORY
  storageLocation: gs://ate-snapshots/ate-demo-counter/
volumes:
- name: data
  durableDir: {}
`

func TestActorTemplateFromManifest(t *testing.T) {
	got, err := actorTemplateFromManifest([]byte(counterTemplateManifest))
	if err != nil {
		t.Fatalf("actorTemplateFromManifest: %v", err)
	}

	want := &ateapi.ActorTemplate{
		Metadata:       &ateapi.ResourceMetadata{Atespace: "ate-demo-counter", Name: "counter"},
		WorkerSelector: &ateapi.Selector{MatchLabels: map[string]string{"workload": "counter"}},
		Containers: []*ateapi.Container{{
			Name:    "counter",
			Image:   "ko://go.ate.dev/substrate/demos/counter",
			Command: []string{"/ko-app/counter", "--extra-port=9090"},
			WakeupProbe: &ateapi.WakeupProbe{
				HttpGet: &ateapi.HttpGetAction{Path: "/readyz", Port: 80},
			},
			VolumeMounts: []*ateapi.VolumeMount{{Name: "data", MountPath: "/home/counter"}},
		}},
		Resources: &ateapi.ResourceRequirements{Limits: &ateapi.Resources{CpuMillicores: 1000, MemoryBytes: 512 << 20}},
		SnapshotConfig: &ateapi.SnapshotConfig{
			PreferredFidelity: ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY,
			StorageLocation:   "gs://ate-snapshots/ate-demo-counter/",
		},
		Volumes: []*ateapi.Volume{{
			Name:       "data",
			DurableDir: &ateapi.DurableDirVolumeSource{},
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
snapshot_config:
  storage_location: gs://ate-snapshots/ate-demo-counter/
`
	got, err := actorTemplateFromManifest([]byte(manifest))
	if err != nil {
		t.Fatalf("actorTemplateFromManifest: %v", err)
	}
	if got.GetSnapshotConfig().GetStorageLocation() != "gs://ate-snapshots/ate-demo-counter/" {
		t.Errorf("storage_location = %q", got.GetSnapshotConfig().GetStorageLocation())
	}
}

func TestActorTemplateFromManifest_Errors(t *testing.T) {
	tests := []struct {
		name     string
		manifest string
	}{
		{name: "empty", manifest: ""},
		{name: "unknown field", manifest: "metadata: {atespace: a, name: n}\nsandboxClass: gvisor\n"},
		{name: "bad enum", manifest: "snapshotConfig: {preferredFidelity: memory}\n"},
		{name: "resources limits list", manifest: "resources: {limits: [{name: cpu, quantity: \"1\"}]}\n"},
		{name: "crd shape", manifest: "apiVersion: ate.dev/v1\nkind: ActorTemplate\nmetadata: {name: counter}\n"},
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

func TestActorTemplateFromManifest_CRDHint(t *testing.T) {
	manifest := "apiVersion: ate.dev/v1\nkind: ActorTemplate\nmetadata: {name: counter}\nspec:\n  containers: []\n"
	_, err := actorTemplateFromManifest([]byte(manifest))
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	want := "ActorTemplate is not a Kubernetes CRD; remove apiVersion/kind/spec"
	if !strings.Contains(err.Error(), want) {
		t.Fatalf("expected error containing %q, got: %v", want, err)
	}
}

func TestActorTemplateFromManifest_EgressTrustBundleDocSnippet(t *testing.T) {
	manifest := `metadata:
  name: my-actor
  atespace: my-atespace
volumes:
- name: system-info
  systemInfo:
    dataSources:
    - trustBundle:
        names:
        - egress-mitm.ate.dev
        - system-roots.ate.dev
        path: trust-bundle.pem
containers:
- name: app
  image: busybox:1.36
  volumeMounts:
  - name: system-info
    mountPath: /run/ate/egress-trust-bundle
`
	got, err := actorTemplateFromManifest([]byte(manifest))
	if err != nil {
		t.Fatalf("actorTemplateFromManifest failed: %v", err)
	}
	if got.GetMetadata().GetName() != "my-actor" || got.GetMetadata().GetAtespace() != "my-atespace" {
		t.Errorf("unexpected metadata: %v", got.GetMetadata())
	}
	if len(got.GetVolumes()) != 1 || got.GetVolumes()[0].GetName() != "system-info" {
		t.Errorf("unexpected volumes: %v", got.GetVolumes())
	}
	tb := got.GetVolumes()[0].GetSystemInfo().GetDataSources()[0].GetTrustBundle()
	if tb == nil || tb.GetPath() != "trust-bundle.pem" {
		t.Errorf("unexpected trust bundle: %v", tb)
	}
	if len(got.GetContainers()) != 1 || got.GetContainers()[0].GetVolumeMounts()[0].GetMountPath() != "/run/ate/egress-trust-bundle" {
		t.Errorf("unexpected container volume mounts: %v", got.GetContainers())
	}
}

// The counter demo's substrate template manifests must stay parseable by
// `create actortemplate -f`; this pins them to the parser.
func TestActorTemplateFromManifest_DemoManifests(t *testing.T) {
	tests := []struct {
		manifest string
		atespace string
		name     string
	}{
		{
			manifest: "counter-template.yaml.tmpl",
			atespace: "ate-demo-counter",
			name:     "counter",
		},
		{
			manifest: "counter-microvm-template.yaml.tmpl",
			atespace: "ate-demo-counter-microvm",
			name:     "counter-microvm",
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
			if len(got.GetContainers()) == 0 || got.GetSnapshotConfig().GetStorageLocation() == "" {
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

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
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/agent-substrate/substrate/internal/manifest"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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
  image: ko://github.com/agent-substrate/substrate/demos/counter
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
  - name: cpu
    quantity: "1"
  - name: memory
    quantity: 512Mi
snapshotConfig:
  onPause: SNAPSHOT_CONTENT_SCOPE_FULL
  onCommit: SNAPSHOT_CONTENT_SCOPE_FULL
  storageLocation: gs://ate-snapshots/ate-demo-counter/
sandboxConfig:
  sandboxClass: SANDBOX_CLASS_GVISOR
  configName: gvisor-default
volumes:
- name: data
  durableDir: {}
`

// parseTemplate parses a manifest that holds exactly one ActorTemplate.
func parseTemplate(t *testing.T, data string) *ateapipb.ActorTemplate {
	t.Helper()
	templates, err := manifest.Parse[ateapipb.ActorTemplate]([]byte(data))
	if err != nil {
		t.Fatalf("manifest.Parse: %v", err)
	}
	if len(templates) != 1 {
		t.Fatalf("manifest.Parse returned %d templates, want 1", len(templates))
	}
	return templates[0]
}

func TestParseActorTemplate(t *testing.T) {
	got := parseTemplate(t, counterTemplateManifest)

	want := &ateapipb.ActorTemplate{
		Metadata:       &ateapipb.ResourceMetadata{Atespace: "ate-demo-counter", Name: "counter"},
		WorkerSelector: &ateapipb.Selector{MatchLabels: map[string]string{"workload": "counter"}},
		Containers: []*ateapipb.Container{{
			Name:    "counter",
			Image:   "ko://github.com/agent-substrate/substrate/demos/counter",
			Command: []string{"/ko-app/counter", "--extra-port=9090"},
			WakeupProbe: &ateapipb.ContainerWakeupProbe{
				HttpGet: &ateapipb.HTTPGetAction{Path: "/readyz", Port: 80},
			},
			VolumeMounts: []*ateapipb.VolumeMount{{Name: "data", MountPath: "/home/counter"}},
		}},
		Resources: &ateapipb.Resources{Limits: []*ateapipb.Limits{
			{Name: "cpu", Quantity: "1"},
			{Name: "memory", Quantity: "512Mi"},
		}},
		SnapshotConfig: &ateapipb.SnapshotConfig{
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
			DurableDir: &ateapipb.DurableDirVolumeSource{},
		}},
	}
	if diff := cmp.Diff(want, got, protocmp.Transform()); diff != "" {
		t.Errorf("template mismatch (-want +got):\n%s", diff)
	}
}

func TestParseActorTemplate_SnakeCase(t *testing.T) {
	// protojson accepts the proto field names as well as the json names.
	got := parseTemplate(t, `metadata:
  atespace: ate-demo-counter
  name: counter
snapshot_config:
  on_pause: SNAPSHOT_CONTENT_SCOPE_FULL
  storage_location: gs://ate-snapshots/ate-demo-counter/
sandbox_config:
  sandbox_class: SANDBOX_CLASS_MICROVM
  config_name: microvm
`)
	if got.GetSnapshotConfig().GetStorageLocation() != "gs://ate-snapshots/ate-demo-counter/" {
		t.Errorf("storage_location = %q", got.GetSnapshotConfig().GetStorageLocation())
	}
	if got.GetSandboxConfig().GetSandboxClass() != ateapipb.SandboxClass_SANDBOX_CLASS_MICROVM {
		t.Errorf("sandbox_class = %v", got.GetSandboxConfig().GetSandboxClass())
	}
}

func TestParseActorTemplate_Errors(t *testing.T) {
	tests := []struct {
		name            string
		manifest        string
		wantErrContains string
	}{
		{name: "unknown field", manifest: "metadata: {atespace: a, name: counter}\nsandboxClass: gvisor\n", wantErrContains: "unknown field"},
		{name: "bad enum", manifest: "sandboxConfig: {sandboxClass: gvisor}\n", wantErrContains: "sandboxClass"},
		{name: "crd shape", manifest: "apiVersion: ate.dev/v1alpha1\nkind: ActorTemplate\nmetadata: {name: counter}\n", wantErrContains: "apiVersion"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := manifest.Parse[ateapipb.ActorTemplate]([]byte(test.manifest))
			if err == nil {
				t.Fatalf("manifest.Parse succeeded: %v", got)
			}
			if !strings.Contains(err.Error(), test.wantErrContains) {
				t.Errorf("error = %q, want it to contain %q", err, test.wantErrContains)
			}
		})
	}
}

// The counter demo's substrate template manifests must stay parseable by
// `create actortemplate -f`; this pins them to the parser.
func TestParseActorTemplate_DemoManifests(t *testing.T) {
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

			got := parseTemplate(t, rendered)
			if got.GetMetadata().GetAtespace() != test.atespace || got.GetMetadata().GetName() != test.name {
				t.Errorf("metadata = %s/%s, want %s/%s",
					got.GetMetadata().GetAtespace(), got.GetMetadata().GetName(), test.atespace, test.name)
			}
			if got.GetSandboxConfig().GetSandboxClass() != test.class {
				t.Errorf("sandbox class = %v, want %v", got.GetSandboxConfig().GetSandboxClass(), test.class)
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

// fakeActorTemplateCreator records the names it was asked to create and fails
// those listed in errs.
type fakeActorTemplateCreator struct {
	errs  map[string]error
	asked []string
}

func (f *fakeActorTemplateCreator) CreateActorTemplate(ctx context.Context, req *ateapipb.CreateActorTemplateRequest, opts ...grpc.CallOption) (*ateapipb.ActorTemplate, error) {
	name := req.GetActorTemplate().GetMetadata().GetName()
	f.asked = append(f.asked, name)
	if err := f.errs[name]; err != nil {
		return nil, err
	}
	return req.GetActorTemplate(), nil
}

func TestCreateActorTemplates(t *testing.T) {
	const manifestText = `metadata: {atespace: a, name: one}
---
metadata: {atespace: a, name: two}
---
metadata: {atespace: b, name: three}
`
	alreadyExists := status.Error(codes.AlreadyExists, "exists")
	tests := []struct {
		name        string
		errs        map[string]error
		wantCreated []string
		wantErrs    []string
	}{
		{name: "all created", wantCreated: []string{"one", "two", "three"}},
		{
			name:        "a failure does not stop the rest",
			errs:        map[string]error{"two": alreadyExists},
			wantCreated: []string{"one", "three"},
			wantErrs:    []string{`"two" in atespace "a"`},
		},
		{
			name:     "every failure is reported",
			errs:     map[string]error{"one": alreadyExists, "two": alreadyExists, "three": alreadyExists},
			wantErrs: []string{`"one" in atespace "a"`, `"two" in atespace "a"`, `"three" in atespace "b"`},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			templates, err := manifest.Parse[ateapipb.ActorTemplate]([]byte(manifestText))
			if err != nil {
				t.Fatalf("manifest.Parse: %v", err)
			}
			creator := &fakeActorTemplateCreator{errs: test.errs}

			created, err := createActorTemplates(t.Context(), creator, templates)

			if diff := cmp.Diff([]string{"one", "two", "three"}, creator.asked); diff != "" {
				t.Errorf("templates attempted mismatch (-want +got):\n%s", diff)
			}
			var gotCreated []string
			for _, template := range created {
				gotCreated = append(gotCreated, template.GetMetadata().GetName())
			}
			if diff := cmp.Diff(test.wantCreated, gotCreated); diff != "" {
				t.Errorf("templates created mismatch (-want +got):\n%s", diff)
			}
			if len(test.wantErrs) == 0 {
				if err != nil {
					t.Fatalf("createActorTemplates() error = %v, want nil", err)
				}
				return
			}
			if err == nil {
				t.Fatal("createActorTemplates() succeeded, want an error")
			}
			for _, want := range test.wantErrs {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("error = %q, want it to contain %q", err, want)
				}
			}
			if !errors.Is(err, alreadyExists) {
				t.Errorf("error = %q does not wrap the RPC error", err)
			}
		})
	}
}

func TestPrintCreatedActorTemplates(t *testing.T) {
	one := &ateapipb.ActorTemplate{Metadata: &ateapipb.ResourceMetadata{Atespace: "a", Name: "one"}}
	two := &ateapipb.ActorTemplate{Metadata: &ateapipb.ResourceMetadata{Atespace: "a", Name: "two"}}
	tests := []struct {
		name      string
		requested int
		created   []*ateapipb.ActorTemplate
		want      string // substring of the JSON output, empty for none
		wantList  bool
	}{
		{name: "one requested and created", requested: 1, created: []*ateapipb.ActorTemplate{one}, want: `"name": "one"`},
		{name: "two requested and created", requested: 2, created: []*ateapipb.ActorTemplate{one, two}, wantList: true},
		{name: "two requested, one created stays a list", requested: 2, created: []*ateapipb.ActorTemplate{two}, wantList: true},
		{name: "none created prints nothing", requested: 2},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var out bytes.Buffer
			if err := printCreatedActorTemplates(&out, test.requested, test.created, "json"); err != nil {
				t.Fatalf("printCreatedActorTemplates: %v", err)
			}
			if len(test.created) == 0 {
				if out.Len() != 0 {
					t.Errorf("output = %q, want none", out.String())
				}
				return
			}
			if isList := strings.Contains(out.String(), "actorTemplates"); isList != test.wantList {
				t.Errorf("list output = %v, want %v:\n%s", isList, test.wantList, out.String())
			}
			if test.want != "" && !strings.Contains(out.String(), test.want) {
				t.Errorf("output = %q, want it to contain %q", out.String(), test.want)
			}
		})
	}
}

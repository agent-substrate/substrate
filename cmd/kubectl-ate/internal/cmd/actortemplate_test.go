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

package cmd

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	"go.ate.dev/substrate/api/ateapi/v1"
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

func TestActorTemplatesFromManifest(t *testing.T) {
	got, err := actorTemplatesFromManifest([]byte(counterTemplateManifest))
	if err != nil {
		t.Fatalf("actorTemplatesFromManifest: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("actorTemplatesFromManifest returned %d templates, want 1", len(got))
	}
	template := got[0].template

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
	if diff := cmp.Diff(want, template, protocmp.Transform()); diff != "" {
		t.Errorf("template mismatch (-want +got):\n%s", diff)
	}
}

func TestActorTemplatesFromManifest_SnakeCase(t *testing.T) {
	// protojson accepts the proto field names as well as the json names.
	manifest := `metadata:
  atespace: ate-demo-counter
  name: counter
snapshot_config:
  storage_location: gs://ate-snapshots/ate-demo-counter/
`
	got, err := actorTemplatesFromManifest([]byte(manifest))
	if err != nil {
		t.Fatalf("actorTemplatesFromManifest: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("actorTemplatesFromManifest returned %d templates, want 1", len(got))
	}
	if got[0].template.GetSnapshotConfig().GetStorageLocation() != "gs://ate-snapshots/ate-demo-counter/" {
		t.Errorf("storage_location = %q", got[0].template.GetSnapshotConfig().GetStorageLocation())
	}
}

func TestActorTemplatesFromManifest_MultiDoc(t *testing.T) {
	manifest := `metadata: {atespace: a, name: one}
---
metadata: {atespace: a, name: two}
---
---
metadata: {atespace: a, name: three}
`
	got, err := actorTemplatesFromManifest([]byte(manifest))
	if err != nil {
		t.Fatalf("actorTemplatesFromManifest: %v", err)
	}
	var names []string
	for _, tmpl := range got {
		names = append(names, tmpl.template.GetMetadata().GetName())
	}
	if diff := cmp.Diff([]string{"one", "two", "three"}, names); diff != "" {
		t.Errorf("names mismatch (-want +got):\n%s", diff)
	}
}

func TestActorTemplatesFromManifest_Errors(t *testing.T) {
	tests := []struct {
		name     string
		manifest string
	}{
		{name: "empty", manifest: ""},
		{name: "only a separator", manifest: "---\n"},
		{name: "unknown field", manifest: "metadata: {atespace: a, name: n}\nsandboxClass: gvisor\n"},
		{name: "bad enum", manifest: "snapshotConfig: {preferredFidelity: memory}\n"},
		{name: "resources limits list", manifest: "resources: {limits: [{name: cpu, quantity: \"1\"}]}\n"},
		{name: "crd shape", manifest: "apiVersion: ate.dev/v1\nkind: ActorTemplate\nmetadata: {name: counter}\n"},
		{name: "not yaml", manifest: "\t{"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got, err := actorTemplatesFromManifest([]byte(test.manifest)); err == nil {
				t.Fatalf("actorTemplatesFromManifest succeeded: %v", got)
			}
		})
	}
}

// The counter demo's substrate template manifests must stay parseable by
// `create actortemplate -f`; this pins them to the parser.
func TestActorTemplatesFromManifest_DemoManifests(t *testing.T) {
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
			// ate-setup drops the unused optional placeholder lines before
			// applying.
			var kept []string
			for _, line := range strings.Split(string(data), "\n") {
				if strings.Contains(line, "${") {
					continue
				}
				kept = append(kept, line)
			}
			rendered := strings.Join(kept, "\n")

			got, err := actorTemplatesFromManifest([]byte(rendered))
			if err != nil {
				t.Fatalf("actorTemplatesFromManifest: %v", err)
			}
			if len(got) != 1 {
				t.Fatalf("actorTemplatesFromManifest returned %d templates, want 1", len(got))
			}
			template := got[0].template
			if template.GetMetadata().GetAtespace() != test.atespace || template.GetMetadata().GetName() != test.name {
				t.Errorf("metadata = %s/%s, want %s/%s",
					template.GetMetadata().GetAtespace(), template.GetMetadata().GetName(), test.atespace, test.name)
			}
			if len(template.GetContainers()) == 0 {
				t.Errorf("manifest has no containers: %v", template)
			}
			// The API rejects storageLocation without its preview gate.
			if location := template.GetSnapshotConfig().GetStorageLocation(); location != "" {
				t.Errorf("manifest sets storageLocation %q, want none", location)
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

// fakeActorTemplateCreator records each request and answers with an error for
// names listed in fail.
type fakeActorTemplateCreator struct {
	fail map[string]error
	got  []string
}

func (f *fakeActorTemplateCreator) CreateActorTemplate(_ context.Context, req *ateapi.CreateActorTemplateRequest, _ ...grpc.CallOption) (*ateapi.ActorTemplate, error) {
	tmpl := req.GetActorTemplate()
	f.got = append(f.got, tmpl.GetMetadata().GetName())
	if err, ok := f.fail[tmpl.GetMetadata().GetName()]; ok {
		return nil, err
	}
	return tmpl, nil
}

func TestCreateActorTemplatesRunner(t *testing.T) {
	mkDocument := func(index int, atespace, name string) actorTemplateDocument {
		return actorTemplateDocument{
			index:    index,
			template: &ateapi.ActorTemplate{Metadata: &ateapi.ResourceMetadata{Atespace: atespace, Name: name}},
		}
	}
	tests := []struct {
		name string
		// documents is the parsed manifest the runner works from.
		documents []actorTemplateDocument
		// fail maps a template name to the error its create call returns.
		fail map[string]error
		// wantCalls is the order in which the runner calls CreateActorTemplate.
		wantCalls []string
		wantErrs  []string
		wantNoErr []string
		// outputFmt is the -o format; empty means yaml.
		outputFmt string
		// wantStdout and wantNoStdout are substrings of the printed output.
		wantStdout      []string
		wantNoStdout    []string
		wantEmptyStdout bool
	}{
		{
			name:       "single document prints one template",
			documents:  []actorTemplateDocument{mkDocument(1, "a", "one")},
			wantCalls:  []string{"one"},
			wantStdout: []string{"name: one"},
			// -o yaml of a list wraps the templates in a response message.
			wantNoStdout: []string{"actorTemplates:"},
		},
		{
			name:       "multi-document all created prints a list",
			documents:  []actorTemplateDocument{mkDocument(1, "a", "one"), mkDocument(2, "a", "two")},
			wantCalls:  []string{"one", "two"},
			wantStdout: []string{"actorTemplates:", "one", "two"},
		},
		{
			name:      "partial failure still prints a list",
			documents: []actorTemplateDocument{mkDocument(1, "a", "one"), mkDocument(2, "a", "two")},
			fail:      map[string]error{"two": status.Error(codes.AlreadyExists, "already exists")},
			wantCalls: []string{"one", "two"},
			wantErrs: []string{
				`document 2: failed to create actor template "two" in atespace "a"`,
				"already exists",
			},
			wantStdout:   []string{"actorTemplates:", "one"},
			wantNoStdout: []string{"two"},
		},
		{
			name:      "all creates fail prints nothing",
			documents: []actorTemplateDocument{mkDocument(1, "a", "one"), mkDocument(2, "a", "two")},
			fail: map[string]error{
				"one": status.Error(codes.NotFound, "no such atespace"),
				"two": status.Error(codes.NotFound, "no such atespace"),
			},
			wantCalls: []string{"one", "two"},
			wantErrs: []string{
				`document 1: failed to create actor template "one" in atespace "a"`,
				`document 2: failed to create actor template "two" in atespace "a"`,
			},
			wantEmptyStdout: true,
		},
		{
			name:       "table output prints a header and a row per template",
			documents:  []actorTemplateDocument{mkDocument(1, "a", "one"), mkDocument(2, "a", "two")},
			outputFmt:  "table",
			wantCalls:  []string{"one", "two"},
			wantStdout: []string{"ATESPACE", "NAME", "one", "two"},
			// The table renderer is the list renderer, not the response message.
			wantNoStdout: []string{"actorTemplates:"},
		},
		{
			name:      "single document with missing metadata names the document",
			documents: []actorTemplateDocument{{index: 1, template: &ateapi.ActorTemplate{}}},
			fail:      map[string]error{"": status.Error(codes.InvalidArgument, "metadata is required")},
			wantCalls: []string{""},
			wantErrs:  []string{"document 1: failed to create actor template", "metadata is required"},
		},
		{
			name: "missing metadata names the document",
			documents: []actorTemplateDocument{
				mkDocument(1, "a", "one"),
				{index: 2, template: &ateapi.ActorTemplate{}},
			},
			fail:      map[string]error{"": status.Error(codes.InvalidArgument, "metadata is required")},
			wantCalls: []string{"one", ""},
			wantErrs:  []string{"document 2: failed to create actor template: rpc error", "metadata is required"},
			// The empty metadata rendered as "/:" before the document was named.
			wantNoErr: []string{"/: "},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			creator := &fakeActorTemplateCreator{fail: tt.fail}
			format := tt.outputFmt
			if format == "" {
				format = "yaml"
			}
			var stdout bytes.Buffer
			runner := &createActorTemplatesRunner{
				creator:   creator,
				documents: tt.documents,
				outputFmt: format,
				stdout:    &stdout,
			}

			err := runner.Run(context.Background())
			switch {
			case len(tt.wantErrs) > 0:
				if err == nil {
					t.Fatal("Run succeeded, want an aggregated error")
				}
				for _, want := range tt.wantErrs {
					if !strings.Contains(err.Error(), want) {
						t.Errorf("Run error = %q, want it to contain %q", err, want)
					}
				}
				for _, no := range tt.wantNoErr {
					if strings.Contains(err.Error(), no) {
						t.Errorf("Run error = %q, want it not to contain %q", err, no)
					}
				}
			case err != nil:
				t.Fatalf("Run: %v", err)
			}
			if diff := cmp.Diff(tt.wantCalls, creator.got); diff != "" {
				t.Errorf("create calls mismatch (-want +got):\n%s", diff)
			}
			if tt.wantEmptyStdout && stdout.Len() != 0 {
				t.Errorf("stdout = %q, want empty", stdout.String())
			}
			for _, want := range tt.wantStdout {
				if !strings.Contains(stdout.String(), want) {
					t.Errorf("stdout = %q, want it to contain %q", stdout.String(), want)
				}
			}
			for _, no := range tt.wantNoStdout {
				if strings.Contains(stdout.String(), no) {
					t.Errorf("stdout = %q, want it not to contain %q", stdout.String(), no)
				}
			}
		})
	}
}

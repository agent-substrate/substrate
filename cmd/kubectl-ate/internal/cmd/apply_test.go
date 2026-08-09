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
	"os"
	"strings"
	"testing"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/testing/protocmp"
)

const applyManifest = `apiVersion: api.ate.dev/v1alpha1
kind: ActorTemplate
metadata:
  name: counter
spec:
  workerSelector:
    matchLabels:
      workload: counter
  defaultVersionOnCreate:
    name: counter-v1
---
apiVersion: api.ate.dev/v1alpha1
kind: ActorTemplateVersion
metadata:
  name: counter-v1
actorTemplate:
  name: counter
spec:
  pauseImage: pause@sha256:abc
  containers:
  - name: counter
    image: app@sha256:def
    readyz:
      httpGet:
        port: 80
  snapshotsConfig:
    storageLocation: gs://bucket/counter/
`

// fakeControl stubs the few Control RPCs apply uses; any other call panics
// through the nil embedded interface.
type fakeControl struct {
	ateapipb.ControlClient

	templateExists bool
	storedVersion  *ateapipb.ActorTemplateVersion

	createdTemplates []*ateapipb.ActorTemplate
	createdVersions  []*ateapipb.ActorTemplateVersion
	updates          []*ateapipb.UpdateActorTemplateRequest
}

func (f *fakeControl) CreateActorTemplate(ctx context.Context, req *ateapipb.CreateActorTemplateRequest, opts ...grpc.CallOption) (*ateapipb.ActorTemplate, error) {
	if f.templateExists {
		return nil, status.Error(codes.AlreadyExists, "exists")
	}
	f.createdTemplates = append(f.createdTemplates, req.GetActorTemplate())
	return req.GetActorTemplate(), nil
}

func (f *fakeControl) CreateActorTemplateVersion(ctx context.Context, req *ateapipb.CreateActorTemplateVersionRequest, opts ...grpc.CallOption) (*ateapipb.ActorTemplateVersion, error) {
	if f.storedVersion != nil {
		return nil, status.Error(codes.AlreadyExists, "exists")
	}
	f.createdVersions = append(f.createdVersions, req.GetActorTemplateVersion())
	return req.GetActorTemplateVersion(), nil
}

func (f *fakeControl) GetActorTemplateVersion(ctx context.Context, req *ateapipb.GetActorTemplateVersionRequest, opts ...grpc.CallOption) (*ateapipb.ActorTemplateVersion, error) {
	if f.storedVersion == nil {
		return nil, status.Error(codes.NotFound, "not found")
	}
	return f.storedVersion, nil
}

func (f *fakeControl) UpdateActorTemplate(ctx context.Context, req *ateapipb.UpdateActorTemplateRequest, opts ...grpc.CallOption) (*ateapipb.ActorTemplate, error) {
	f.updates = append(f.updates, req)
	return req.GetActorTemplate(), nil
}

func TestParseApplyDocs(t *testing.T) {
	docs, err := parseApplyDocs(strings.NewReader(applyManifest))
	if err != nil {
		t.Fatalf("parseApplyDocs failed: %v", err)
	}
	if len(docs) != 2 {
		t.Fatalf("parsed %d documents, want 2", len(docs))
	}
	wantTemplate := &ateapipb.ActorTemplate{
		Metadata: &ateapipb.ResourceMetadata{Name: "counter"},
		Spec: &ateapipb.ActorTemplateSpec{
			WorkerSelector:         &ateapipb.Selector{MatchLabels: map[string]string{"workload": "counter"}},
			DefaultVersionOnCreate: &ateapipb.ObjectRef{Name: "counter-v1"},
		},
	}
	if diff := cmp.Diff(wantTemplate, docs[0].template, protocmp.Transform()); diff != "" {
		t.Errorf("template mismatch (-want +got):\n%s", diff)
	}
	if want := []string{"spec.worker_selector", "spec.default_version_on_create"}; !cmp.Equal(want, docs[0].maskPaths) {
		t.Errorf("maskPaths = %v, want %v", docs[0].maskPaths, want)
	}
	if docs[1].version.GetMetadata().GetName() != "counter-v1" || docs[1].version.GetActorTemplate().GetName() != "counter" {
		t.Errorf("version parsed wrong: %v", docs[1].version)
	}
}

func TestParseApplyDocs_Errors(t *testing.T) {
	cases := []struct {
		name     string
		manifest string
		wantErr  string
	}{{
		"CRD apiVersion rejected",
		"apiVersion: ate.dev/v1alpha1\nkind: ActorTemplate\nmetadata: {name: x}\n",
		"unsupported apiVersion",
	}, {
		"unknown kind",
		"apiVersion: api.ate.dev/v1alpha1\nkind: Actor\nmetadata: {name: x}\n",
		`unsupported kind "Actor"`,
	}, {
		"unknown field",
		"apiVersion: api.ate.dev/v1alpha1\nkind: ActorTemplate\nmetadata: {name: x}\nspec: {bogus: 1}\n",
		"invalid ActorTemplate",
	}}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := parseApplyDocs(strings.NewReader(tc.manifest))
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Errorf("error = %v, want contains %q", err, tc.wantErr)
			}
		})
	}
}

func TestParseApplyDocs_SkipsEmptyDocuments(t *testing.T) {
	docs, err := parseApplyDocs(strings.NewReader("---\n# just a comment\n---\n" + applyManifest))
	if err != nil {
		t.Fatalf("parseApplyDocs failed: %v", err)
	}
	if len(docs) != 2 {
		t.Errorf("parsed %d documents, want 2", len(docs))
	}
}

func TestApplyDocs_FreshCreate(t *testing.T) {
	docs, err := parseApplyDocs(strings.NewReader(applyManifest))
	if err != nil {
		t.Fatalf("parseApplyDocs failed: %v", err)
	}
	fake := &fakeControl{}
	var out bytes.Buffer
	if err := applyDocs(context.Background(), &out, fake, docs); err != nil {
		t.Fatalf("applyDocs failed: %v", err)
	}

	// The create must not carry the default (the API forbids it at
	// creation); the default lands via the pass-2 update, after the version
	// defined later in the file was created.
	if len(fake.createdTemplates) != 1 || fake.createdTemplates[0].GetSpec().GetDefaultVersionOnCreate() != nil {
		t.Errorf("created templates = %v, want one create without default_version_on_create", fake.createdTemplates)
	}
	if len(fake.createdVersions) != 1 {
		t.Errorf("created %d versions, want 1", len(fake.createdVersions))
	}
	if len(fake.updates) != 1 {
		t.Fatalf("update calls = %d, want 1", len(fake.updates))
	}
	if want := []string{"spec.default_version_on_create"}; !cmp.Equal(want, fake.updates[0].GetUpdateMask().GetPaths()) {
		t.Errorf("fresh-create update mask = %v, want %v (selector already set at create)", fake.updates[0].GetUpdateMask().GetPaths(), want)
	}
	for _, want := range []string{"actortemplate/counter created", "actortemplateversion/counter-v1 created"} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("output %q missing %q", out.String(), want)
		}
	}
}

func TestApplyDocs_ExistingTemplateConfigured(t *testing.T) {
	docs, err := parseApplyDocs(strings.NewReader(applyManifest))
	if err != nil {
		t.Fatalf("parseApplyDocs failed: %v", err)
	}
	fake := &fakeControl{templateExists: true}
	var out bytes.Buffer
	if err := applyDocs(context.Background(), &out, fake, docs); err != nil {
		t.Fatalf("applyDocs failed: %v", err)
	}
	if len(fake.updates) != 1 {
		t.Fatalf("update calls = %d, want 1", len(fake.updates))
	}
	if want := []string{"spec.worker_selector", "spec.default_version_on_create"}; !cmp.Equal(want, fake.updates[0].GetUpdateMask().GetPaths()) {
		t.Errorf("update mask = %v, want %v", fake.updates[0].GetUpdateMask().GetPaths(), want)
	}
	if !strings.Contains(out.String(), "actortemplate/counter configured") {
		t.Errorf("output %q missing configured verdict", out.String())
	}
}

func TestApplyDocs_VersionUnchangedAndDrifted(t *testing.T) {
	docs, err := parseApplyDocs(strings.NewReader(applyManifest))
	if err != nil {
		t.Fatalf("parseApplyDocs failed: %v", err)
	}

	// The stored spec carries server-applied defaults (readyz timeout 30,
	// path /readyz) that the manifest omits; the comparison must default
	// the manifest the same way and call this unchanged.
	stored := &ateapipb.ActorTemplateVersion{
		Metadata:      &ateapipb.ResourceMetadata{Name: "counter-v1"},
		ActorTemplate: &ateapipb.ObjectRef{Name: "counter"},
		Spec: &ateapipb.ActorTemplateVersionSpec{
			PauseImage: "pause@sha256:abc",
			Containers: []*ateapipb.Container{{
				Name:  "counter",
				Image: "app@sha256:def",
				Readyz: &ateapipb.ContainerReadyz{
					HttpGet:        &ateapipb.HTTPGetAction{Path: "/readyz", Port: 80},
					TimeoutSeconds: 30,
				},
			}},
			SnapshotsConfig: &ateapipb.SnapshotsConfig{StorageLocation: "gs://bucket/counter/"},
		},
	}
	fake := &fakeControl{templateExists: true, storedVersion: stored}
	var out bytes.Buffer
	if err := applyDocs(context.Background(), &out, fake, docs); err != nil {
		t.Fatalf("applyDocs failed: %v", err)
	}
	if !strings.Contains(out.String(), "actortemplateversion/counter-v1 unchanged") {
		t.Errorf("output %q missing unchanged verdict", out.String())
	}

	drifted := &fakeControl{templateExists: true, storedVersion: stored}
	docs[1].version.Spec.PauseImage = "pause@sha256:other"
	err = applyDocs(context.Background(), &bytes.Buffer{}, drifted, docs)
	if err == nil || !strings.Contains(err.Error(), "versions are immutable") {
		t.Errorf("drifted apply error = %v, want immutability error", err)
	}
}

// The demo manifest is the reference api.ate.dev document; parsing it here
// pins the accepted enum spellings and field names.
func TestParseApplyDocs_CounterDemoTemplate(t *testing.T) {
	raw, err := os.ReadFile("../../../../demos/counter/counter-atv.yaml.tmpl")
	if err != nil {
		t.Fatalf("read demo template: %v", err)
	}
	manifest := strings.ReplaceAll(string(raw), "${BUCKET_NAME}", "test-bucket")

	docs, err := parseApplyDocs(strings.NewReader(manifest))
	if err != nil {
		t.Fatalf("parseApplyDocs failed: %v", err)
	}
	if len(docs) != 2 || docs[0].template == nil || docs[1].version == nil {
		t.Fatalf("parsed %d documents, want ActorTemplate + ActorTemplateVersion", len(docs))
	}
	spec := docs[1].version.GetSpec()
	if got := spec.GetSnapshotsConfig().GetOnPause(); got != ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_FULL {
		t.Errorf("onPause = %v, want FULL", got)
	}
	if got := spec.GetSnapshotsConfig().GetOnCommit(); got != ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_DATA {
		t.Errorf("onCommit = %v, want DATA", got)
	}
	if got := spec.GetSnapshotsConfig().GetStorageLocation(); got != "gs://test-bucket/ate-demo-counter-atv/" {
		t.Errorf("storageLocation = %q", got)
	}
	if got := docs[0].template.GetSpec().GetDefaultVersionOnCreate().GetName(); got != "counter-atv-v1" {
		t.Errorf("defaultVersionOnCreate = %q, want counter-atv-v1", got)
	}
}

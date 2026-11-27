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
	"context"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/spf13/cobra"
	"go.ate.dev/substrate/api/ateapi/v1"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/testing/protocmp"
)

func TestParseLabelSelector(t *testing.T) {
	got, err := parseLabelSelector("tier=paid,env=prod")
	if err != nil {
		t.Fatalf("parseLabelSelector() error = %v", err)
	}
	want := &ateapi.Selector{MatchLabels: map[string]string{"tier": "paid", "env": "prod"}}
	if diff := cmp.Diff(want, got, protocmp.Transform()); diff != "" {
		t.Errorf("parseLabelSelector() mismatch (-want +got):\n%s", diff)
	}
	if _, err := parseLabelSelector("bad key!=v"); err == nil || !strings.Contains(err.Error(), "invalid label key") {
		t.Fatalf("parseLabelSelector(bad key) error = %v, want invalid label key", err)
	}
}

func TestApplyLabelChanges(t *testing.T) {
	have := map[string]string{"tier": "free", "env": "dev"}
	changes, err := parseLabelChanges([]string{"tier=paid", "env-", "app=api"})
	if err != nil {
		t.Fatalf("parseLabelChanges() error = %v", err)
	}
	if _, err := applyLabelChanges(have, changes, false); err == nil || !strings.Contains(err.Error(), "--overwrite") {
		t.Fatalf("applyLabelChanges(no overwrite) error = %v, want --overwrite", err)
	}
	got, err := applyLabelChanges(have, changes, true)
	if err != nil {
		t.Fatalf("applyLabelChanges() error = %v", err)
	}
	want := map[string]string{"tier": "paid", "app": "api"}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("applyLabelChanges() mismatch (-want +got):\n%s", diff)
	}
	if have["tier"] != "free" {
		t.Errorf("applyLabelChanges mutated the input: tier = %q", have["tier"])
	}

	removeMissing, err := parseLabelChanges([]string{"missing-"})
	if err != nil {
		t.Fatalf("parseLabelChanges(missing-) error = %v", err)
	}
	if _, err := applyLabelChanges(nil, removeMissing, false); err == nil || !strings.Contains(err.Error(), "not found") {
		t.Fatalf("applyLabelChanges(missing) error = %v, want not found", err)
	}
}

func TestLabelActor(t *testing.T) {
	fake := &fakeActorUpdater{actor: &ateapi.Actor{
		Metadata: &ateapi.ResourceMetadata{
			Atespace: "team-a",
			Name:     "runner",
			Uid:      "01234567-89ab-cdef-0123-456789abcdef",
			Version:  3,
			Labels:   map[string]string{"tier": "free"},
		},
		ActorTemplate: &ateapi.ObjectRef{Atespace: "team-a", Name: "tmpl"},
	}}
	changes, err := parseLabelChanges([]string{"env=prod"})
	if err != nil {
		t.Fatalf("parseLabelChanges() error = %v", err)
	}
	if _, err := labelActor(context.Background(), fake, &ateapi.ObjectRef{Atespace: "team-a", Name: "runner"}, changes, false); err != nil {
		t.Fatalf("labelActor() error = %v", err)
	}
	got := fake.updateReq.GetActor().GetMetadata().GetLabels()
	want := map[string]string{"tier": "free", "env": "prod"}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("updated labels mismatch (-want +got):\n%s", diff)
	}
	if fake.updateReq.GetActor().GetActorTemplate().GetName() != "tmpl" {
		t.Errorf("labelActor dropped the template")
	}
}

type fakeTemplateLabeler struct {
	tmpl      *ateapi.ActorTemplate
	updateReq *ateapi.UpdateActorTemplateRequest
}

func (f *fakeTemplateLabeler) GetActorTemplate(context.Context, *ateapi.GetActorTemplateRequest, ...grpc.CallOption) (*ateapi.ActorTemplate, error) {
	return proto.Clone(f.tmpl).(*ateapi.ActorTemplate), nil
}

func (f *fakeTemplateLabeler) UpdateActorTemplate(_ context.Context, in *ateapi.UpdateActorTemplateRequest, _ ...grpc.CallOption) (*ateapi.ActorTemplate, error) {
	f.updateReq = in
	return in.GetActorTemplate(), nil
}

func TestLabelActorTemplateSendsLabelsOnly(t *testing.T) {
	fake := &fakeTemplateLabeler{tmpl: &ateapi.ActorTemplate{
		Metadata: &ateapi.ResourceMetadata{
			Atespace: "team-a",
			Name:     "tmpl",
			Uid:      "01234567-89ab-cdef-0123-456789abcdef",
			Version:  4,
		},
		Containers: []*ateapi.Container{{Name: "main"}},
	}}
	changes, err := parseLabelChanges([]string{"tier=paid"})
	if err != nil {
		t.Fatalf("parseLabelChanges() error = %v", err)
	}
	if _, err := labelActorTemplate(context.Background(), fake, &ateapi.ObjectRef{Atespace: "team-a", Name: "tmpl"}, changes, false); err != nil {
		t.Fatalf("labelActorTemplate() error = %v", err)
	}
	sent := fake.updateReq.GetActorTemplate()
	if len(sent.GetContainers()) != 0 {
		t.Errorf("UpdateActorTemplate request carried containers")
	}
	if sent.GetMetadata().GetUid() != fake.tmpl.GetMetadata().GetUid() || sent.GetMetadata().GetVersion() != 4 {
		t.Errorf("preconditions = uid %q version %d", sent.GetMetadata().GetUid(), sent.GetMetadata().GetVersion())
	}
	if sent.GetMetadata().GetLabels()["tier"] != "paid" {
		t.Errorf("labels = %v, want tier=paid", sent.GetMetadata().GetLabels())
	}
}

func TestLabelCommandArgs(t *testing.T) {
	resetFlagsRecursive(rootCmd)
	t.Cleanup(func() { resetFlagsRecursive(rootCmd) })
	for _, c := range []*cobra.Command{labelActorCmd, labelActorTemplateCmd, labelEgressPolicyCmd} {
		if err := c.Flags().Set("atespace", "team-a"); err != nil {
			t.Fatalf("set atespace: %v", err)
		}
	}
	runCommandArgsTests(t, []commandArgsTest{
		{name: "actor", command: labelActorCmd, args: []string{"runner", "tier=paid"}},
		{name: "actor requires a label", command: labelActorCmd, args: []string{"runner"}, wantErr: true},
		{name: "actor requires a name", command: labelActorCmd, wantErr: true},
		{name: "template", command: labelActorTemplateCmd, args: []string{"tmpl", "tier-"}},
		{name: "egress", command: labelEgressPolicyCmd, args: []string{"runner", "env=prod", "tier-"}},
	})
}

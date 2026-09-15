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
	"strings"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/cmd/kubectl-ate/internal/printer"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/testing/protocmp"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestEgressPolicyFromManifest(t *testing.T) {
	t.Parallel()

	fullMetadata := &ateapipb.ResourceMetadata{Atespace: "team-a", Name: "default"}
	hostnames := []*ateapipb.EgressRule{{Hostnames: &ateapipb.HostnameRule{Patterns: []string{"api.example.com"}}}}
	cidrs := []*ateapipb.EgressRule{{Cidrs: &ateapipb.CIDRRule{Cidrs: []string{"10.64.0.0/16"}}}}
	all := []*ateapipb.EgressRule{{All: &emptypb.Empty{}}}

	tests := []struct {
		name     string
		manifest string
		want     *ateapipb.EgressPolicy
		wantErr  bool
		// wantErrContains is checked only for errors this package produces.
		wantErrContains string
	}{
		{
			name: "camel case hostnames",
			manifest: `metadata:
  atespace: team-a
  name: default
rules:
- hostnames:
    patterns:
    - api.example.com
`,
			want: &ateapipb.EgressPolicy{Metadata: fullMetadata, Rules: hostnames},
		},
		{
			name: "snake case cidrs",
			manifest: `metadata: {atespace: team-a, name: default}
rules:
- cidrs: {cidrs: ["10.64.0.0/16"]}
`,
			want: &ateapipb.EgressPolicy{Metadata: fullMetadata, Rules: cidrs},
		},
		{
			name: "all rule",
			manifest: `metadata: {atespace: team-a, name: default}
rules:
- all: {}
`,
			want: &ateapipb.EgressPolicy{Metadata: fullMetadata, Rules: []*ateapipb.EgressRule{{All: &emptypb.Empty{}}}},
		},
		{
			name: "metadata omitted is left nil",
			manifest: `rules:
- hostnames: {patterns: [api.example.com]}
`,
			want: &ateapipb.EgressPolicy{Rules: hostnames},
		},
		{
			name: "uid version and timestamps preserved",
			manifest: `metadata:
  atespace: team-a
  name: default
  uid: 3f2b1c0e-8d5a-4b6e-9c1d-2a7e4f6b8c0d
  version: "2"
  createTime: "2026-01-01T11:55:00Z"
rules:
- hostnames: {patterns: [api.example.com]}
`,
			want: &ateapipb.EgressPolicy{
				Metadata: &ateapipb.ResourceMetadata{
					Atespace:   "team-a",
					Name:       "default",
					Uid:        "3f2b1c0e-8d5a-4b6e-9c1d-2a7e4f6b8c0d",
					Version:    2,
					CreateTime: timestamppb.New(time.Date(2026, 1, 1, 11, 55, 0, 0, time.UTC)),
				},
				Rules: hostnames,
			},
		},
		{
			name:     "json input",
			manifest: `{"metadata": {"atespace": "team-a", "name": "default"}, "rules": [{"cidrs": {"cidrs": ["10.64.0.0/16"]}}]}`,
			want:     &ateapipb.EgressPolicy{Metadata: fullMetadata, Rules: cidrs},
		},
		{name: "empty", manifest: "", wantErr: true, wantErrContains: "manifest is empty"},
		{name: "unknown field", manifest: "rulez: []", wantErr: true, wantErrContains: "invalid EgressPolicy"},
		{name: "rules not a list", manifest: "rules: {all: {}}", wantErr: true},
		{
			name: "crd shape",
			manifest: `apiVersion: ate.dev/v1alpha1
kind: EgressPolicy
metadata: {name: default}
`,
			wantErr: true,
		},
		{name: "not yaml", manifest: "\t{", wantErr: true, wantErrContains: "invalid YAML"},
		{
			name: "leading document separator",
			manifest: `---
rules:
- all: {}
`,
			want: &ateapipb.EgressPolicy{Rules: all},
		},
		{
			name: "trailing document separator",
			manifest: `rules:
- all: {}
---
`,
			want: &ateapipb.EgressPolicy{Rules: all},
		},
		{
			name: "empty second document",
			manifest: `rules:
- all: {}
---
# nothing here
`,
			want: &ateapipb.EgressPolicy{Rules: all},
		},
		{
			name: "two egress policies separated by ---",
			manifest: `metadata:
  atespace: team-a
  name: default
rules:
- hostnames:
    patterns:
    - api.example.com
---
metadata:
  atespace: team-b
  name: default
rules:
- cidrs:
    cidrs:
    - 10.64.0.0/16
`,
			wantErr:         true,
			wantErrContains: "manifest holds more than one document",
		},
		{
			name: "three egress policies separated by ---",
			manifest: `metadata: {atespace: team-a, name: default}
rules:
- hostnames: {patterns: [api.example.com]}
---
metadata: {atespace: team-b, name: default}
rules:
- cidrs: {cidrs: ["10.64.0.0/16"]}
---
metadata: {atespace: team-c, name: default}
rules:
- all: {}
`,
			wantErr:         true,
			wantErrContains: "manifest holds more than one document",
		},
		{
			name: "two policies with an empty document between",
			manifest: `metadata: {atespace: team-a, name: default}
rules:
- hostnames: {patterns: [api.example.com]}
---
# just a comment
---
metadata: {atespace: team-b, name: default}
rules:
- cidrs: {cidrs: ["10.64.0.0/16"]}
`,
			wantErr:         true,
			wantErrContains: "manifest holds more than one document",
		},
		{
			name: `two json documents separated by ---`,
			manifest: `{"metadata": {"atespace": "team-a", "name": "default"}, "rules": [{"all": {}}]}
---
{"metadata": {"atespace": "team-b", "name": "default"}, "rules": [{"all": {}}]}
`,
			wantErr:         true,
			wantErrContains: "manifest holds more than one document",
		},
		{name: "only a separator", manifest: "---\n", wantErr: true, wantErrContains: "manifest is empty"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			got, err := egressPolicyFromManifest([]byte(test.manifest))
			if (err != nil) != test.wantErr {
				t.Fatalf("egressPolicyFromManifest() error = %v, wantErr %t", err, test.wantErr)
			}
			if test.wantErr {
				if test.wantErrContains != "" && !strings.Contains(err.Error(), test.wantErrContains) {
					t.Fatalf("egressPolicyFromManifest() error = %v, want it to contain %q", err, test.wantErrContains)
				}
				return
			}
			if diff := cmp.Diff(test.want, got, protocmp.Transform()); diff != "" {
				t.Errorf("policy mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestFillEgressPolicyMetadata(t *testing.T) {
	t.Parallel()

	rules := []*ateapipb.EgressRule{{All: &emptypb.Empty{}}}
	filled := &ateapipb.EgressPolicy{Metadata: &ateapipb.ResourceMetadata{Atespace: "team-a", Name: "default"}, Rules: rules}
	pinned := &ateapipb.ResourceMetadata{Atespace: "team-a", Name: "default", Uid: "3f2b1c0e-8d5a-4b6e-9c1d-2a7e4f6b8c0d", Version: 2}

	tests := []struct {
		name            string
		policy          *ateapipb.EgressPolicy
		want            *ateapipb.EgressPolicy
		wantErrContains string
	}{
		{
			name:   "metadata omitted is filled",
			policy: &ateapipb.EgressPolicy{Rules: rules},
			want:   filled,
		},
		{
			name:   "name omitted is filled",
			policy: &ateapipb.EgressPolicy{Metadata: &ateapipb.ResourceMetadata{Atespace: "team-a"}, Rules: rules},
			want:   filled,
		},
		{
			name:   "atespace omitted is filled",
			policy: &ateapipb.EgressPolicy{Metadata: &ateapipb.ResourceMetadata{Name: "default"}, Rules: rules},
			want:   filled,
		},
		{
			name:   "matching metadata is kept",
			policy: &ateapipb.EgressPolicy{Metadata: proto.Clone(pinned).(*ateapipb.ResourceMetadata), Rules: rules},
			want:   &ateapipb.EgressPolicy{Metadata: pinned, Rules: rules},
		},
		{
			name:            "atespace mismatch",
			policy:          &ateapipb.EgressPolicy{Metadata: &ateapipb.ResourceMetadata{Atespace: "dev"}, Rules: rules},
			wantErrContains: `metadata.atespace "dev" does not match --atespace "team-a"`,
		},
		{
			name:            "name mismatch",
			policy:          &ateapipb.EgressPolicy{Metadata: &ateapipb.ResourceMetadata{Name: "other"}, Rules: rules},
			wantErrContains: `metadata.name "other" must be "default"`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			err := fillEgressPolicyMetadata(test.policy, "team-a")
			if test.wantErrContains != "" {
				if err == nil || !strings.Contains(err.Error(), test.wantErrContains) {
					t.Fatalf("fillEgressPolicyMetadata() error = %v, want it to contain %q", err, test.wantErrContains)
				}
				return
			}
			if err != nil {
				t.Fatalf("fillEgressPolicyMetadata() error = %v, want nil", err)
			}
			if diff := cmp.Diff(test.want, test.policy, protocmp.Transform()); diff != "" {
				t.Errorf("policy mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

// A printed policy must decode back unchanged, so `get -o yaml` output can be
// fed to `create -f` as is.
func TestEgressPolicyManifest_RoundTrip(t *testing.T) {
	t.Parallel()

	policy := &ateapipb.EgressPolicy{
		Metadata: &ateapipb.ResourceMetadata{
			Atespace:   "team-a",
			Name:       "default",
			Uid:        "3f2b1c0e-8d5a-4b6e-9c1d-2a7e4f6b8c0d",
			Version:    2,
			CreateTime: timestamppb.New(time.Date(2026, 1, 1, 11, 55, 0, 0, time.UTC)),
			UpdateTime: timestamppb.New(time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)),
		},
		Rules: []*ateapipb.EgressRule{
			{Hostnames: &ateapipb.HostnameRule{Patterns: []string{"*.example.com"}}},
			{Cidrs: &ateapipb.CIDRRule{Cidrs: []string{"10.64.0.0/16"}}},
			{All: &emptypb.Empty{}},
		},
	}

	for _, format := range []string{"yaml", "json"} {
		t.Run(format, func(t *testing.T) {
			t.Parallel()
			var buf bytes.Buffer
			if err := printer.PrintEgressPolicyTo(&buf, "c1", policy, format); err != nil {
				t.Fatalf("PrintEgressPolicyTo(%s) error = %v", format, err)
			}
			got, err := egressPolicyFromManifest(buf.Bytes())
			if err != nil {
				t.Fatalf("egressPolicyFromManifest(%q) error = %v", buf.String(), err)
			}
			if diff := cmp.Diff(policy, got, protocmp.Transform()); diff != "" {
				t.Errorf("round trip mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

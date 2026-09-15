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
	"google.golang.org/protobuf/testing/protocmp"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestEgressPolicyFromManifest(t *testing.T) {
	t.Parallel()

	fullMetadata := &ateapipb.ResourceMetadata{Atespace: "team-a", Name: "default"}
	hostnames := []*ateapipb.EgressRule{{Hostnames: &ateapipb.HostnameRule{Patterns: []string{"api.example.com"}}}}
	cidrs := []*ateapipb.EgressRule{{Cidrs: &ateapipb.CIDRRule{Cidrs: []string{"10.64.0.0/16"}}}}

	tests := []struct {
		name     string
		manifest string
		atespace string
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
			atespace: "team-a",
			want:     &ateapipb.EgressPolicy{Metadata: fullMetadata, Rules: hostnames},
		},
		{
			name: "snake case cidrs",
			manifest: `metadata: {atespace: team-a, name: default}
rules:
- cidrs: {cidrs: ["10.64.0.0/16"]}
`,
			atespace: "team-a",
			want:     &ateapipb.EgressPolicy{Metadata: fullMetadata, Rules: cidrs},
		},
		{
			name: "all rule",
			manifest: `metadata: {atespace: team-a, name: default}
rules:
- all: {}
`,
			atespace: "team-a",
			want:     &ateapipb.EgressPolicy{Metadata: fullMetadata, Rules: []*ateapipb.EgressRule{{All: &emptypb.Empty{}}}},
		},
		{
			name: "metadata omitted is filled from atespace",
			manifest: `rules:
- hostnames: {patterns: [api.example.com]}
`,
			atespace: "team-a",
			want:     &ateapipb.EgressPolicy{Metadata: fullMetadata, Rules: hostnames},
		},
		{
			name: "name omitted is filled",
			manifest: `metadata: {atespace: team-a}
rules:
- hostnames: {patterns: [api.example.com]}
`,
			atespace: "team-a",
			want:     &ateapipb.EgressPolicy{Metadata: fullMetadata, Rules: hostnames},
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
			atespace: "team-a",
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
			atespace: "team-a",
			want:     &ateapipb.EgressPolicy{Metadata: fullMetadata, Rules: cidrs},
		},
		{name: "empty", manifest: "", atespace: "team-a", wantErr: true, wantErrContains: "manifest is empty"},
		{name: "unknown field", manifest: "rulez: []", atespace: "team-a", wantErr: true},
		{name: "rules not a list", manifest: "rules: {all: {}}", atespace: "team-a", wantErr: true},
		{
			name: "crd shape",
			manifest: `apiVersion: ate.dev/v1alpha1
kind: EgressPolicy
metadata: {name: default}
`,
			atespace: "team-a",
			wantErr:  true,
		},
		{name: "not yaml", manifest: "\t{", atespace: "team-a", wantErr: true, wantErrContains: "invalid YAML"},
		{
			name: "atespace mismatch",
			manifest: `metadata: {atespace: dev}
rules:
- all: {}
`,
			atespace:        "team-a",
			wantErr:         true,
			wantErrContains: `metadata.atespace "dev" does not match --atespace "team-a"`,
		},
		{
			name: "name mismatch",
			manifest: `metadata: {name: other}
rules:
- all: {}
`,
			atespace:        "team-a",
			wantErr:         true,
			wantErrContains: `metadata.name "other" must be "default"`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			got, err := egressPolicyFromManifest([]byte(test.manifest), test.atespace)
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
			got, err := egressPolicyFromManifest(buf.Bytes(), "team-a")
			if err != nil {
				t.Fatalf("egressPolicyFromManifest(%q) error = %v", buf.String(), err)
			}
			if diff := cmp.Diff(policy, got, protocmp.Transform()); diff != "" {
				t.Errorf("round trip mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

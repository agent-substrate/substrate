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

package defaults

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"go.ate.dev/substrate/api/ateapi/v1"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/testing/protocmp"
)

func TestApply(t *testing.T) {
	const (
		scopeFull = ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY
		scopeData = ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_VOLUMES
	)
	// defaultSnapshotConfig is what an omitted snapshot_config defaults to.
	defaultSnapshotConfig := func() *ateapi.SnapshotConfig {
		return &ateapi.SnapshotConfig{
			PreferredFidelity:    scopeFull,
			GoldenSnapshotConfig: &ateapi.GoldenSnapshotConfig{Mode: ateapi.GoldenSnapshotMode_GOLDEN_SNAPSHOT_MODE_ENABLED},
		}
	}
	tests := []struct {
		name string
		in   proto.Message
		want proto.Message
	}{{
		name: "nil actor template is tolerated",
		in:   (*ateapi.ActorTemplate)(nil),
		want: (*ateapi.ActorTemplate)(nil),
	}, {
		name: "missing snapshot_config gets every default",
		in:   &ateapi.ActorTemplate{},
		want: &ateapi.ActorTemplate{SnapshotConfig: defaultSnapshotConfig()},
	}, {
		name: "empty snapshot_config gets every default",
		in:   &ateapi.ActorTemplate{SnapshotConfig: &ateapi.SnapshotConfig{}},
		want: &ateapi.ActorTemplate{SnapshotConfig: &ateapi.SnapshotConfig{
			PreferredFidelity:    scopeFull,
			GoldenSnapshotConfig: &ateapi.GoldenSnapshotConfig{Mode: ateapi.GoldenSnapshotMode_GOLDEN_SNAPSHOT_MODE_ENABLED},
		}},
	}, {
		name: "set scopes are kept",
		in: &ateapi.ActorTemplate{SnapshotConfig: &ateapi.SnapshotConfig{
			PreferredFidelity: scopeData,
		}},
		want: &ateapi.ActorTemplate{SnapshotConfig: &ateapi.SnapshotConfig{
			PreferredFidelity:    scopeData,
			GoldenSnapshotConfig: &ateapi.GoldenSnapshotConfig{Mode: ateapi.GoldenSnapshotMode_GOLDEN_SNAPSHOT_MODE_ENABLED},
		}},
	}, {
		name: "container without wakeup probe stays without one",
		in:   &ateapi.ActorTemplate{Containers: []*ateapi.Container{{Name: "main"}}},
		want: &ateapi.ActorTemplate{SnapshotConfig: defaultSnapshotConfig(), Containers: []*ateapi.Container{{Name: "main"}}},
	}, {
		name: "wakeup probe gets timeout and path",
		in: &ateapi.ActorTemplate{Containers: []*ateapi.Container{
			{Name: "main", WakeupProbe: &ateapi.WakeupProbe{HttpGet: &ateapi.HttpGetAction{Port: 8080}}},
		}},
		want: &ateapi.ActorTemplate{SnapshotConfig: defaultSnapshotConfig(), Containers: []*ateapi.Container{
			{Name: "main", WakeupProbe: &ateapi.WakeupProbe{
				HttpGet:        &ateapi.HttpGetAction{Port: 8080, Path: "/"},
				TimeoutSeconds: 30,
			}},
		}},
	}, {
		name: "wakeup probe without http_get gets only the timeout",
		in: &ateapi.ActorTemplate{Containers: []*ateapi.Container{
			{Name: "main", WakeupProbe: &ateapi.WakeupProbe{}},
		}},
		want: &ateapi.ActorTemplate{SnapshotConfig: defaultSnapshotConfig(), Containers: []*ateapi.Container{
			{Name: "main", WakeupProbe: &ateapi.WakeupProbe{TimeoutSeconds: 30}},
		}},
	}, {
		name: "set wakeup probe fields are kept",
		in: &ateapi.ActorTemplate{Containers: []*ateapi.Container{
			{Name: "main", WakeupProbe: &ateapi.WakeupProbe{
				HttpGet:        &ateapi.HttpGetAction{Port: 8080, Path: "/healthz"},
				TimeoutSeconds: 5,
			}},
		}},
		want: &ateapi.ActorTemplate{SnapshotConfig: defaultSnapshotConfig(), Containers: []*ateapi.Container{
			{Name: "main", WakeupProbe: &ateapi.WakeupProbe{
				HttpGet:        &ateapi.HttpGetAction{Port: 8080, Path: "/healthz"},
				TimeoutSeconds: 5,
			}},
		}},
	}, {
		name: "every container is defaulted",
		in: &ateapi.ActorTemplate{Containers: []*ateapi.Container{
			{Name: "a", WakeupProbe: &ateapi.WakeupProbe{HttpGet: &ateapi.HttpGetAction{Port: 1}}},
			{Name: "b"},
			{Name: "c", WakeupProbe: &ateapi.WakeupProbe{HttpGet: &ateapi.HttpGetAction{Port: 3}}},
		}},
		want: &ateapi.ActorTemplate{SnapshotConfig: defaultSnapshotConfig(), Containers: []*ateapi.Container{
			{Name: "a", WakeupProbe: &ateapi.WakeupProbe{HttpGet: &ateapi.HttpGetAction{Port: 1, Path: "/"}, TimeoutSeconds: 30}},
			{Name: "b"},
			{Name: "c", WakeupProbe: &ateapi.WakeupProbe{HttpGet: &ateapi.HttpGetAction{Port: 3, Path: "/"}, TimeoutSeconds: 30}},
		}},
	}, {
		name: "actor has no defaults",
		in:   &ateapi.Actor{WorkerSelector: &ateapi.Selector{MatchLabels: map[string]string{"tier": "1"}}},
		want: &ateapi.Actor{WorkerSelector: &ateapi.Selector{MatchLabels: map[string]string{"tier": "1"}}},
	}, {
		name: "atespace has no defaults",
		in:   &ateapi.Atespace{Metadata: &ateapi.ResourceMetadata{Name: "team-a"}},
		want: &ateapi.Atespace{Metadata: &ateapi.ResourceMetadata{Name: "team-a"}},
	}, {
		name: "egress policy defaults http and https ports only",
		in: &ateapi.EgressPolicy{Rules: []*ateapi.EgressRule{
			{Http: &ateapi.HttpRule{Hostnames: []string{"a.example.com"}}},
			{Https: &ateapi.HttpsRule{Hostnames: []string{"b.example.com"}}},
			{Http: &ateapi.HttpRule{Hostnames: []string{"c.example.com"}, Ports: &ateapi.Ports{All: &ateapi.AllPorts{}}}},
			{TlsPassthrough: &ateapi.TlsPassthroughRule{Hostnames: []string{"d.example.com"}}},
			{},
		}},
		want: &ateapi.EgressPolicy{Rules: []*ateapi.EgressRule{
			{Http: &ateapi.HttpRule{Hostnames: []string{"a.example.com"}, Ports: &ateapi.Ports{Numbers: []int32{80}}}},
			{Https: &ateapi.HttpsRule{Hostnames: []string{"b.example.com"}, Ports: &ateapi.Ports{Numbers: []int32{443}}}},
			{Http: &ateapi.HttpRule{Hostnames: []string{"c.example.com"}, Ports: &ateapi.Ports{All: &ateapi.AllPorts{}}}},
			{TlsPassthrough: &ateapi.TlsPassthroughRule{Hostnames: []string{"d.example.com"}}},
			{},
		}},
	}, {
		name: "template defaults egress ports",
		in: &ateapi.ActorTemplate{GoldenEgressPolicy: &ateapi.EgressPolicyTemplate{Rules: []*ateapi.EgressRule{
			{Http: &ateapi.HttpRule{Hostnames: []string{"api.example.com"}}},
			{Https: &ateapi.HttpsRule{Hostnames: []string{"api.example.com"}}},
			{Https: &ateapi.HttpsRule{Hostnames: []string{"custom.example.com"}, Ports: &ateapi.Ports{Numbers: []int32{8443}}}},
			{TlsPassthrough: &ateapi.TlsPassthroughRule{Hostnames: []string{"tcp.example.com"}}},
		}}},
		want: &ateapi.ActorTemplate{SnapshotConfig: defaultSnapshotConfig(), GoldenEgressPolicy: &ateapi.EgressPolicyTemplate{Rules: []*ateapi.EgressRule{
			{Http: &ateapi.HttpRule{Hostnames: []string{"api.example.com"}, Ports: &ateapi.Ports{Numbers: []int32{80}}}},
			{Https: &ateapi.HttpsRule{Hostnames: []string{"api.example.com"}, Ports: &ateapi.Ports{Numbers: []int32{443}}}},
			{Https: &ateapi.HttpsRule{Hostnames: []string{"custom.example.com"}, Ports: &ateapi.Ports{Numbers: []int32{8443}}}},
			{TlsPassthrough: &ateapi.TlsPassthroughRule{Hostnames: []string{"tcp.example.com"}}},
		}}},
	}, {
		name: "tag has no defaults",
		in:   &ateapi.Tag{Scope: ateapi.TagScope_TAG_SCOPE_PUBLISHED},
		want: &ateapi.Tag{Scope: ateapi.TagScope_TAG_SCOPE_PUBLISHED},
	}, {
		name: "worker has no defaults",
		in:   &ateapi.Worker{Metadata: &ateapi.ResourceMetadata{Name: "worker-1"}},
		want: &ateapi.Worker{Metadata: &ateapi.ResourceMetadata{Name: "worker-1"}},
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := proto.Clone(tt.in)
			Apply(got)
			if diff := cmp.Diff(tt.want, got, protocmp.Transform()); diff != "" {
				t.Fatalf("Apply mismatch (-want +got):\n%s", diff)
			}
			again := proto.Clone(got)
			Apply(again)
			if diff := cmp.Diff(got, again, protocmp.Transform()); diff != "" {
				t.Errorf("Apply is not idempotent (-once +twice):\n%s", diff)
			}
		})
	}
}

func TestGoldenSnapshotDefaults(t *testing.T) {
	for _, tt := range []struct {
		name   string
		config string
		mode   ateapi.GoldenSnapshotMode
	}{
		{"omitted config", `{}`, ateapi.GoldenSnapshotMode_GOLDEN_SNAPSHOT_MODE_ENABLED},
		{"omitted mode", `{"goldenSnapshotConfig": {}}`, ateapi.GoldenSnapshotMode_GOLDEN_SNAPSHOT_MODE_ENABLED},
		{"unspecified mode", `{"goldenSnapshotConfig": {"mode": "GOLDEN_SNAPSHOT_MODE_UNSPECIFIED"}}`, ateapi.GoldenSnapshotMode_GOLDEN_SNAPSHOT_MODE_ENABLED},
		{"explicit zero", `{"goldenSnapshotConfig": {"mode": 0}}`, ateapi.GoldenSnapshotMode_GOLDEN_SNAPSHOT_MODE_ENABLED},
		{"enabled", `{"goldenSnapshotConfig": {"mode": "GOLDEN_SNAPSHOT_MODE_ENABLED"}}`, ateapi.GoldenSnapshotMode_GOLDEN_SNAPSHOT_MODE_ENABLED},
		{"disabled", `{"goldenSnapshotConfig": {"mode": "GOLDEN_SNAPSHOT_MODE_DISABLED"}}`, ateapi.GoldenSnapshotMode_GOLDEN_SNAPSHOT_MODE_DISABLED},
	} {
		t.Run(tt.name, func(t *testing.T) {
			tmpl := &ateapi.ActorTemplate{}
			if err := protojson.Unmarshal([]byte(`{"snapshotConfig": `+tt.config+`}`), tmpl); err != nil {
				t.Fatal(err)
			}
			// JSON and gRPC use the enum value rather than field presence.
			data, err := proto.Marshal(tmpl)
			if err != nil {
				t.Fatal(err)
			}
			if err := proto.Unmarshal(data, tmpl); err != nil {
				t.Fatal(err)
			}
			Apply(tmpl)
			want := &ateapi.GoldenSnapshotConfig{Mode: tt.mode}
			if diff := cmp.Diff(want, tmpl.GetSnapshotConfig().GetGoldenSnapshotConfig(), protocmp.Transform()); diff != "" {
				t.Fatalf("golden snapshot default mismatch (-want +got):\n%s", diff)
			}
			again := proto.Clone(tmpl)
			Apply(again)
			if !proto.Equal(tmpl, again) {
				t.Fatal("golden snapshot defaulting is not idempotent")
			}
		})
	}
}

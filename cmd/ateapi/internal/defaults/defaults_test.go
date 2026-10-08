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

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/testing/protocmp"
)

func TestApply(t *testing.T) {
	const (
		scopeFull = ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY
		scopeData = ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_VOLUMES
	)
	tests := []struct {
		name string
		in   proto.Message
		want proto.Message
	}{{
		name: "nil actor template is tolerated",
		in:   (*ateapipb.ActorTemplate)(nil),
		want: (*ateapipb.ActorTemplate)(nil),
	}, {
		name: "missing snapshot_config is left for validation",
		in:   &ateapipb.ActorTemplate{},
		want: &ateapipb.ActorTemplate{},
	}, {
		name: "empty snapshot_config gets every default",
		in:   &ateapipb.ActorTemplate{SnapshotConfig: &ateapipb.SnapshotConfig{}},
		want: &ateapipb.ActorTemplate{SnapshotConfig: &ateapipb.SnapshotConfig{
			PreferredFidelity:    scopeFull,
			GoldenSnapshotConfig: &ateapipb.GoldenSnapshotConfig{Enabled: proto.Bool(true)},
		}},
	}, {
		name: "set scopes are kept",
		in: &ateapipb.ActorTemplate{SnapshotConfig: &ateapipb.SnapshotConfig{
			PreferredFidelity: scopeData,
		}},
		want: &ateapipb.ActorTemplate{SnapshotConfig: &ateapipb.SnapshotConfig{
			PreferredFidelity:    scopeData,
			GoldenSnapshotConfig: &ateapipb.GoldenSnapshotConfig{Enabled: proto.Bool(true)},
		}},
	}, {
		name: "container without wakeup probe stays without one",
		in:   &ateapipb.ActorTemplate{Containers: []*ateapipb.Container{{Name: "main"}}},
		want: &ateapipb.ActorTemplate{Containers: []*ateapipb.Container{{Name: "main"}}},
	}, {
		name: "wakeup probe gets timeout and path",
		in: &ateapipb.ActorTemplate{Containers: []*ateapipb.Container{
			{Name: "main", WakeupProbe: &ateapipb.ContainerWakeupProbe{HttpGet: &ateapipb.HTTPGetAction{Port: 8080}}},
		}},
		want: &ateapipb.ActorTemplate{Containers: []*ateapipb.Container{
			{Name: "main", WakeupProbe: &ateapipb.ContainerWakeupProbe{
				HttpGet:        &ateapipb.HTTPGetAction{Port: 8080, Path: "/"},
				TimeoutSeconds: 30,
			}},
		}},
	}, {
		name: "wakeup probe without http_get gets only the timeout",
		in: &ateapipb.ActorTemplate{Containers: []*ateapipb.Container{
			{Name: "main", WakeupProbe: &ateapipb.ContainerWakeupProbe{}},
		}},
		want: &ateapipb.ActorTemplate{Containers: []*ateapipb.Container{
			{Name: "main", WakeupProbe: &ateapipb.ContainerWakeupProbe{TimeoutSeconds: 30}},
		}},
	}, {
		name: "set wakeup probe fields are kept",
		in: &ateapipb.ActorTemplate{Containers: []*ateapipb.Container{
			{Name: "main", WakeupProbe: &ateapipb.ContainerWakeupProbe{
				HttpGet:        &ateapipb.HTTPGetAction{Port: 8080, Path: "/healthz"},
				TimeoutSeconds: 5,
			}},
		}},
		want: &ateapipb.ActorTemplate{Containers: []*ateapipb.Container{
			{Name: "main", WakeupProbe: &ateapipb.ContainerWakeupProbe{
				HttpGet:        &ateapipb.HTTPGetAction{Port: 8080, Path: "/healthz"},
				TimeoutSeconds: 5,
			}},
		}},
	}, {
		name: "every container is defaulted",
		in: &ateapipb.ActorTemplate{Containers: []*ateapipb.Container{
			{Name: "a", WakeupProbe: &ateapipb.ContainerWakeupProbe{HttpGet: &ateapipb.HTTPGetAction{Port: 1}}},
			{Name: "b"},
			{Name: "c", WakeupProbe: &ateapipb.ContainerWakeupProbe{HttpGet: &ateapipb.HTTPGetAction{Port: 3}}},
		}},
		want: &ateapipb.ActorTemplate{Containers: []*ateapipb.Container{
			{Name: "a", WakeupProbe: &ateapipb.ContainerWakeupProbe{HttpGet: &ateapipb.HTTPGetAction{Port: 1, Path: "/"}, TimeoutSeconds: 30}},
			{Name: "b"},
			{Name: "c", WakeupProbe: &ateapipb.ContainerWakeupProbe{HttpGet: &ateapipb.HTTPGetAction{Port: 3, Path: "/"}, TimeoutSeconds: 30}},
		}},
	}, {
		name: "actor has no defaults",
		in:   &ateapipb.Actor{WorkerSelector: &ateapipb.Selector{MatchLabels: map[string]string{"tier": "1"}}},
		want: &ateapipb.Actor{WorkerSelector: &ateapipb.Selector{MatchLabels: map[string]string{"tier": "1"}}},
	}, {
		name: "atespace has no defaults",
		in:   &ateapipb.Atespace{Metadata: &ateapipb.ResourceMetadata{Name: "team-a"}},
		want: &ateapipb.Atespace{Metadata: &ateapipb.ResourceMetadata{Name: "team-a"}},
	}, {
		name: "egress policy defaults http and https ports only",
		in: &ateapipb.EgressPolicy{Rules: []*ateapipb.EgressRule{
			{Http: &ateapipb.HTTPRule{Hostnames: []string{"a.example.com"}}},
			{Https: &ateapipb.HTTPSRule{Hostnames: []string{"b.example.com"}}},
			{Http: &ateapipb.HTTPRule{Hostnames: []string{"c.example.com"}, Ports: &ateapipb.Ports{All: &ateapipb.AllPorts{}}}},
			{TlsPassthrough: &ateapipb.TLSPassthroughRule{Hostnames: []string{"d.example.com"}}},
			{},
		}},
		want: &ateapipb.EgressPolicy{Rules: []*ateapipb.EgressRule{
			{Http: &ateapipb.HTTPRule{Hostnames: []string{"a.example.com"}, Ports: &ateapipb.Ports{Numbers: []int32{80}}}},
			{Https: &ateapipb.HTTPSRule{Hostnames: []string{"b.example.com"}, Ports: &ateapipb.Ports{Numbers: []int32{443}}}},
			{Http: &ateapipb.HTTPRule{Hostnames: []string{"c.example.com"}, Ports: &ateapipb.Ports{All: &ateapipb.AllPorts{}}}},
			{TlsPassthrough: &ateapipb.TLSPassthroughRule{Hostnames: []string{"d.example.com"}}},
			{},
		}},
	}, {
		name: "tag has no defaults",
		in:   &ateapipb.Tag{Scope: ateapipb.TagScope_TAG_SCOPE_PUBLISHED},
		want: &ateapipb.Tag{Scope: ateapipb.TagScope_TAG_SCOPE_PUBLISHED},
	}, {
		name: "worker has no defaults",
		in:   &ateapipb.Worker{Metadata: &ateapipb.ResourceMetadata{Name: "worker-1"}},
		want: &ateapipb.Worker{Metadata: &ateapipb.ResourceMetadata{Name: "worker-1"}},
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
		name    string
		config  string
		enabled bool
	}{
		{"omitted config", `{}`, true},
		{"omitted enabled", `{"goldenSnapshotConfig": {}}`, true},
		{"enabled", `{"goldenSnapshotConfig": {"enabled": true}}`, true},
		{"disabled", `{"goldenSnapshotConfig": {"enabled": false}}`, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			tmpl := &ateapipb.ActorTemplate{}
			if err := protojson.Unmarshal([]byte(`{"snapshotConfig": `+tt.config+`}`), tmpl); err != nil {
				t.Fatal(err)
			}
			// Both JSON and gRPC must preserve an explicit false through defaulting.
			data, err := proto.Marshal(tmpl)
			if err != nil {
				t.Fatal(err)
			}
			if err := proto.Unmarshal(data, tmpl); err != nil {
				t.Fatal(err)
			}
			Apply(tmpl)
			want := &ateapipb.GoldenSnapshotConfig{Enabled: proto.Bool(tt.enabled)}
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

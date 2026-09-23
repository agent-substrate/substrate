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
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/testing/protocmp"
)

func TestApply(t *testing.T) {
	const (
		scopeFull = ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_FULL
		scopeData = ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_DATA
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
			OnPause:  scopeFull,
			OnCommit: scopeFull,
			OnResume: &ateapipb.OnResumeConfig{FromData: ateapipb.ResumeSource_RESUME_SOURCE_COLD_BOOT},
		}},
	}, {
		name: "set scopes are kept",
		in: &ateapipb.ActorTemplate{SnapshotConfig: &ateapipb.SnapshotConfig{
			OnPause:  scopeFull,
			OnCommit: scopeData,
			OnResume: &ateapipb.OnResumeConfig{FromData: ateapipb.ResumeSource_RESUME_SOURCE_GOLDEN},
		}},
		want: &ateapipb.ActorTemplate{SnapshotConfig: &ateapipb.SnapshotConfig{
			OnPause:  scopeFull,
			OnCommit: scopeData,
			OnResume: &ateapipb.OnResumeConfig{FromData: ateapipb.ResumeSource_RESUME_SOURCE_GOLDEN},
		}},
	}, {
		name: "present but empty on_resume gets from_data",
		in: &ateapipb.ActorTemplate{SnapshotConfig: &ateapipb.SnapshotConfig{
			OnPause: scopeFull, OnCommit: scopeFull, OnResume: &ateapipb.OnResumeConfig{},
		}},
		want: &ateapipb.ActorTemplate{SnapshotConfig: &ateapipb.SnapshotConfig{
			OnPause: scopeFull, OnCommit: scopeFull,
			OnResume: &ateapipb.OnResumeConfig{FromData: ateapipb.ResumeSource_RESUME_SOURCE_COLD_BOOT},
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
		name: "egress policy has no defaults",
		in:   &ateapipb.EgressPolicy{Metadata: &ateapipb.ResourceMetadata{Name: "default"}},
		want: &ateapipb.EgressPolicy{Metadata: &ateapipb.ResourceMetadata{Name: "default"}},
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

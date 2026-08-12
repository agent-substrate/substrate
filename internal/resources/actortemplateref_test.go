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

package resources

import (
	"testing"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

func TestActorTemplateRefString(t *testing.T) {
	got := ActorTemplateRef{Atespace: "team-a", Name: "tmpl-1"}.String()
	if want := "team-a/tmpl-1"; got != want {
		t.Errorf("String() = %q, want %q", got, want)
	}
}

func TestActorTemplateRefObjectRefRoundTrip(t *testing.T) {
	templateRef := ActorTemplateRef{Atespace: "team-a", Name: "tmpl-1"}

	obj := templateRef.ToObjectRef()
	if obj.GetAtespace() != "team-a" || obj.GetName() != "tmpl-1" {
		t.Errorf("ToObjectRef() = (%q, %q), want (team-a, tmpl-1)", obj.GetAtespace(), obj.GetName())
	}
	if got := ActorTemplateRefFromObjectRef(obj); got != templateRef {
		t.Errorf("round-trip = %+v, want %+v", got, templateRef)
	}
}

func TestActorTemplateRefFromActorTemplate(t *testing.T) {
	tests := []struct {
		name     string
		template *ateapipb.ActorTemplate
		want     ActorTemplateRef
	}{
		{
			name: "populated",
			template: &ateapipb.ActorTemplate{Metadata: &ateapipb.ResourceMetadata{
				Atespace: "team-a",
				Name:     "tmpl-1",
			}},
			want: ActorTemplateRef{Atespace: "team-a", Name: "tmpl-1"},
		},
		{"nil template", nil, ActorTemplateRef{}},
		{"nil metadata", &ateapipb.ActorTemplate{}, ActorTemplateRef{}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ActorTemplateRefFromActorTemplate(tt.template); got != tt.want {
				t.Errorf("ActorTemplateRefFromActorTemplate() = %+v, want %+v", got, tt.want)
			}
		})
	}
}

func TestActorTemplateVersionRefString(t *testing.T) {
	got := ActorTemplateVersionRef{Atespace: "team-a", Name: "tmpl-1-v1"}.String()
	if want := "team-a/tmpl-1-v1"; got != want {
		t.Errorf("String() = %q, want %q", got, want)
	}
}

func TestActorTemplateVersionRefObjectRefRoundTrip(t *testing.T) {
	versionRef := ActorTemplateVersionRef{Atespace: "team-a", Name: "tmpl-1-v1"}

	obj := versionRef.ToObjectRef()
	if obj.GetAtespace() != "team-a" || obj.GetName() != "tmpl-1-v1" {
		t.Errorf("ToObjectRef() = (%q, %q), want (team-a, tmpl-1-v1)", obj.GetAtespace(), obj.GetName())
	}
	if got := ActorTemplateVersionRefFromObjectRef(obj); got != versionRef {
		t.Errorf("round-trip = %+v, want %+v", got, versionRef)
	}
}

func TestActorTemplateVersionRefFromActorTemplateVersion(t *testing.T) {
	tests := []struct {
		name    string
		version *ateapipb.ActorTemplateVersion
		want    ActorTemplateVersionRef
	}{
		{
			name: "populated",
			version: &ateapipb.ActorTemplateVersion{Metadata: &ateapipb.ResourceMetadata{
				Atespace: "team-a",
				Name:     "tmpl-1-v1",
			}},
			want: ActorTemplateVersionRef{Atespace: "team-a", Name: "tmpl-1-v1"},
		},
		{"nil version", nil, ActorTemplateVersionRef{}},
		{"nil metadata", &ateapipb.ActorTemplateVersion{}, ActorTemplateVersionRef{}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ActorTemplateVersionRefFromActorTemplateVersion(tt.version); got != tt.want {
				t.Errorf("ActorTemplateVersionRefFromActorTemplateVersion() = %+v, want %+v", got, tt.want)
			}
		})
	}
}

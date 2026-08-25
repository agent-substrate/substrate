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
	"log/slog"
	"reflect"
	"testing"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

// The ref aliases must stay distinct types: the phantom ResourceRef kind is
// what stops a reference to one resource kind from being passed where another
// is expected.
func TestRefAliasesAreDistinctTypes(t *testing.T) {
	types := map[string]reflect.Type{
		"ActorRef":         reflect.TypeFor[ActorRef](),
		"ActorTemplateRef": reflect.TypeFor[ActorTemplateRef](),
	}
	seen := make(map[reflect.Type]string)
	for name, typ := range types {
		if other, dup := seen[typ]; dup {
			t.Errorf("%s and %s are the same type; the phantom kind marker was lost", name, other)
		}
		seen[typ] = name
	}
}

// The type attr comes from the type parameter alone, so a zero ref — where no
// value of R was ever constructed — must log without panicking and still name
// its resource kind.
func TestResourceRefLogValue(t *testing.T) {
	tests := []struct {
		name string
		val  slog.Value
		want map[string]string
	}{
		{
			name: "zero actor ref",
			val:  ActorRef{}.LogValue(),
			want: map[string]string{"type": "*ateapipb.Actor", "atespace": "", "name": ""},
		},
		{
			name: "zero template ref",
			val:  ActorTemplateRef{}.LogValue(),
			want: map[string]string{"type": "*ateapipb.ActorTemplate", "atespace": "", "name": ""},
		},
		{
			name: "populated actor ref",
			val:  ActorRef{Atespace: "team-a", Name: "act-1"}.LogValue(),
			want: map[string]string{"type": "*ateapipb.Actor", "atespace": "team-a", "name": "act-1"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if kind := tt.val.Kind(); kind != slog.KindGroup {
				t.Fatalf("LogValue() kind = %v, want %v", kind, slog.KindGroup)
			}
			got := make(map[string]string)
			for _, attr := range tt.val.Group() {
				got[attr.Key] = attr.Value.String()
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("LogValue() attrs = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestActorRefString(t *testing.T) {
	got := ActorRef{Atespace: "team-a", Name: "act-1"}.String()
	if want := "team-a/act-1"; got != want {
		t.Errorf("String() = %q, want %q", got, want)
	}
}

func TestActorRefDNSName(t *testing.T) {
	actorRef := ActorRef{Atespace: "team-a", Name: "act-1"}

	got := ActorDNSName(actorRef)
	want := "act-1.team-a.actors.resources.substrate.ate.dev"
	if got != want {
		t.Errorf("ActorDNSName() = %q, want %q", got, want)
	}

	parsed, err := ParseActorDNSName(got)
	if err != nil {
		t.Fatalf("ParseActorDNSName(%q) error = %v", got, err)
	}
	if parsed != actorRef {
		t.Errorf("round-trip = %+v, want %+v", parsed, actorRef)
	}
}

func TestParseActorDNSName(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    ActorRef
		wantErr bool
	}{
		{"valid", "act-1.team-a.actors.resources.substrate.ate.dev", ActorRef{Atespace: "team-a", Name: "act-1"}, false},
		{"valid trailing dot", "act-1.team-a.actors.resources.substrate.ate.dev.", ActorRef{Atespace: "team-a", Name: "act-1"}, false},
		{"wrong suffix", "act-1.team-a.example.com", ActorRef{}, true},
		{"missing atespace", "act-1.actors.resources.substrate.ate.dev", ActorRef{}, true},
		{"invalid actor name", "ACT-1.team-a.actors.resources.substrate.ate.dev", ActorRef{}, true},
		{"invalid atespace", "act-1.TEAM.actors.resources.substrate.ate.dev", ActorRef{}, true},
		{"host:port not accepted", "act-1.team-a.actors.resources.substrate.ate.dev:8080", ActorRef{}, true},
		{"empty", "", ActorRef{}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseActorDNSName(tt.input)
			if (err != nil) != tt.wantErr {
				t.Fatalf("ParseActorDNSName(%q) error = %v, wantErr %v", tt.input, err, tt.wantErr)
			}
			if got != tt.want {
				t.Errorf("ParseActorDNSName(%q) = %+v, want %+v", tt.input, got, tt.want)
			}
		})
	}
}

func TestActorRefObjectRefRoundTrip(t *testing.T) {
	actorRef := ActorRef{Atespace: "team-a", Name: "act-1"}

	obj := actorRef.ToObjectRef()
	if obj.GetAtespace() != "team-a" || obj.GetName() != "act-1" {
		t.Errorf("ToObjectRef() = (%q, %q), want (team-a, act-1)", obj.GetAtespace(), obj.GetName())
	}
	if got := ActorRefFromObjectRef(obj); got != actorRef {
		t.Errorf("round-trip = %+v, want %+v", got, actorRef)
	}
}

func TestActorRefFromActor(t *testing.T) {
	tests := []struct {
		name  string
		actor *ateapipb.Actor
		want  ActorRef
	}{
		{
			name: "populated",
			actor: &ateapipb.Actor{Metadata: &ateapipb.ResourceMetadata{
				Atespace: "team-a",
				Name:     "act-1",
			}},
			want: ActorRef{Atespace: "team-a", Name: "act-1"},
		},
		{"nil actor", nil, ActorRef{}},
		{"nil metadata", &ateapipb.Actor{}, ActorRef{}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ActorRefFromActor(tt.actor); got != tt.want {
				t.Errorf("ActorRefFromActor() = %+v, want %+v", got, tt.want)
			}
		})
	}
}

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

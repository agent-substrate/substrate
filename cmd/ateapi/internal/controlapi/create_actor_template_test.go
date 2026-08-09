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

package controlapi

import (
	"testing"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

func TestValidateCreateActorTemplateRequest(t *testing.T) {
	tests := []struct {
		name string
		req  *ateapipb.CreateActorTemplateRequest
		want field.ErrorList
	}{{
		"valid",
		&ateapipb.CreateActorTemplateRequest{ActorTemplate: &ateapipb.ActorTemplate{
			Metadata: &ateapipb.ResourceMetadata{Name: "tmpl-a"},
		}},
		nil,
	}, {
		"valid with worker selector",
		&ateapipb.CreateActorTemplateRequest{ActorTemplate: &ateapipb.ActorTemplate{
			Metadata: &ateapipb.ResourceMetadata{Name: "tmpl-a"},
			Spec: &ateapipb.ActorTemplateSpec{
				WorkerSelector: &ateapipb.Selector{MatchLabels: map[string]string{"tier": "1"}},
			},
		}},
		nil,
	}, {
		"missing actor_template",
		&ateapipb.CreateActorTemplateRequest{},
		field.ErrorList{field.Required(field.NewPath("actor_template"), "")},
	}, {
		"metadata.atespace must be empty",
		&ateapipb.CreateActorTemplateRequest{ActorTemplate: &ateapipb.ActorTemplate{
			Metadata: &ateapipb.ResourceMetadata{Atespace: "ns1", Name: "tmpl-a"},
		}},
		field.ErrorList{field.Invalid(field.NewPath("actor_template", "metadata", "atespace"), "ns1", "")},
	}, {
		"missing metadata.name",
		&ateapipb.CreateActorTemplateRequest{ActorTemplate: &ateapipb.ActorTemplate{
			Metadata: &ateapipb.ResourceMetadata{},
		}},
		field.ErrorList{field.Required(field.NewPath("actor_template", "metadata", "name"), "")},
	}, {
		"invalid metadata.name",
		&ateapipb.CreateActorTemplateRequest{ActorTemplate: &ateapipb.ActorTemplate{
			Metadata: &ateapipb.ResourceMetadata{Name: "Tmpl_A"},
		}},
		field.ErrorList{field.Invalid(field.NewPath("actor_template", "metadata", "name"), "Tmpl_A", "")},
	}, {
		"invalid worker selector",
		&ateapipb.CreateActorTemplateRequest{ActorTemplate: &ateapipb.ActorTemplate{
			Metadata: &ateapipb.ResourceMetadata{Name: "tmpl-a"},
			Spec: &ateapipb.ActorTemplateSpec{
				WorkerSelector: &ateapipb.Selector{MatchLabels: map[string]string{"bad key!": "1"}},
			},
		}},
		field.ErrorList{field.Invalid(field.NewPath("actor_template", "spec", "worker_selector", "match_labels").Key("bad key!"), "bad key!", "")},
	}, {
		"default_version_on_create forbidden at creation",
		&ateapipb.CreateActorTemplateRequest{ActorTemplate: &ateapipb.ActorTemplate{
			Metadata: &ateapipb.ResourceMetadata{Name: "tmpl-a"},
			Spec: &ateapipb.ActorTemplateSpec{
				DefaultVersionOnCreate: &ateapipb.ObjectRef{Name: "tmpl-a-v1"},
			},
		}},
		field.ErrorList{field.Forbidden(field.NewPath("actor_template", "spec", "default_version_on_create"), "")},
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertValidateErr(t, validateCreateActorTemplateRequest(tt.req), tt.want)
		})
	}
}

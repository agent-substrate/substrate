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

func TestValidateCreateActorTemplateVersionRequest(t *testing.T) {
	validReq := func(mutations ...func(*ateapipb.ActorTemplateVersion)) *ateapipb.CreateActorTemplateVersionRequest {
		version := validTemplateVersion()
		version.Metadata = &ateapipb.ResourceMetadata{Atespace: "ns1", Name: "tmpl-a-v1"}
		version.ActorTemplate = &ateapipb.ObjectRef{Atespace: "ns1", Name: "tmpl-a"}
		for _, m := range mutations {
			m(version)
		}
		return &ateapipb.CreateActorTemplateVersionRequest{ActorTemplateVersion: version}
	}

	tests := []struct {
		name string
		req  *ateapipb.CreateActorTemplateVersionRequest
		want field.ErrorList
	}{{
		"valid",
		validReq(),
		nil,
	}, {
		"missing actor_template_version",
		&ateapipb.CreateActorTemplateVersionRequest{},
		field.ErrorList{field.Required(field.NewPath("actor_template_version"), "")},
	}, {
		"missing metadata.atespace",
		validReq(func(v *ateapipb.ActorTemplateVersion) { v.Metadata.Atespace = "" }),
		field.ErrorList{field.Required(field.NewPath("actor_template_version", "metadata", "atespace"), "")},
	}, {
		"invalid metadata.atespace",
		validReq(func(v *ateapipb.ActorTemplateVersion) { v.Metadata.Atespace = "NS_1" }),
		field.ErrorList{
			field.Invalid(field.NewPath("actor_template_version", "metadata", "atespace"), "NS_1", ""),
			// The parent ref no longer matches the (invalid) version atespace.
			field.Invalid(field.NewPath("actor_template_version", "actor_template", "atespace"), "ns1", ""),
		},
	}, {
		"missing metadata.name",
		validReq(func(v *ateapipb.ActorTemplateVersion) { v.Metadata.Name = "" }),
		field.ErrorList{field.Required(field.NewPath("actor_template_version", "metadata", "name"), "")},
	}, {
		"invalid metadata.name",
		validReq(func(v *ateapipb.ActorTemplateVersion) { v.Metadata.Name = "V1" }),
		field.ErrorList{field.Invalid(field.NewPath("actor_template_version", "metadata", "name"), "V1", "")},
	}, {
		"missing actor_template parent ref",
		validReq(func(v *ateapipb.ActorTemplateVersion) { v.ActorTemplate = nil }),
		field.ErrorList{field.Required(field.NewPath("actor_template_version", "actor_template"), "")},
	}, {
		"parent ref missing atespace",
		validReq(func(v *ateapipb.ActorTemplateVersion) { v.ActorTemplate.Atespace = "" }),
		field.ErrorList{field.Required(field.NewPath("actor_template_version", "actor_template", "atespace"), "")},
	}, {
		"parent ref in a different atespace",
		validReq(func(v *ateapipb.ActorTemplateVersion) { v.ActorTemplate.Atespace = "ns2" }),
		field.ErrorList{field.Invalid(field.NewPath("actor_template_version", "actor_template", "atespace"), "ns2", "")},
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertValidateErr(t, validateCreateActorTemplateVersionRequest(tt.req), tt.want)
		})
	}
}

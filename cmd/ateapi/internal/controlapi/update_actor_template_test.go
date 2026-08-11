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
	"google.golang.org/protobuf/types/known/fieldmaskpb"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

func TestValidateUpdateActorTemplateRequest(t *testing.T) {
	supported := []string{"default_version_on_create"}
	validReq := func(mutations ...func(*ateapipb.UpdateActorTemplateRequest)) *ateapipb.UpdateActorTemplateRequest {
		req := &ateapipb.UpdateActorTemplateRequest{
			ActorTemplate: &ateapipb.ActorTemplate{
				Metadata:               &ateapipb.ResourceMetadata{Atespace: "ns1", Name: "tmpl-a"},
				DefaultVersionOnCreate: &ateapipb.ObjectRef{Atespace: "ns1", Name: "tmpl-a-v1"},
			},
			UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"default_version_on_create"}},
		}
		for _, m := range mutations {
			m(req)
		}
		return req
	}

	tests := []struct {
		name string
		req  *ateapipb.UpdateActorTemplateRequest
		want field.ErrorList
	}{{
		"valid default update",
		validReq(),
		nil,
	}, {
		"valid default clear",
		validReq(func(r *ateapipb.UpdateActorTemplateRequest) {
			r.ActorTemplate.DefaultVersionOnCreate = nil
		}),
		nil,
	}, {
		"missing actor_template",
		&ateapipb.UpdateActorTemplateRequest{UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"default_version_on_create"}}},
		field.ErrorList{field.Required(field.NewPath("actor_template"), "")},
	}, {
		"missing metadata.atespace",
		validReq(func(r *ateapipb.UpdateActorTemplateRequest) { r.ActorTemplate.Metadata.Atespace = "" }),
		field.ErrorList{field.Required(field.NewPath("actor_template", "metadata", "atespace"), "")},
	}, {
		"invalid metadata.atespace",
		validReq(func(r *ateapipb.UpdateActorTemplateRequest) { r.ActorTemplate.Metadata.Atespace = "NS_1" }),
		field.ErrorList{field.Invalid(field.NewPath("actor_template", "metadata", "atespace"), "NS_1", "")},
	}, {
		"missing metadata.name",
		validReq(func(r *ateapipb.UpdateActorTemplateRequest) { r.ActorTemplate.Metadata.Name = "" }),
		field.ErrorList{field.Required(field.NewPath("actor_template", "metadata", "name"), "")},
	}, {
		"invalid uid precondition",
		validReq(func(r *ateapipb.UpdateActorTemplateRequest) { r.ActorTemplate.Metadata.Uid = "not-a-uuid" }),
		field.ErrorList{field.Invalid(field.NewPath("actor_template", "metadata", "uid"), "not-a-uuid", "")},
	}, {
		"negative version precondition",
		validReq(func(r *ateapipb.UpdateActorTemplateRequest) { r.ActorTemplate.Metadata.Version = -1 }),
		field.ErrorList{field.Invalid(field.NewPath("actor_template", "metadata", "version"), int64(-1), "")},
	}, {
		"missing update mask",
		validReq(func(r *ateapipb.UpdateActorTemplateRequest) { r.UpdateMask = nil }),
		field.ErrorList{field.Required(field.NewPath("update_mask"), "")},
	}, {
		"unsupported mask path",
		validReq(func(r *ateapipb.UpdateActorTemplateRequest) {
			r.UpdateMask = &fieldmaskpb.FieldMask{Paths: []string{"metadata"}}
		}),
		field.ErrorList{field.NotSupported(field.NewPath("update_mask"), "metadata", supported)},
	}, {
		"worker_selector no longer a mask path",
		validReq(func(r *ateapipb.UpdateActorTemplateRequest) {
			r.UpdateMask = &fieldmaskpb.FieldMask{Paths: []string{"worker_selector"}}
		}),
		field.ErrorList{field.NotSupported(field.NewPath("update_mask"), "worker_selector", supported)},
	}, {
		"default ref missing atespace",
		validReq(func(r *ateapipb.UpdateActorTemplateRequest) {
			r.ActorTemplate.DefaultVersionOnCreate = &ateapipb.ObjectRef{Name: "tmpl-a-v1"}
		}),
		field.ErrorList{field.Required(field.NewPath("actor_template", "default_version_on_create", "atespace"), "")},
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertValidateErr(t, validateUpdateActorTemplateRequest(tt.req), tt.want)
		})
	}
}

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
	supported := []string{"spec.default_version_on_create", "spec.worker_selector"}
	validReq := func(mutations ...func(*ateapipb.UpdateActorTemplateRequest)) *ateapipb.UpdateActorTemplateRequest {
		req := &ateapipb.UpdateActorTemplateRequest{
			ActorTemplate: &ateapipb.ActorTemplate{
				Metadata: &ateapipb.ResourceMetadata{Name: "tmpl-a"},
				Spec: &ateapipb.ActorTemplateSpec{
					WorkerSelector: &ateapipb.Selector{MatchLabels: map[string]string{"tier": "1"}},
				},
			},
			UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"spec.worker_selector"}},
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
		"valid selector update",
		validReq(),
		nil,
	}, {
		"valid default update",
		validReq(func(r *ateapipb.UpdateActorTemplateRequest) {
			r.ActorTemplate.Spec = &ateapipb.ActorTemplateSpec{
				DefaultVersionOnCreate: &ateapipb.ObjectRef{Name: "tmpl-a-v1"},
			}
			r.UpdateMask = &fieldmaskpb.FieldMask{Paths: []string{"spec.default_version_on_create"}}
		}),
		nil,
	}, {
		"valid default clear",
		validReq(func(r *ateapipb.UpdateActorTemplateRequest) {
			r.ActorTemplate.Spec = nil
			r.UpdateMask = &fieldmaskpb.FieldMask{Paths: []string{"spec.default_version_on_create"}}
		}),
		nil,
	}, {
		"missing actor_template",
		&ateapipb.UpdateActorTemplateRequest{UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"spec.worker_selector"}}},
		field.ErrorList{field.Required(field.NewPath("actor_template"), "")},
	}, {
		"metadata.atespace must be empty",
		validReq(func(r *ateapipb.UpdateActorTemplateRequest) { r.ActorTemplate.Metadata.Atespace = "ns1" }),
		field.ErrorList{field.Invalid(field.NewPath("actor_template", "metadata", "atespace"), "ns1", "")},
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
			r.UpdateMask = &fieldmaskpb.FieldMask{Paths: []string{"spec"}}
		}),
		field.ErrorList{field.NotSupported(field.NewPath("update_mask"), "spec", supported)},
	}, {
		"invalid worker selector",
		validReq(func(r *ateapipb.UpdateActorTemplateRequest) {
			r.ActorTemplate.Spec.WorkerSelector = &ateapipb.Selector{MatchLabels: map[string]string{"bad key!": "1"}}
		}),
		field.ErrorList{field.Invalid(field.NewPath("actor_template", "spec", "worker_selector", "match_labels").Key("bad key!"), "bad key!", "")},
	}, {
		"default ref with atespace",
		validReq(func(r *ateapipb.UpdateActorTemplateRequest) {
			r.ActorTemplate.Spec.DefaultVersionOnCreate = &ateapipb.ObjectRef{Atespace: "ns1", Name: "tmpl-a-v1"}
		}),
		field.ErrorList{field.Invalid(field.NewPath("actor_template", "spec", "default_version_on_create", "atespace"), "ns1", "")},
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertValidateErr(t, validateUpdateActorTemplateRequest(tt.req), tt.want)
		})
	}
}

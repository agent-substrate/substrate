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

package authz

import (
	"slices"
	"strings"
	"testing"

	"github.com/agent-substrate/substrate/internal/apierror"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc/codes"
)

func TestDefaultRPCPermissions(t *testing.T) {
	actorRef := &ateapipb.ObjectRef{Atespace: "team-a", Name: "runner"}
	actorMeta := &ateapipb.ResourceMetadata{Atespace: "team-a", Name: "runner"}
	localTemplate := &ateapipb.ObjectRef{Atespace: "team-a", Name: "tmpl"}
	sharedTemplate := &ateapipb.ObjectRef{Atespace: "shared", Name: "tmpl"}

	tests := []struct {
		name       string
		fullMethod string
		req        any
		want       []check
	}{
		// Atespaces.
		{
			name:       "CreateAtespace",
			fullMethod: ateapipb.Control_CreateAtespace_FullMethodName,
			req:        &ateapipb.CreateAtespaceRequest{},
			want:       []check{{RelationCanCreateAtespace, GlobalRootObject}},
		},
		{
			name:       "ListAtespaces",
			fullMethod: ateapipb.Control_ListAtespaces_FullMethodName,
			req:        &ateapipb.ListAtespacesRequest{},
			want:       []check{{RelationCanListAtespaces, GlobalRootObject}},
		},
		{
			name:       "GetAtespace",
			fullMethod: ateapipb.Control_GetAtespace_FullMethodName,
			req:        &ateapipb.GetAtespaceRequest{Atespace: &ateapipb.ObjectRef{Name: "team-a"}},
			want:       []check{{RelationCanGet, "atespace:team-a"}},
		},
		{
			name:       "DeleteAtespace",
			fullMethod: ateapipb.Control_DeleteAtespace_FullMethodName,
			req:        &ateapipb.DeleteAtespaceRequest{Atespace: &ateapipb.ObjectRef{Name: "team-a"}},
			want:       []check{{RelationCanDelete, "atespace:team-a"}},
		},

		// Actor templates.
		{
			name:       "CreateActorTemplate",
			fullMethod: ateapipb.Control_CreateActorTemplate_FullMethodName,
			req: &ateapipb.CreateActorTemplateRequest{ActorTemplate: &ateapipb.ActorTemplate{
				Metadata: &ateapipb.ResourceMetadata{Atespace: "team-a", Name: "tmpl"},
			}},
			want: []check{{RelationCanCreateActorTemplate, "atespace:team-a"}},
		},
		{
			name:       "GetActorTemplate",
			fullMethod: ateapipb.Control_GetActorTemplate_FullMethodName,
			req:        &ateapipb.GetActorTemplateRequest{ActorTemplate: localTemplate},
			want:       []check{{RelationCanGet, "actor_template:team-a/tmpl"}},
		},
		{
			name:       "ListActorTemplates in an atespace",
			fullMethod: ateapipb.Control_ListActorTemplates_FullMethodName,
			req:        &ateapipb.ListActorTemplatesRequest{Atespace: "team-a"},
			want:       []check{{RelationCanListActorTemplates, "atespace:team-a"}},
		},
		{
			name:       "ListActorTemplates across all atespaces",
			fullMethod: ateapipb.Control_ListActorTemplates_FullMethodName,
			req:        &ateapipb.ListActorTemplatesRequest{},
			want:       []check{{RelationCanListActorTemplates, GlobalRootObject}},
		},
		{
			name:       "DeleteActorTemplate",
			fullMethod: ateapipb.Control_DeleteActorTemplate_FullMethodName,
			req:        &ateapipb.DeleteActorTemplateRequest{ActorTemplate: localTemplate},
			want:       []check{{RelationCanDelete, "actor_template:team-a/tmpl"}},
		},

		// Actors.
		{
			name:       "CreateActor checks the atespace and the template",
			fullMethod: ateapipb.Control_CreateActor_FullMethodName,
			req:        &ateapipb.CreateActorRequest{Actor: &ateapipb.Actor{Metadata: actorMeta, ActorTemplate: sharedTemplate}},
			want: []check{
				{RelationCanCreateActor, "atespace:team-a"},
				{RelationCanUseTemplate, "actor_template:shared/tmpl"},
			},
		},
		{
			name:       "GetActor",
			fullMethod: ateapipb.Control_GetActor_FullMethodName,
			req:        &ateapipb.GetActorRequest{Actor: actorRef},
			want:       []check{{RelationCanGet, "actor:team-a/runner"}},
		},
		{
			name:       "ListActors in an atespace",
			fullMethod: ateapipb.Control_ListActors_FullMethodName,
			req:        &ateapipb.ListActorsRequest{Atespace: "team-a"},
			want:       []check{{RelationCanListActors, "atespace:team-a"}},
		},
		{
			name:       "ListActors across all atespaces",
			fullMethod: ateapipb.Control_ListActors_FullMethodName,
			req:        &ateapipb.ListActorsRequest{},
			want:       []check{{RelationCanListActors, GlobalRootObject}},
		},
		{
			name:       "UpdateActor checks the actor and the template",
			fullMethod: ateapipb.Control_UpdateActor_FullMethodName,
			req:        &ateapipb.UpdateActorRequest{Actor: &ateapipb.Actor{Metadata: actorMeta, ActorTemplate: sharedTemplate}},
			want: []check{
				{RelationCanUpdate, "actor:team-a/runner"},
				{RelationCanUseTemplate, "actor_template:shared/tmpl"},
			},
		},
		{
			name:       "DeleteActor",
			fullMethod: ateapipb.Control_DeleteActor_FullMethodName,
			req:        &ateapipb.DeleteActorRequest{Actor: actorRef},
			want:       []check{{RelationCanDelete, "actor:team-a/runner"}},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r, ok := defaultRPCPermissions[tc.fullMethod]
			if !ok {
				t.Fatalf("%s is not registered", tc.fullMethod)
			}
			got, err := r.extract(tc.req)
			if err != nil {
				t.Fatalf("extract() error = %v", err)
			}
			if !slices.Equal(got, tc.want) {
				t.Errorf("extract() = %v, want %v", got, tc.want)
			}
		})
	}
}

// A request missing an identifier a check needs, or naming a resource with an
// invalid name, is rejected rather than having that check skipped.
func TestDefaultRPCPermissions_MalformedIdentifierFailsClosed(t *testing.T) {
	actorMeta := &ateapipb.ResourceMetadata{Atespace: "team-a", Name: "runner"}
	template := &ateapipb.ObjectRef{Atespace: "shared", Name: "tmpl"}

	tests := []struct {
		name       string
		fullMethod string
		req        any
		wantField  string
	}{
		{
			name:       "GetAtespace without a name",
			fullMethod: ateapipb.Control_GetAtespace_FullMethodName,
			req:        &ateapipb.GetAtespaceRequest{},
			wantField:  "atespace.name",
		},
		{
			name:       "DeleteAtespace with an invalid name",
			fullMethod: ateapipb.Control_DeleteAtespace_FullMethodName,
			req:        &ateapipb.DeleteAtespaceRequest{Atespace: &ateapipb.ObjectRef{Name: "Team A"}},
			wantField:  "atespace.name",
		},
		{
			name:       "GetAtespaceAccessPolicy without an atespace",
			fullMethod: ateapipb.Control_GetAtespaceAccessPolicy_FullMethodName,
			req:        &ateapipb.GetAtespaceAccessPolicyRequest{},
			wantField:  "atespace.name",
		},
		{
			name:       "UpdateAtespaceAccessPolicy with an invalid atespace",
			fullMethod: ateapipb.Control_UpdateAtespaceAccessPolicy_FullMethodName,
			req:        &ateapipb.UpdateAtespaceAccessPolicyRequest{Atespace: &ateapipb.ObjectRef{Name: "Team-A"}},
			wantField:  "atespace.name",
		},
		{
			name:       "CreateActorTemplate without an atespace",
			fullMethod: ateapipb.Control_CreateActorTemplate_FullMethodName,
			req:        &ateapipb.CreateActorTemplateRequest{},
			wantField:  "actor_template.metadata.atespace",
		},
		{
			name:       "GetActorTemplate with a name containing a tab",
			fullMethod: ateapipb.Control_GetActorTemplate_FullMethodName,
			req:        &ateapipb.GetActorTemplateRequest{ActorTemplate: &ateapipb.ObjectRef{Atespace: "team-a", Name: "a\tb"}},
			wantField:  "actor_template.name",
		},
		{
			name:       "ListActorTemplates with an invalid atespace",
			fullMethod: ateapipb.Control_ListActorTemplates_FullMethodName,
			req:        &ateapipb.ListActorTemplatesRequest{Atespace: "team/a"},
			wantField:  "atespace",
		},
		{
			name:       "DeleteActorTemplate without an atespace",
			fullMethod: ateapipb.Control_DeleteActorTemplate_FullMethodName,
			req:        &ateapipb.DeleteActorTemplateRequest{ActorTemplate: &ateapipb.ObjectRef{Name: "tmpl"}},
			wantField:  "actor_template.atespace",
		},
		{
			name:       "CreateActor without an actor",
			fullMethod: ateapipb.Control_CreateActor_FullMethodName,
			req:        &ateapipb.CreateActorRequest{},
			wantField:  "actor.metadata.atespace",
		},
		{
			name:       "CreateActor without a template",
			fullMethod: ateapipb.Control_CreateActor_FullMethodName,
			req:        &ateapipb.CreateActorRequest{Actor: &ateapipb.Actor{Metadata: actorMeta}},
			wantField:  "actor.actor_template.atespace",
		},
		{
			name:       "CreateActor with a template missing its atespace",
			fullMethod: ateapipb.Control_CreateActor_FullMethodName,
			req: &ateapipb.CreateActorRequest{Actor: &ateapipb.Actor{
				Metadata:      actorMeta,
				ActorTemplate: &ateapipb.ObjectRef{Name: "tmpl"},
			}},
			wantField: "actor.actor_template.atespace",
		},
		{
			name:       "CreateActor with an invalid template atespace",
			fullMethod: ateapipb.Control_CreateActor_FullMethodName,
			req: &ateapipb.CreateActorRequest{Actor: &ateapipb.Actor{
				Metadata:      actorMeta,
				ActorTemplate: &ateapipb.ObjectRef{Atespace: "shared/x", Name: "tmpl"},
			}},
			wantField: "actor.actor_template.atespace",
		},
		{
			name:       "GetActor without a name",
			fullMethod: ateapipb.Control_GetActor_FullMethodName,
			req:        &ateapipb.GetActorRequest{Actor: &ateapipb.ObjectRef{Atespace: "team-a"}},
			wantField:  "actor.name",
		},
		{
			name:       "ListActors with an over-long atespace",
			fullMethod: ateapipb.Control_ListActors_FullMethodName,
			req:        &ateapipb.ListActorsRequest{Atespace: strings.Repeat("a", 64)},
			wantField:  "atespace",
		},
		{
			name:       "UpdateActor without metadata",
			fullMethod: ateapipb.Control_UpdateActor_FullMethodName,
			req:        &ateapipb.UpdateActorRequest{Actor: &ateapipb.Actor{ActorTemplate: template}},
			wantField:  "actor.metadata.atespace",
		},
		{
			name:       "UpdateActor with metadata missing its atespace",
			fullMethod: ateapipb.Control_UpdateActor_FullMethodName,
			req: &ateapipb.UpdateActorRequest{Actor: &ateapipb.Actor{
				Metadata:      &ateapipb.ResourceMetadata{Name: "runner"},
				ActorTemplate: template,
			}},
			wantField: "actor.metadata.atespace",
		},
		{
			name:       "UpdateActor without a template",
			fullMethod: ateapipb.Control_UpdateActor_FullMethodName,
			req:        &ateapipb.UpdateActorRequest{Actor: &ateapipb.Actor{Metadata: actorMeta}},
			wantField:  "actor.actor_template.name",
		},
		{
			name:       "UpdateActor with an invalid template atespace",
			fullMethod: ateapipb.Control_UpdateActor_FullMethodName,
			req: &ateapipb.UpdateActorRequest{Actor: &ateapipb.Actor{
				Metadata:      actorMeta,
				ActorTemplate: &ateapipb.ObjectRef{Atespace: "Shared", Name: "tmpl"},
			}},
			wantField: "actor.actor_template.atespace",
		},
		{
			name:       "DeleteActor with an invalid atespace",
			fullMethod: ateapipb.Control_DeleteActor_FullMethodName,
			req:        &ateapipb.DeleteActorRequest{Actor: &ateapipb.ObjectRef{Atespace: "team:a", Name: "runner"}},
			wantField:  "actor.atespace",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := defaultRPCPermissions[tc.fullMethod].extract(tc.req)
			if apierror.Code(err) != codes.InvalidArgument {
				t.Fatalf("extract() = %v, %v; want code InvalidArgument", got, err)
			}
			if got != nil {
				t.Errorf("extract() checks = %v, want none alongside the error", got)
			}
			if msg := err.Error(); !strings.Contains(msg, tc.wantField) {
				t.Errorf("extract() error %q does not name field %q", msg, tc.wantField)
			}
		})
	}
}

func TestDefaultRPCPermissions_UnexpectedRequestTypeFailsClosed(t *testing.T) {
	for fullMethod, r := range defaultRPCPermissions {
		if _, err := r.extract("wrong-type"); apierror.Code(err) != codes.Internal {
			t.Errorf("%s: extract(wrong type) error = %v, want code Internal", fullMethod, err)
		}
	}
}

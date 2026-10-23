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

	"go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/internal/apierror"
	"google.golang.org/grpc/codes"
)

func TestDefaultRPCPermissions(t *testing.T) {
	actorRef := &ateapi.ObjectRef{Atespace: "team-a", Name: "runner"}
	actorMeta := &ateapi.ResourceMetadata{Atespace: "team-a", Name: "runner"}
	localTemplate := &ateapi.ObjectRef{Atespace: "team-a", Name: "tmpl"}
	sharedTemplate := &ateapi.ObjectRef{Atespace: "shared", Name: "tmpl"}
	workerRef := &ateapi.ObjectRef{Name: "w-1"}
	tagRef := &ateapi.ObjectRef{Atespace: "team-a", Name: "v1"}

	tests := []struct {
		name       string
		fullMethod string
		req        any
		want       []check
	}{
		// Atespaces.
		{
			name:       "CreateAtespace",
			fullMethod: ateapi.Control_CreateAtespace_FullMethodName,
			req:        &ateapi.CreateAtespaceRequest{},
			want:       []check{{RelationCanCreateAtespace, GlobalRootObject}},
		},
		{
			name:       "ListAtespaces",
			fullMethod: ateapi.Control_ListAtespaces_FullMethodName,
			req:        &ateapi.ListAtespacesRequest{},
			want:       []check{{RelationCanListAtespaces, GlobalRootObject}},
		},
		{
			name:       "GetAtespace",
			fullMethod: ateapi.Control_GetAtespace_FullMethodName,
			req:        &ateapi.GetAtespaceRequest{Atespace: &ateapi.ObjectRef{Name: "team-a"}},
			want:       []check{{RelationCanGet, "atespace:team-a"}},
		},
		{
			name:       "DeleteAtespace",
			fullMethod: ateapi.Control_DeleteAtespace_FullMethodName,
			req:        &ateapi.DeleteAtespaceRequest{Atespace: &ateapi.ObjectRef{Name: "team-a"}},
			want:       []check{{RelationCanDelete, "atespace:team-a"}},
		},

		// Actor templates.
		{
			name:       "CreateActorTemplate",
			fullMethod: ateapi.Control_CreateActorTemplate_FullMethodName,
			req: &ateapi.CreateActorTemplateRequest{ActorTemplate: &ateapi.ActorTemplate{
				Metadata: &ateapi.ResourceMetadata{Atespace: "team-a", Name: "tmpl"},
			}},
			want: []check{{RelationCanCreateActorTemplate, "atespace:team-a"}},
		},
		{
			name:       "GetActorTemplate",
			fullMethod: ateapi.Control_GetActorTemplate_FullMethodName,
			req:        &ateapi.GetActorTemplateRequest{ActorTemplate: localTemplate},
			want:       []check{{RelationCanGet, "actor_template:team-a/tmpl"}},
		},
		{
			name:       "ListActorTemplates in an atespace",
			fullMethod: ateapi.Control_ListActorTemplates_FullMethodName,
			req:        &ateapi.ListActorTemplatesRequest{Atespace: "team-a"},
			want:       []check{{RelationCanListActorTemplates, "atespace:team-a"}},
		},
		{
			name:       "ListActorTemplates across all atespaces",
			fullMethod: ateapi.Control_ListActorTemplates_FullMethodName,
			req:        &ateapi.ListActorTemplatesRequest{},
			want:       []check{{RelationCanListActorTemplates, GlobalRootObject}},
		},
		{
			name:       "DeleteActorTemplate",
			fullMethod: ateapi.Control_DeleteActorTemplate_FullMethodName,
			req:        &ateapi.DeleteActorTemplateRequest{ActorTemplate: localTemplate},
			want:       []check{{RelationCanDelete, "actor_template:team-a/tmpl"}},
		},

		// Actors.
		{
			name:       "CreateActor checks the atespace and the template",
			fullMethod: ateapi.Control_CreateActor_FullMethodName,
			req:        &ateapi.CreateActorRequest{Actor: &ateapi.Actor{Metadata: actorMeta, ActorTemplate: sharedTemplate}},
			want: []check{
				{RelationCanCreateActor, "atespace:team-a"},
				{RelationCanUseTemplate, "actor_template:shared/tmpl"},
			},
		},
		{
			name:       "CreateActor from a tag also checks the tag",
			fullMethod: ateapi.Control_CreateActor_FullMethodName,
			req: &ateapi.CreateActorRequest{Actor: &ateapi.Actor{
				Metadata:      actorMeta,
				ActorTemplate: sharedTemplate,
				SourceTag:     &ateapi.ObjectRef{Atespace: "team-b", Name: "v1"},
			}},
			want: []check{
				{RelationCanCreateActor, "atespace:team-a"},
				{RelationCanUseTemplate, "actor_template:shared/tmpl"},
				{RelationCanUseTag, "tag:team-b/v1"},
			},
		},
		{
			name:       "CreateActor from a template tag checks the tag",
			fullMethod: ateapi.Control_CreateActor_FullMethodName,
			req: &ateapi.CreateActorRequest{Actor: &ateapi.Actor{
				Metadata:      actorMeta,
				ActorTemplate: sharedTemplate,
				SourceTag:     &ateapi.ObjectRef{Atespace: "shared", Name: "0b5c7d4e-golden"},
			}},
			want: []check{
				{RelationCanCreateActor, "atespace:team-a"},
				{RelationCanUseTemplate, "actor_template:shared/tmpl"},
				{RelationCanUseTag, "tag:shared/0b5c7d4e-golden"},
			},
		},
		{
			name:       "GetActor",
			fullMethod: ateapi.Control_GetActor_FullMethodName,
			req:        &ateapi.GetActorRequest{Actor: actorRef},
			want:       []check{{RelationCanGet, "actor:team-a/runner"}},
		},
		{
			name:       "ListActors in an atespace",
			fullMethod: ateapi.Control_ListActors_FullMethodName,
			req:        &ateapi.ListActorsRequest{Atespace: "team-a"},
			want:       []check{{RelationCanListActors, "atespace:team-a"}},
		},
		{
			name:       "ListActors across all atespaces",
			fullMethod: ateapi.Control_ListActors_FullMethodName,
			req:        &ateapi.ListActorsRequest{},
			want:       []check{{RelationCanListActors, GlobalRootObject}},
		},
		{
			name:       "UpdateActor checks the actor and the template",
			fullMethod: ateapi.Control_UpdateActor_FullMethodName,
			req:        &ateapi.UpdateActorRequest{Actor: &ateapi.Actor{Metadata: actorMeta, ActorTemplate: sharedTemplate}},
			want: []check{
				{RelationCanUpdate, "actor:team-a/runner"},
				{RelationCanUseTemplate, "actor_template:shared/tmpl"},
			},
		},
		{
			name:       "DeleteActor",
			fullMethod: ateapi.Control_DeleteActor_FullMethodName,
			req:        &ateapi.DeleteActorRequest{Actor: actorRef},
			want:       []check{{RelationCanDelete, "actor:team-a/runner"}},
		},

		// Actor lifecycle.
		{
			name:       "ResumeActor",
			fullMethod: ateapi.Control_ResumeActor_FullMethodName,
			req:        &ateapi.ResumeActorRequest{Actor: actorRef},
			want:       []check{{RelationCanResume, "actor:team-a/runner"}},
		},
		{
			name:       "SuspendActor",
			fullMethod: ateapi.Control_SuspendActor_FullMethodName,
			req:        &ateapi.SuspendActorRequest{Actor: actorRef},
			want:       []check{{RelationCanSuspend, "actor:team-a/runner"}},
		},
		{
			name:       "RevertActor",
			fullMethod: ateapi.Control_RevertActor_FullMethodName,
			req:        &ateapi.RevertActorRequest{Actor: actorRef},
			want:       []check{{RelationCanRevert, "actor:team-a/runner"}},
		},

		// Actor egress policies.
		{
			name:       "GetActorEgressPolicy",
			fullMethod: ateapi.Control_GetActorEgressPolicy_FullMethodName,
			req:        &ateapi.GetActorEgressPolicyRequest{Actor: actorRef},
			want:       []check{{RelationCanGetEgressPolicy, "actor:team-a/runner"}},
		},
		{
			name:       "CreateActorEgressPolicy",
			fullMethod: ateapi.Control_CreateActorEgressPolicy_FullMethodName,
			req:        &ateapi.CreateActorEgressPolicyRequest{Actor: actorRef},
			want:       []check{{RelationCanCreateEgressPolicy, "actor:team-a/runner"}},
		},
		{
			name:       "UpdateActorEgressPolicy",
			fullMethod: ateapi.Control_UpdateActorEgressPolicy_FullMethodName,
			req:        &ateapi.UpdateActorEgressPolicyRequest{Actor: actorRef},
			want:       []check{{RelationCanUpdateEgressPolicy, "actor:team-a/runner"}},
		},
		{
			name:       "DeleteActorEgressPolicy",
			fullMethod: ateapi.Control_DeleteActorEgressPolicy_FullMethodName,
			req:        &ateapi.DeleteActorEgressPolicyRequest{Actor: actorRef},
			want:       []check{{RelationCanDeleteEgressPolicy, "actor:team-a/runner"}},
		},

		// Workers.
		{
			name:       "CreateWorker",
			fullMethod: ateapi.Control_CreateWorker_FullMethodName,
			req:        &ateapi.CreateWorkerRequest{},
			want:       []check{{RelationCanCreateWorker, GlobalRootObject}},
		},
		{
			name:       "ListWorkers",
			fullMethod: ateapi.Control_ListWorkers_FullMethodName,
			req:        &ateapi.ListWorkersRequest{},
			want:       []check{{RelationCanListWorkers, GlobalRootObject}},
		},
		{
			name:       "GetWorker",
			fullMethod: ateapi.Control_GetWorker_FullMethodName,
			req:        &ateapi.GetWorkerRequest{Worker: workerRef},
			want:       []check{{RelationCanGet, "worker:w-1"}},
		},
		{
			name:       "UpdateWorker",
			fullMethod: ateapi.Control_UpdateWorker_FullMethodName,
			req: &ateapi.UpdateWorkerRequest{Worker: &ateapi.Worker{
				Metadata: &ateapi.ResourceMetadata{Name: "w-1"},
			}},
			want: []check{{RelationCanUpdate, "worker:w-1"}},
		},
		{
			name:       "DeleteWorker",
			fullMethod: ateapi.Control_DeleteWorker_FullMethodName,
			req:        &ateapi.DeleteWorkerRequest{Worker: workerRef},
			want:       []check{{RelationCanDelete, "worker:w-1"}},
		},
		{
			name:       "DrainWorker",
			fullMethod: ateapi.Control_DrainWorker_FullMethodName,
			req:        &ateapi.DrainWorkerRequest{Worker: workerRef},
			want:       []check{{RelationCanDrain, "worker:w-1"}},
		},
		{
			name:       "MarkWorkerAvailable",
			fullMethod: ateapi.Control_MarkWorkerAvailable_FullMethodName,
			req:        &ateapi.MarkWorkerAvailableRequest{Worker: workerRef},
			want:       []check{{RelationCanUpdate, "worker:w-1"}},
		},
		{
			name:       "MarkWorkerUnavailable",
			fullMethod: ateapi.Control_MarkWorkerUnavailable_FullMethodName,
			req:        &ateapi.MarkWorkerUnavailableRequest{Worker: workerRef},
			want:       []check{{RelationCanUpdate, "worker:w-1"}},
		},
		{
			name:       "ListWorkerActorAssignments",
			fullMethod: ateapi.Control_ListWorkerActorAssignments_FullMethodName,
			req:        &ateapi.ListWorkerActorAssignmentsRequest{Worker: workerRef},
			want:       []check{{RelationCanListActorAssignments, "worker:w-1"}},
		},

		// Actor identity credentials.
		{
			name:       "MintActorJWT",
			fullMethod: ateapi.Control_MintActorJwt_FullMethodName,
			req:        &ateapi.MintActorJwtRequest{Actor: actorRef},
			want:       []check{{RelationCanMintActorJWT, "actor:team-a/runner"}},
		},
		{
			name:       "MintActorCertificate",
			fullMethod: ateapi.Control_MintActorCertificate_FullMethodName,
			req:        &ateapi.MintActorCertificateRequest{Actor: actorRef},
			want:       []check{{RelationCanMintActorCertificate, "actor:team-a/runner"}},
		},

		// Tags.
		{
			name:       "CreateTag checks its source actor's atespace",
			fullMethod: ateapi.Control_CreateTag_FullMethodName,
			req: &ateapi.CreateTagRequest{Tag: &ateapi.Tag{
				Metadata:    &ateapi.ResourceMetadata{Atespace: "other", Name: "v1"},
				SourceActor: actorRef,
			}},
			want: []check{{RelationCanCreateTag, "atespace:team-a"}},
		},
		{
			name:       "GetTag",
			fullMethod: ateapi.Control_GetTag_FullMethodName,
			req:        &ateapi.GetTagRequest{Tag: tagRef},
			want:       []check{{RelationCanGet, "tag:team-a/v1"}},
		},
		{
			name:       "ListTags in an atespace",
			fullMethod: ateapi.Control_ListTags_FullMethodName,
			req:        &ateapi.ListTagsRequest{Atespace: "team-a"},
			want:       []check{{RelationCanListTags, "atespace:team-a"}},
		},
		{
			name:       "ListTags across all atespaces",
			fullMethod: ateapi.Control_ListTags_FullMethodName,
			req:        &ateapi.ListTagsRequest{},
			want:       []check{{RelationCanListTags, GlobalRootObject}},
		},
		{
			name:       "UpdateTag",
			fullMethod: ateapi.Control_UpdateTag_FullMethodName,
			req: &ateapi.UpdateTagRequest{Tag: &ateapi.Tag{
				Metadata: &ateapi.ResourceMetadata{Atespace: "team-a", Name: "v1"},
			}},
			want: []check{{RelationCanUpdate, "tag:team-a/v1"}},
		},
		{
			name:       "DeleteTag",
			fullMethod: ateapi.Control_DeleteTag_FullMethodName,
			req:        &ateapi.DeleteTagRequest{Tag: tagRef},
			want:       []check{{RelationCanDelete, "tag:team-a/v1"}},
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
	actorMeta := &ateapi.ResourceMetadata{Atespace: "team-a", Name: "runner"}
	template := &ateapi.ObjectRef{Atespace: "shared", Name: "tmpl"}

	tests := []struct {
		name       string
		fullMethod string
		req        any
		wantField  string
	}{
		{
			name:       "GetAtespace without a name",
			fullMethod: ateapi.Control_GetAtespace_FullMethodName,
			req:        &ateapi.GetAtespaceRequest{},
			wantField:  "atespace.name",
		},
		{
			name:       "DeleteAtespace with an invalid name",
			fullMethod: ateapi.Control_DeleteAtespace_FullMethodName,
			req:        &ateapi.DeleteAtespaceRequest{Atespace: &ateapi.ObjectRef{Name: "Team A"}},
			wantField:  "atespace.name",
		},
		{
			name:       "GetAtespaceAccessPolicy without an atespace",
			fullMethod: ateapi.Control_GetAtespaceAccessPolicy_FullMethodName,
			req:        &ateapi.GetAtespaceAccessPolicyRequest{},
			wantField:  "atespace.name",
		},
		{
			name:       "UpdateAtespaceAccessPolicy with an invalid atespace",
			fullMethod: ateapi.Control_UpdateAtespaceAccessPolicy_FullMethodName,
			req:        &ateapi.UpdateAtespaceAccessPolicyRequest{Atespace: &ateapi.ObjectRef{Name: "Team-A"}},
			wantField:  "atespace.name",
		},
		{
			name:       "CreateActorTemplate without an atespace",
			fullMethod: ateapi.Control_CreateActorTemplate_FullMethodName,
			req:        &ateapi.CreateActorTemplateRequest{},
			wantField:  "actor_template.metadata.atespace",
		},
		{
			name:       "GetActorTemplate with a name containing a tab",
			fullMethod: ateapi.Control_GetActorTemplate_FullMethodName,
			req:        &ateapi.GetActorTemplateRequest{ActorTemplate: &ateapi.ObjectRef{Atespace: "team-a", Name: "a\tb"}},
			wantField:  "actor_template.name",
		},
		{
			name:       "ListActorTemplates with an invalid atespace",
			fullMethod: ateapi.Control_ListActorTemplates_FullMethodName,
			req:        &ateapi.ListActorTemplatesRequest{Atespace: "team/a"},
			wantField:  "atespace",
		},
		{
			name:       "DeleteActorTemplate without an atespace",
			fullMethod: ateapi.Control_DeleteActorTemplate_FullMethodName,
			req:        &ateapi.DeleteActorTemplateRequest{ActorTemplate: &ateapi.ObjectRef{Name: "tmpl"}},
			wantField:  "actor_template.atespace",
		},
		{
			name:       "CreateActor without an actor",
			fullMethod: ateapi.Control_CreateActor_FullMethodName,
			req:        &ateapi.CreateActorRequest{},
			wantField:  "actor.metadata.atespace",
		},
		{
			name:       "CreateActor without a template",
			fullMethod: ateapi.Control_CreateActor_FullMethodName,
			req:        &ateapi.CreateActorRequest{Actor: &ateapi.Actor{Metadata: actorMeta}},
			wantField:  "actor.actor_template.atespace",
		},
		{
			name:       "CreateActor with a template missing its atespace",
			fullMethod: ateapi.Control_CreateActor_FullMethodName,
			req: &ateapi.CreateActorRequest{Actor: &ateapi.Actor{
				Metadata:      actorMeta,
				ActorTemplate: &ateapi.ObjectRef{Name: "tmpl"},
			}},
			wantField: "actor.actor_template.atespace",
		},
		{
			name:       "CreateActor with an invalid template atespace",
			fullMethod: ateapi.Control_CreateActor_FullMethodName,
			req: &ateapi.CreateActorRequest{Actor: &ateapi.Actor{
				Metadata:      actorMeta,
				ActorTemplate: &ateapi.ObjectRef{Atespace: "shared/x", Name: "tmpl"},
			}},
			wantField: "actor.actor_template.atespace",
		},
		{
			name:       "CreateActor with a source tag missing its name",
			fullMethod: ateapi.Control_CreateActor_FullMethodName,
			req: &ateapi.CreateActorRequest{Actor: &ateapi.Actor{
				Metadata:      actorMeta,
				ActorTemplate: template,
				SourceTag:     &ateapi.ObjectRef{Atespace: "team-b"},
			}},
			wantField: "actor.source_tag.name",
		},
		{
			name:       "CreateActor with a source tag missing its atespace",
			fullMethod: ateapi.Control_CreateActor_FullMethodName,
			req: &ateapi.CreateActorRequest{Actor: &ateapi.Actor{
				Metadata:      actorMeta,
				ActorTemplate: template,
				SourceTag:     &ateapi.ObjectRef{Name: "v1"},
			}},
			wantField: "actor.source_tag.atespace",
		},
		{
			name:       "GetActor without a name",
			fullMethod: ateapi.Control_GetActor_FullMethodName,
			req:        &ateapi.GetActorRequest{Actor: &ateapi.ObjectRef{Atespace: "team-a"}},
			wantField:  "actor.name",
		},
		{
			name:       "ListActors with an over-long atespace",
			fullMethod: ateapi.Control_ListActors_FullMethodName,
			req:        &ateapi.ListActorsRequest{Atespace: strings.Repeat("a", 64)},
			wantField:  "atespace",
		},
		{
			name:       "UpdateActor without metadata",
			fullMethod: ateapi.Control_UpdateActor_FullMethodName,
			req:        &ateapi.UpdateActorRequest{Actor: &ateapi.Actor{ActorTemplate: template}},
			wantField:  "actor.metadata.atespace",
		},
		{
			name:       "UpdateActor with metadata missing its atespace",
			fullMethod: ateapi.Control_UpdateActor_FullMethodName,
			req: &ateapi.UpdateActorRequest{Actor: &ateapi.Actor{
				Metadata:      &ateapi.ResourceMetadata{Name: "runner"},
				ActorTemplate: template,
			}},
			wantField: "actor.metadata.atespace",
		},
		{
			name:       "UpdateActor without a template",
			fullMethod: ateapi.Control_UpdateActor_FullMethodName,
			req:        &ateapi.UpdateActorRequest{Actor: &ateapi.Actor{Metadata: actorMeta}},
			wantField:  "actor.actor_template.name",
		},
		{
			name:       "UpdateActor with an invalid template atespace",
			fullMethod: ateapi.Control_UpdateActor_FullMethodName,
			req: &ateapi.UpdateActorRequest{Actor: &ateapi.Actor{
				Metadata:      actorMeta,
				ActorTemplate: &ateapi.ObjectRef{Atespace: "Shared", Name: "tmpl"},
			}},
			wantField: "actor.actor_template.atespace",
		},
		{
			name:       "DeleteActor with an invalid atespace",
			fullMethod: ateapi.Control_DeleteActor_FullMethodName,
			req:        &ateapi.DeleteActorRequest{Actor: &ateapi.ObjectRef{Atespace: "team:a", Name: "runner"}},
			wantField:  "actor.atespace",
		},
		{
			name:       "SuspendActor without an actor",
			fullMethod: ateapi.Control_SuspendActor_FullMethodName,
			req:        &ateapi.SuspendActorRequest{},
			wantField:  "actor.atespace",
		},
		{
			name:       "ResumeActor with an invalid name",
			fullMethod: ateapi.Control_ResumeActor_FullMethodName,
			req:        &ateapi.ResumeActorRequest{Actor: &ateapi.ObjectRef{Atespace: "team-a", Name: "Runner"}},
			wantField:  "actor.name",
		},
		{
			name:       "SuspendActor without a name",
			fullMethod: ateapi.Control_SuspendActor_FullMethodName,
			req:        &ateapi.SuspendActorRequest{Actor: &ateapi.ObjectRef{Atespace: "team-a"}},
			wantField:  "actor.name",
		},
		{
			name:       "RevertActor with an invalid atespace",
			fullMethod: ateapi.Control_RevertActor_FullMethodName,
			req:        &ateapi.RevertActorRequest{Actor: &ateapi.ObjectRef{Atespace: "team/a", Name: "runner"}},
			wantField:  "actor.atespace",
		},
		{
			name:       "GetActorEgressPolicy without an atespace",
			fullMethod: ateapi.Control_GetActorEgressPolicy_FullMethodName,
			req:        &ateapi.GetActorEgressPolicyRequest{Actor: &ateapi.ObjectRef{Name: "runner"}},
			wantField:  "actor.atespace",
		},
		{
			name:       "CreateActorEgressPolicy without an actor",
			fullMethod: ateapi.Control_CreateActorEgressPolicy_FullMethodName,
			req:        &ateapi.CreateActorEgressPolicyRequest{},
			wantField:  "actor.atespace",
		},
		{
			name:       "UpdateActorEgressPolicy with an invalid name",
			fullMethod: ateapi.Control_UpdateActorEgressPolicy_FullMethodName,
			req:        &ateapi.UpdateActorEgressPolicyRequest{Actor: &ateapi.ObjectRef{Atespace: "team-a", Name: "a b"}},
			wantField:  "actor.name",
		},
		{
			name:       "DeleteActorEgressPolicy without a name",
			fullMethod: ateapi.Control_DeleteActorEgressPolicy_FullMethodName,
			req:        &ateapi.DeleteActorEgressPolicyRequest{Actor: &ateapi.ObjectRef{Atespace: "team-a"}},
			wantField:  "actor.name",
		},
		{
			name:       "GetWorker without a worker",
			fullMethod: ateapi.Control_GetWorker_FullMethodName,
			req:        &ateapi.GetWorkerRequest{},
			wantField:  "worker.name",
		},
		{
			name:       "UpdateWorker without metadata",
			fullMethod: ateapi.Control_UpdateWorker_FullMethodName,
			req:        &ateapi.UpdateWorkerRequest{Worker: &ateapi.Worker{}},
			wantField:  "worker.metadata.name",
		},
		{
			name:       "DeleteWorker with an invalid name",
			fullMethod: ateapi.Control_DeleteWorker_FullMethodName,
			req:        &ateapi.DeleteWorkerRequest{Worker: &ateapi.ObjectRef{Name: "w/1"}},
			wantField:  "worker.name",
		},
		{
			name:       "DrainWorker with an over-long name",
			fullMethod: ateapi.Control_DrainWorker_FullMethodName,
			req:        &ateapi.DrainWorkerRequest{Worker: &ateapi.ObjectRef{Name: strings.Repeat("w", 64)}},
			wantField:  "worker.name",
		},
		{
			name:       "MarkWorkerAvailable without a name",
			fullMethod: ateapi.Control_MarkWorkerAvailable_FullMethodName,
			req:        &ateapi.MarkWorkerAvailableRequest{Worker: &ateapi.ObjectRef{}},
			wantField:  "worker.name",
		},
		{
			name:       "MarkWorkerUnavailable with an invalid name",
			fullMethod: ateapi.Control_MarkWorkerUnavailable_FullMethodName,
			req:        &ateapi.MarkWorkerUnavailableRequest{Worker: &ateapi.ObjectRef{Name: "w/1"}},
			wantField:  "worker.name",
		},
		{
			name:       "ListWorkerActorAssignments without a name",
			fullMethod: ateapi.Control_ListWorkerActorAssignments_FullMethodName,
			req:        &ateapi.ListWorkerActorAssignmentsRequest{Worker: &ateapi.ObjectRef{}},
			wantField:  "worker.name",
		},
		{
			name:       "MintActorJWT without an actor",
			fullMethod: ateapi.Control_MintActorJwt_FullMethodName,
			req:        &ateapi.MintActorJwtRequest{},
			wantField:  "actor.atespace",
		},
		{
			name:       "MintActorCertificate with an invalid name",
			fullMethod: ateapi.Control_MintActorCertificate_FullMethodName,
			req:        &ateapi.MintActorCertificateRequest{Actor: &ateapi.ObjectRef{Atespace: "team-a", Name: "a/b"}},
			wantField:  "actor.name",
		},
		{
			name:       "CreateTag without a source actor",
			fullMethod: ateapi.Control_CreateTag_FullMethodName,
			req: &ateapi.CreateTagRequest{Tag: &ateapi.Tag{
				Metadata: &ateapi.ResourceMetadata{Atespace: "team-a", Name: "v1"},
			}},
			wantField: "tag.source_actor.atespace",
		},
		{
			name:       "CreateTag with an invalid source actor atespace",
			fullMethod: ateapi.Control_CreateTag_FullMethodName,
			req: &ateapi.CreateTagRequest{Tag: &ateapi.Tag{
				Metadata:    &ateapi.ResourceMetadata{Atespace: "team-a", Name: "v1"},
				SourceActor: &ateapi.ObjectRef{Atespace: "Team-A", Name: "runner"},
			}},
			wantField: "tag.source_actor.atespace",
		},
		{
			name:       "GetTag without an atespace",
			fullMethod: ateapi.Control_GetTag_FullMethodName,
			req:        &ateapi.GetTagRequest{Tag: &ateapi.ObjectRef{Name: "v1"}},
			wantField:  "tag.atespace",
		},
		{
			name:       "ListTags with an invalid atespace",
			fullMethod: ateapi.Control_ListTags_FullMethodName,
			req:        &ateapi.ListTagsRequest{Atespace: "Team-A"},
			wantField:  "atespace",
		},
		{
			name:       "UpdateTag without metadata",
			fullMethod: ateapi.Control_UpdateTag_FullMethodName,
			req:        &ateapi.UpdateTagRequest{Tag: &ateapi.Tag{}},
			wantField:  "tag.metadata.atespace",
		},
		{
			name:       "DeleteTag without a name",
			fullMethod: ateapi.Control_DeleteTag_FullMethodName,
			req:        &ateapi.DeleteTagRequest{Tag: &ateapi.ObjectRef{Atespace: "team-a"}},
			wantField:  "tag.name",
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
		if r.authorizedInHandler != "" {
			continue
		}
		if _, err := r.extract("wrong-type"); apierror.Code(err) != codes.Internal {
			t.Errorf("%s: extract(wrong type) error = %v, want code Internal", fullMethod, err)
		}
	}
}

// Every RPC in ateapi.proto has exactly one of a permission rule or a reason
// its handler authorizes it, and the registry names no RPC that isn't in
// ateapi.proto.
func TestDefaultRPCPermissions_CoversEveryServedRPC(t *testing.T) {
	served := map[string]bool{}
	services := ateapi.File_api_ateapi_v1_ateapi_proto.Services()
	for i := range services.Len() {
		sd := services.Get(i)
		methods := sd.Methods()
		for j := range methods.Len() {
			m := methods.Get(j)
			fullMethod := "/" + string(sd.FullName()) + "/" + string(m.Name())
			if m.IsStreamingClient() || m.IsStreamingServer() {
				// The authz interceptor is unary-only, so a streaming RPC would go
				// unchecked.
				t.Errorf("%s is a streaming RPC, which the authz interceptor does not check", fullMethod)
			}
			served[fullMethod] = true
		}
	}

	for fullMethod := range served {
		r, ok := defaultRPCPermissions[fullMethod]
		if !ok {
			t.Errorf("%s has no entry in defaultRPCPermissions; add a rule, or inHandler if its handler authorizes the caller", fullMethod)
			continue
		}
		if (r.extract == nil) == (r.authorizedInHandler == "") {
			t.Errorf("%s: want exactly one of a check extractor or authorizedInHandler", fullMethod)
		}
	}
	for fullMethod := range defaultRPCPermissions {
		if !served[fullMethod] {
			t.Errorf("defaultRPCPermissions has %s, which no served service has", fullMethod)
		}
	}
}

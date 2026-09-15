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

package functionaltest

import (
	"context"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/testing/protocmp"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func setupEgressPolicyActor(t *testing.T, testName string) (*testContext, *ateapipb.ObjectRef) {
	t.Helper()
	ns := namespaceForTest(testName)
	tc := setupTest(t, ns)
	t.Cleanup(tc.cleanup)
	createTemplate(t, tc, ns)

	actor := &ateapipb.Actor{
		Metadata:      &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: "egress-actor"},
		ActorTemplate: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "tmpl1"},
	}
	if _, err := tc.client.CreateActor(context.Background(), &ateapipb.CreateActorRequest{Actor: actor}); err != nil {
		t.Fatalf("CreateActor failed: %v", err)
	}
	return tc, &ateapipb.ObjectRef{Atespace: testAtespace, Name: "egress-actor"}
}

func functionalEgressPolicy() *ateapipb.EgressPolicy {
	return &ateapipb.EgressPolicy{
		Metadata: &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: "default"},
		Rules: []*ateapipb.EgressRule{{
			Hostnames: &ateapipb.HostnameRule{Patterns: []string{"api.example.com"}},
		}},
	}
}

func TestCreateActor_DefaultEgressPolicy(t *testing.T) {
	ns := namespaceForTest("ns-template-egress")
	tc := setupTest(t, ns)
	t.Cleanup(tc.cleanup)
	base := createTemplate(t, tc, ns)
	createAtespace(t, tc, "template-space")
	ctx := t.Context()

	for _, tt := range []struct {
		name   string
		policy *ateapipb.EgressPolicyTemplate
	}{
		{name: "absent"},
		{name: "empty", policy: &ateapipb.EgressPolicyTemplate{}},
		{name: "rules", policy: &ateapipb.EgressPolicyTemplate{Rules: functionalEgressPolicy().Rules}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			template := proto.CloneOf(base)
			template.Metadata = &ateapipb.ResourceMetadata{Atespace: "template-space", Name: tt.name}
			template.DefaultEgressPolicy = tt.policy
			createdTemplate, err := tc.client.CreateActorTemplate(ctx, &ateapipb.CreateActorTemplateRequest{ActorTemplate: template})
			if err != nil {
				t.Fatal(err)
			}
			actor, err := tc.client.CreateActor(ctx, &ateapipb.CreateActorRequest{Actor: &ateapipb.Actor{
				Metadata:      &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: tt.name},
				ActorTemplate: &ateapipb.ObjectRef{Atespace: "template-space", Name: tt.name},
			}})
			if err != nil {
				t.Fatal(err)
			}
			actorRef := &ateapipb.ObjectRef{Atespace: testAtespace, Name: tt.name}
			policy, err := tc.client.GetActorEgressPolicy(ctx, &ateapipb.GetActorEgressPolicyRequest{Actor: actorRef})
			if tt.policy == nil {
				assertGrpcError(t, err, codes.NotFound, "EgressPolicy not found")
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			md := policy.GetMetadata()
			if md.GetAtespace() != testAtespace || md.GetName() != "default" || md.GetVersion() != 1 || md.GetUid() == "" || md.GetUid() == actor.GetMetadata().GetUid() || md.GetCreateTime() == nil || md.GetUpdateTime() == nil {
				t.Errorf("unexpected policy metadata: %v", md)
			}
			if diff := cmp.Diff(tt.policy.Rules, policy.Rules, protocmp.Transform()); diff != "" {
				t.Errorf("copied rules mismatch (-want +got):\n%s", diff)
			}

			// Templates have no public spec-update RPC. Change the stored default
			// to verify that existing policies retain their copied rules.
			createdTemplate, err = tc.persistence.UpdateActorTemplate(ctx, resources.ActorTemplateRefFromActorTemplate(createdTemplate), store.PreconditionFrom(createdTemplate), func(db *ateapipb.ActorTemplate) error {
				db.DefaultEgressPolicy = &ateapipb.EgressPolicyTemplate{Rules: []*ateapipb.EgressRule{{All: &emptypb.Empty{}}}}
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
			gotPolicy, err := tc.client.GetActorEgressPolicy(ctx, &ateapipb.GetActorEgressPolicyRequest{Actor: actorRef})
			if err != nil {
				t.Fatal(err)
			}
			if diff := cmp.Diff(policy, gotPolicy, protocmp.Transform()); diff != "" {
				t.Errorf("template edit changed actor policy (-want +got):\n%s", diff)
			}

			policy.Rules = []*ateapipb.EgressRule{{Hostnames: &ateapipb.HostnameRule{Patterns: []string{"updated.example.com"}}}}
			updated, err := tc.client.UpdateActorEgressPolicy(ctx, &ateapipb.UpdateActorEgressPolicyRequest{Actor: actorRef, EgressPolicy: policy})
			if err != nil {
				t.Fatal(err)
			}
			if updated.GetMetadata().GetUid() != md.GetUid() || updated.GetMetadata().GetVersion() != 2 {
				t.Errorf("unexpected updated policy metadata: %v", updated.GetMetadata())
			}
			if _, err := tc.client.DeleteActorEgressPolicy(ctx, &ateapipb.DeleteActorEgressPolicyRequest{Actor: actorRef}); err != nil {
				t.Fatal(err)
			}
			_, err = tc.client.GetActorEgressPolicy(ctx, &ateapipb.GetActorEgressPolicyRequest{Actor: actorRef})
			assertGrpcError(t, err, codes.NotFound, "EgressPolicy not found")
			gotTemplate, err := tc.client.GetActorTemplate(ctx, &ateapipb.GetActorTemplateRequest{ActorTemplate: actor.ActorTemplate})
			if err != nil {
				t.Fatal(err)
			}
			if diff := cmp.Diff(createdTemplate, gotTemplate, protocmp.Transform()); diff != "" {
				t.Errorf("policy edits changed template (-want +got):\n%s", diff)
			}
		})
	}
}

func createEgressPolicy(t *testing.T, tc *testContext, actor *ateapipb.ObjectRef) *ateapipb.EgressPolicy {
	t.Helper()
	created, err := tc.client.CreateActorEgressPolicy(context.Background(), &ateapipb.CreateActorEgressPolicyRequest{
		Actor:        actor,
		EgressPolicy: functionalEgressPolicy(),
	})
	if err != nil {
		t.Fatalf("CreateActorEgressPolicy failed: %v", err)
	}
	return created
}

func TestCreateActorEgressPolicy_Success(t *testing.T) {
	tc, actor := setupEgressPolicyActor(t, "ns-create-egress-policy")
	policy := functionalEgressPolicy()
	policy.Metadata.Uid = "caller-supplied-uid"
	policy.Metadata.Version = 999
	policy.Metadata.CreateTime = timestamppb.New(time.Unix(1, 0))
	policy.Metadata.UpdateTime = timestamppb.New(time.Unix(1, 0))

	created, err := tc.client.CreateActorEgressPolicy(context.Background(), &ateapipb.CreateActorEgressPolicyRequest{
		Actor:        actor,
		EgressPolicy: policy,
	})
	if err != nil {
		t.Fatalf("CreateActorEgressPolicy failed: %v", err)
	}
	md := created.GetMetadata()
	if md.GetAtespace() != testAtespace || md.GetName() != "default" {
		t.Errorf("identity = %s/%s, want %s/default", md.GetAtespace(), md.GetName(), testAtespace)
	}
	if md.GetVersion() != 1 {
		t.Errorf("version = %d, want 1", md.GetVersion())
	}
	if md.GetUid() == "" || md.GetUid() == "caller-supplied-uid" {
		t.Errorf("uid = %q, want a server-generated value", md.GetUid())
	}
	if md.GetCreateTime() == nil || md.GetUpdateTime() == nil || md.GetCreateTime().AsTime().Equal(time.Unix(1, 0)) || md.GetUpdateTime().AsTime().Equal(time.Unix(1, 0)) {
		t.Errorf("timestamps = (%v, %v), want server-generated values", md.GetCreateTime(), md.GetUpdateTime())
	}

	got, err := tc.client.GetActorEgressPolicy(context.Background(), &ateapipb.GetActorEgressPolicyRequest{Actor: actor})
	if err != nil {
		t.Fatalf("GetActorEgressPolicy failed: %v", err)
	}
	if diff := cmp.Diff(created, got, protocmp.Transform()); diff != "" {
		t.Errorf("GetActorEgressPolicy mismatch (-created +got):\n%s", diff)
	}
}

func TestCreateActorEgressPolicy_Errors(t *testing.T) {
	tc, actor := setupEgressPolicyActor(t, "ns-create-egress-policy-errors")
	createEgressPolicy(t, tc, actor)

	_, err := tc.client.CreateActorEgressPolicy(context.Background(), &ateapipb.CreateActorEgressPolicyRequest{
		Actor:        actor,
		EgressPolicy: functionalEgressPolicy(),
	})
	assertGrpcError(t, err, codes.AlreadyExists, "EgressPolicy already exists")

	_, err = tc.client.CreateActorEgressPolicy(context.Background(), &ateapipb.CreateActorEgressPolicyRequest{
		Actor:        &ateapipb.ObjectRef{Atespace: testAtespace, Name: "missing-actor"},
		EgressPolicy: functionalEgressPolicy(),
	})
	assertGrpcError(t, err, codes.FailedPrecondition, "parent Actor does not exist")
}

func TestGetActorEgressPolicy_NotFound(t *testing.T) {
	tc, actor := setupEgressPolicyActor(t, "ns-get-egress-policy-missing")
	_, err := tc.client.GetActorEgressPolicy(context.Background(), &ateapipb.GetActorEgressPolicyRequest{Actor: actor})
	assertGrpcError(t, err, codes.NotFound, "EgressPolicy not found")
}

func TestUpdateActorEgressPolicy(t *testing.T) {
	tc, actor := setupEgressPolicyActor(t, "ns-update-egress-policy")
	created := createEgressPolicy(t, tc, actor)
	toUpdate := proto.Clone(created).(*ateapipb.EgressPolicy)
	toUpdate.Rules = []*ateapipb.EgressRule{{All: &emptypb.Empty{}}}

	updated, err := tc.client.UpdateActorEgressPolicy(context.Background(), &ateapipb.UpdateActorEgressPolicyRequest{
		Actor:        actor,
		EgressPolicy: toUpdate,
	})
	if err != nil {
		t.Fatalf("UpdateActorEgressPolicy failed: %v", err)
	}
	if updated.GetMetadata().GetUid() != created.GetMetadata().GetUid() || updated.GetMetadata().GetVersion() != 2 {
		t.Errorf("updated metadata = %v, want original UID and version 2", updated.GetMetadata())
	}
	if !updated.GetMetadata().GetCreateTime().AsTime().Equal(created.GetMetadata().GetCreateTime().AsTime()) {
		t.Errorf("create_time changed from %v to %v", created.GetMetadata().GetCreateTime(), updated.GetMetadata().GetCreateTime())
	}
	if !updated.GetMetadata().GetUpdateTime().AsTime().After(created.GetMetadata().GetUpdateTime().AsTime()) {
		t.Errorf("update_time = %v, want after %v", updated.GetMetadata().GetUpdateTime(), created.GetMetadata().GetUpdateTime())
	}
	if len(updated.GetRules()) != 1 || updated.GetRules()[0].GetAll() == nil {
		t.Errorf("updated rules = %v, want one all rule", updated.GetRules())
	}

	got, err := tc.client.GetActorEgressPolicy(context.Background(), &ateapipb.GetActorEgressPolicyRequest{Actor: actor})
	if err != nil {
		t.Fatalf("GetActorEgressPolicy failed: %v", err)
	}
	if diff := cmp.Diff(updated, got, protocmp.Transform()); diff != "" {
		t.Errorf("persisted policy mismatch (-updated +got):\n%s", diff)
	}

	_, err = tc.client.UpdateActorEgressPolicy(context.Background(), &ateapipb.UpdateActorEgressPolicyRequest{
		Actor:        actor,
		EgressPolicy: toUpdate,
	})
	assertGrpcError(t, err, codes.Aborted, "EgressPolicy version conflict")
}

func TestUpdateActorEgressPolicy_Preconditions(t *testing.T) {
	tc, actor := setupEgressPolicyActor(t, "ns-update-egress-policy-preconditions")
	created := createEgressPolicy(t, tc, actor)

	unguarded := proto.Clone(created).(*ateapipb.EgressPolicy)
	unguarded.Metadata.Uid, unguarded.Metadata.Version = "", 0
	_, err := tc.client.UpdateActorEgressPolicy(context.Background(), &ateapipb.UpdateActorEgressPolicyRequest{Actor: actor, EgressPolicy: unguarded})
	assertGrpcError(t, err, codes.InvalidArgument, "EgressPolicy UID and version are required")

	wrongUID := proto.Clone(created).(*ateapipb.EgressPolicy)
	wrongUID.Metadata.Uid = "9a2b1c3d-4e5f-6a7b-8c9d-0e1f2a3b4c5d"
	_, err = tc.client.UpdateActorEgressPolicy(context.Background(), &ateapipb.UpdateActorEgressPolicyRequest{Actor: actor, EgressPolicy: wrongUID})
	assertGrpcError(t, err, codes.Aborted, "EgressPolicy UID conflict")

	missingActor := &ateapipb.ObjectRef{Atespace: testAtespace, Name: "missing-actor"}
	_, err = tc.client.UpdateActorEgressPolicy(context.Background(), &ateapipb.UpdateActorEgressPolicyRequest{Actor: missingActor, EgressPolicy: created})
	assertGrpcError(t, err, codes.NotFound, "EgressPolicy not found")
}

func TestDeleteActorEgressPolicy(t *testing.T) {
	tc, actor := setupEgressPolicyActor(t, "ns-delete-egress-policy")
	created := createEgressPolicy(t, tc, actor)

	deleted, err := tc.client.DeleteActorEgressPolicy(context.Background(), &ateapipb.DeleteActorEgressPolicyRequest{Actor: actor})
	if err != nil {
		t.Fatalf("DeleteActorEgressPolicy failed: %v", err)
	}
	if diff := cmp.Diff(created, deleted, protocmp.Transform()); diff != "" {
		t.Errorf("deleted policy mismatch (-created +deleted):\n%s", diff)
	}
	_, err = tc.client.DeleteActorEgressPolicy(context.Background(), &ateapipb.DeleteActorEgressPolicyRequest{Actor: actor})
	assertGrpcError(t, err, codes.NotFound, "EgressPolicy not found")
}

func TestDeleteActor_CascadesEgressPolicy(t *testing.T) {
	tc, actor := setupEgressPolicyActor(t, "ns-delete-actor-egress-policy")
	createEgressPolicy(t, tc, actor)

	if _, err := tc.client.DeleteActor(context.Background(), &ateapipb.DeleteActorRequest{Actor: actor}); err != nil {
		t.Fatalf("DeleteActor failed: %v", err)
	}
	_, err := tc.client.GetActorEgressPolicy(context.Background(), &ateapipb.GetActorEgressPolicyRequest{Actor: actor})
	assertGrpcError(t, err, codes.NotFound, "EgressPolicy not found")
}

func TestValidation_ActorEgressPolicy(t *testing.T) {
	tc, _ := setupEgressPolicyActor(t, "ns-validation-egress-policy")
	ctx := context.Background()

	_, err := tc.client.CreateActorEgressPolicy(ctx, &ateapipb.CreateActorEgressPolicyRequest{})
	assertGrpcErrorRegex(t, err, codes.InvalidArgument, "actor: Required value")
	_, err = tc.client.GetActorEgressPolicy(ctx, &ateapipb.GetActorEgressPolicyRequest{})
	assertGrpcErrorRegex(t, err, codes.InvalidArgument, "actor: Required value")
	_, err = tc.client.UpdateActorEgressPolicy(ctx, &ateapipb.UpdateActorEgressPolicyRequest{})
	assertGrpcErrorRegex(t, err, codes.InvalidArgument, "actor: Required value")
	_, err = tc.client.DeleteActorEgressPolicy(ctx, &ateapipb.DeleteActorEgressPolicyRequest{})
	assertGrpcErrorRegex(t, err, codes.InvalidArgument, "actor: Required value")
}

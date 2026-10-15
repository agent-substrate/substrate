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

	"github.com/agent-substrate/substrate/internal/preview"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/testing/protocmp"
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
			Http: &ateapipb.HttpRule{Hostnames: []string{"api.example.com"}},
		}},
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

func TestCreateActor_InitialEgressPolicy(t *testing.T) {
	// Explicit initial policies do not require the golden policy preview gate
	// or a client that opts in to preview features.
	preview.InitForTest(t)
	ns := namespaceForTest("ns-initial-egress-policy")
	tc := setupTest(t, ns)
	t.Cleanup(tc.cleanup)
	createTemplate(t, tc, ns)
	ctx := t.Context()
	newPolicy := func(mods ...func(*ateapipb.EgressPolicy)) *ateapipb.EgressPolicy {
		policy := &ateapipb.EgressPolicy{
			Metadata: &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: resources.SingletonName},
			Rules: []*ateapipb.EgressRule{
				{Http: &ateapipb.HttpRule{Hostnames: []string{"api.example.com"}}},
				{Https: &ateapipb.HttpsRule{Hostnames: []string{"api.example.com"}}},
			},
		}
		for _, mod := range mods {
			mod(policy)
		}
		return policy
	}

	for _, tt := range []struct {
		name   string
		policy *ateapipb.EgressPolicy
		valid  bool
	}{
		{name: "omitted", valid: true},
		{name: "configured", valid: true, policy: newPolicy(func(p *ateapipb.EgressPolicy) {
			p.Metadata.Uid = "caller-supplied-uid"
			p.Metadata.Version = 999
			p.Metadata.CreateTime = timestamppb.New(time.Unix(1, 0))
			p.Metadata.UpdateTime = timestamppb.New(time.Unix(1, 0))
		})},
		{name: "empty", policy: newPolicy(func(p *ateapipb.EgressPolicy) { p.Rules = nil })},
		{name: "missing-metadata", policy: newPolicy(func(p *ateapipb.EgressPolicy) { p.Metadata = nil })},
		{name: "missing-name", policy: newPolicy(func(p *ateapipb.EgressPolicy) { p.Metadata.Name = "" })},
		{name: "wrong-name", policy: newPolicy(func(p *ateapipb.EgressPolicy) { p.Metadata.Name = "custom" })},
		{name: "missing-atespace", policy: newPolicy(func(p *ateapipb.EgressPolicy) { p.Metadata.Atespace = "" })},
		{name: "wrong-atespace", policy: newPolicy(func(p *ateapipb.EgressPolicy) { p.Metadata.Atespace = "other" })},
		{name: "invalid", policy: newPolicy(func(p *ateapipb.EgressPolicy) { p.Rules[0].Http.Hostnames = []string{"bad/host"} })},
		{name: "conflicting", policy: newPolicy(func(p *ateapipb.EgressPolicy) {
			p.Rules = append(p.Rules, &ateapipb.EgressRule{TlsPassthrough: &ateapipb.TlsPassthroughRule{Hostnames: []string{"api.example.com"}, Ports: &ateapipb.Ports{Numbers: []int32{443}}}})
		})},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ref := &ateapipb.ObjectRef{Atespace: testAtespace, Name: tt.name}
			req := &ateapipb.CreateActorRequest{
				Actor: &ateapipb.Actor{
					Metadata:      &ateapipb.ResourceMetadata{Atespace: ref.Atespace, Name: ref.Name},
					ActorTemplate: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "tmpl1"},
				},
				InitialEgressPolicy: tt.policy,
			}
			actor, err := tc.client.CreateActor(ctx, req)
			if !tt.valid {
				assertGrpcErrorRegex(t, err, codes.InvalidArgument, "initial_egress_policy")
				_, err = tc.client.GetActor(ctx, &ateapipb.GetActorRequest{Actor: ref})
				assertGrpcErrorRegex(t, err, codes.NotFound, "not found")
				_, err = tc.client.GetActorEgressPolicy(ctx, &ateapipb.GetActorEgressPolicyRequest{Actor: ref, Name: resources.SingletonName})
				assertGrpcErrorRegex(t, err, codes.NotFound, "not found")
				return
			}
			if err != nil {
				t.Fatalf("CreateActor: %v", err)
			}
			policy, err := tc.client.GetActorEgressPolicy(ctx, &ateapipb.GetActorEgressPolicyRequest{Actor: ref, Name: resources.SingletonName})
			if tt.policy == nil {
				assertGrpcErrorRegex(t, err, codes.NotFound, "not found")
				return
			}
			if err != nil {
				t.Fatalf("GetActorEgressPolicy: %v", err)
			}
			want := &ateapipb.EgressPolicy{Metadata: policy.Metadata, Rules: []*ateapipb.EgressRule{
				{Http: &ateapipb.HttpRule{Hostnames: []string{"api.example.com"}, Ports: &ateapipb.Ports{Numbers: []int32{80}}}},
				{Https: &ateapipb.HttpsRule{Hostnames: []string{"api.example.com"}, Ports: &ateapipb.Ports{Numbers: []int32{443}}}},
			}}
			if diff := cmp.Diff(want, policy, protocmp.Transform()); diff != "" {
				t.Fatalf("initial policy (-want +got):\n%s", diff)
			}
			md := policy.GetMetadata()
			if md.GetAtespace() != ref.Atespace || md.GetName() != resources.SingletonName || md.GetUid() == "" || md.GetUid() == actor.GetMetadata().GetUid() || md.GetVersion() != 1 || md.GetCreateTime() == nil || md.GetUpdateTime() == nil {
				t.Fatalf("unexpected policy metadata: %v", md)
			}
			if md.GetUid() == "caller-supplied-uid" || md.GetCreateTime().AsTime().Equal(time.Unix(1, 0)) || md.GetUpdateTime().AsTime().Equal(time.Unix(1, 0)) {
				t.Fatalf("caller-supplied server metadata was retained: %v", md)
			}

			// Repeating CreateActor must not replace the policy on the existing actor.
			req.InitialEgressPolicy.Rules = []*ateapipb.EgressRule{{Http: &ateapipb.HttpRule{Hostnames: []string{"other.example.com"}}}}
			_, err = tc.client.CreateActor(ctx, req)
			assertGrpcErrorRegex(t, err, codes.AlreadyExists, "already exists")
			got, err := tc.client.GetActorEgressPolicy(ctx, &ateapipb.GetActorEgressPolicyRequest{Actor: ref, Name: resources.SingletonName})
			if err != nil {
				t.Fatal(err)
			}
			if diff := cmp.Diff(policy, got, protocmp.Transform()); diff != "" {
				t.Fatalf("duplicate CreateActor changed policy (-want +got):\n%s", diff)
			}
		})
	}
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

	got, err := tc.client.GetActorEgressPolicy(context.Background(), &ateapipb.GetActorEgressPolicyRequest{Actor: actor, Name: resources.SingletonName})
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
	_, err := tc.client.GetActorEgressPolicy(context.Background(), &ateapipb.GetActorEgressPolicyRequest{Actor: actor, Name: resources.SingletonName})
	assertGrpcError(t, err, codes.NotFound, "EgressPolicy for actor "+actor.GetAtespace()+"/"+actor.GetName()+" not found")
}

func TestUpdateActorEgressPolicy(t *testing.T) {
	tc, actor := setupEgressPolicyActor(t, "ns-update-egress-policy")
	created := createEgressPolicy(t, tc, actor)
	toUpdate := proto.Clone(created).(*ateapipb.EgressPolicy)
	toUpdate.Rules = []*ateapipb.EgressRule{{TlsPassthrough: &ateapipb.TlsPassthroughRule{Hostnames: []string{"*"}, Ports: &ateapipb.Ports{Numbers: []int32{443}}}}}

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
	if len(updated.GetRules()) != 1 || updated.GetRules()[0].GetTlsPassthrough() == nil {
		t.Errorf("updated rules = %v, want one tls_passthrough rule", updated.GetRules())
	}

	got, err := tc.client.GetActorEgressPolicy(context.Background(), &ateapipb.GetActorEgressPolicyRequest{Actor: actor, Name: resources.SingletonName})
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

	deleted, err := tc.client.DeleteActorEgressPolicy(context.Background(), &ateapipb.DeleteActorEgressPolicyRequest{Actor: actor, Name: resources.SingletonName})
	if err != nil {
		t.Fatalf("DeleteActorEgressPolicy failed: %v", err)
	}
	if diff := cmp.Diff(created, deleted, protocmp.Transform()); diff != "" {
		t.Errorf("deleted policy mismatch (-created +deleted):\n%s", diff)
	}
	_, err = tc.client.DeleteActorEgressPolicy(context.Background(), &ateapipb.DeleteActorEgressPolicyRequest{Actor: actor, Name: resources.SingletonName})
	assertGrpcError(t, err, codes.NotFound, "EgressPolicy not found")
}

func TestDeleteActor_CascadesEgressPolicy(t *testing.T) {
	tc, actor := setupEgressPolicyActor(t, "ns-delete-actor-egress-policy")
	createEgressPolicy(t, tc, actor)

	if _, err := tc.client.DeleteActor(context.Background(), &ateapipb.DeleteActorRequest{Actor: actor}); err != nil {
		t.Fatalf("DeleteActor failed: %v", err)
	}
	_, err := tc.client.GetActorEgressPolicy(context.Background(), &ateapipb.GetActorEgressPolicyRequest{Actor: actor, Name: resources.SingletonName})
	assertGrpcError(t, err, codes.NotFound, "EgressPolicy for actor "+actor.GetAtespace()+"/"+actor.GetName()+" not found")
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

func TestDeleteActorEgressPolicy_Preconditions(t *testing.T) {
	tc, actor := setupEgressPolicyActor(t, "ns-delete-egress-policy-preconditions")
	ctx := context.Background()
	del := func(opts *ateapipb.DeleteOptions) error {
		_, err := tc.client.DeleteActorEgressPolicy(ctx, &ateapipb.DeleteActorEgressPolicyRequest{Actor: actor, Name: resources.SingletonName, Options: opts})
		return err
	}

	policy := createEgressPolicy(t, tc, actor)
	uid, version := policy.GetMetadata().GetUid(), policy.GetMetadata().GetVersion()

	assertGrpcError(t, del(&ateapipb.DeleteOptions{Version: version + 1}), codes.Aborted, "EgressPolicy version conflict")
	assertGrpcError(t, del(&ateapipb.DeleteOptions{Uid: uid, Version: version + 1}), codes.Aborted, "EgressPolicy version conflict")
	assertGrpcError(t, del(&ateapipb.DeleteOptions{Uid: foreignUID}), codes.Aborted, "EgressPolicy UID conflict")
	assertGrpcError(t, del(&ateapipb.DeleteOptions{Uid: foreignUID, Version: version}), codes.Aborted, "EgressPolicy UID conflict")
	if _, err := tc.client.GetActorEgressPolicy(ctx, &ateapipb.GetActorEgressPolicyRequest{Actor: actor, Name: resources.SingletonName}); err != nil {
		t.Fatalf("a refused delete removed the policy: %v", err)
	}

	if err := del(&ateapipb.DeleteOptions{Version: version}); err != nil {
		t.Fatalf("DeleteActorEgressPolicy with the matching version: %v", err)
	}
	policy = createEgressPolicy(t, tc, actor)
	if err := del(&ateapipb.DeleteOptions{Uid: policy.GetMetadata().GetUid()}); err != nil {
		t.Fatalf("DeleteActorEgressPolicy with the matching uid: %v", err)
	}
	policy = createEgressPolicy(t, tc, actor)
	if err := del(&ateapipb.DeleteOptions{Uid: policy.GetMetadata().GetUid(), Version: policy.GetMetadata().GetVersion()}); err != nil {
		t.Fatalf("DeleteActorEgressPolicy with both guards: %v", err)
	}
	_, err := tc.client.GetActorEgressPolicy(ctx, &ateapipb.GetActorEgressPolicyRequest{Actor: actor, Name: resources.SingletonName})
	assertGrpcError(t, err, codes.NotFound, "EgressPolicy for actor "+actor.GetAtespace()+"/"+actor.GetName()+" not found")
}

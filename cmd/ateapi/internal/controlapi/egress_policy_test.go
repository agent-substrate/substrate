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

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store/storetest"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func TestActorEgressPolicy(t *testing.T) {
	persistence, cleanup := storetest.SetupTestStore(t)
	defer cleanup()
	service := &RPCService{impl: &ServiceImpl{store: persistence}}
	if _, err := persistence.CreateAtespace(t.Context(), &ateapipb.Atespace{
		Metadata: &ateapipb.ResourceMetadata{Name: testAtespace},
	}); err != nil {
		t.Fatal(err)
	}
	_, err := persistence.CreateActor(t.Context(), &ateapipb.Actor{
		Metadata: &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: "egress-actor"},
		Status:   &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_RUNNING},
	})
	if err != nil {
		t.Fatal(err)
	}
	actorRef := &ateapipb.ObjectRef{Atespace: testAtespace, Name: "egress-actor"}

	if _, err := service.GetActorEgressPolicy(t.Context(), &ateapipb.GetActorEgressPolicyRequest{
		Actor: actorRef,
	}); status.Code(err) != codes.NotFound {
		t.Fatalf("policy before create status = %v, want NotFound", status.Code(err))
	}
	if _, err := service.GetActorEgressPolicy(t.Context(), &ateapipb.GetActorEgressPolicyRequest{
		Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "missing-actor"},
	}); status.Code(err) != codes.NotFound {
		t.Fatalf("missing parent status = %v, want NotFound", status.Code(err))
	}
	created, err := service.CreateActorEgressPolicy(t.Context(), &ateapipb.CreateActorEgressPolicyRequest{
		Actor: actorRef,
		EgressPolicy: &ateapipb.EgressPolicy{
			Metadata: &ateapipb.ResourceMetadata{
				Atespace: testAtespace,
				Name:     "default",
				Uid:      "ignored",
				Version:  99,
			}, Rules: []*ateapipb.EgressRule{{
				Hostnames: &ateapipb.HostnameRule{
					Patterns: []string{"api.example.com"},
					Effects: &ateapipb.EgressRuleEffects{
						InjectStaticHeaders: []*ateapipb.CredentialHeaderInjection{{
							Header:        "Authorization",
							Prefix:        "Bearer ",
							CredentialUri: "substrate-secret://kubernetes.io/provider/ns/name",
						}},
					},
				},
			}},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.CreateActorEgressPolicy(t.Context(), &ateapipb.CreateActorEgressPolicyRequest{
		Actor: actorRef,
		EgressPolicy: &ateapipb.EgressPolicy{
			Metadata: &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: "default"},
		},
	}); status.Code(err) != codes.AlreadyExists {
		t.Fatalf("create collision status = %v, want AlreadyExists", status.Code(err))
	}
	if created.GetRules()[0].GetHostnames().GetEffects().GetInjectStaticHeaders()[0].GetHeader() != "Authorization" {
		t.Fatalf("policy input was rewritten: %v", created)
	}
	if md := created.GetMetadata(); md.GetName() != "default" || md.GetAtespace() != testAtespace || md.GetUid() == "" || md.GetVersion() != 1 || md.GetCreateTime() == nil || md.GetUpdateTime() == nil {
		t.Fatalf("created metadata = %v", md)
	}
	got, err := service.GetActorEgressPolicy(t.Context(), &ateapipb.GetActorEgressPolicyRequest{Actor: actorRef})
	if err != nil || !proto.Equal(got, created) {
		t.Fatalf("policy after create = %v, %v; want %v", got, err, created)
	}
	if _, err := service.impl.UpdateEgressPolicy(t.Context(), resources.ActorRefFromObjectRef(actorRef), store.PreconditionFrom(created), func(policy *ateapipb.EgressPolicy) error {
		policy.Rules[0].Hostnames.Patterns = nil
		return nil
	}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("invalid internal update status = %v, want InvalidArgument", status.Code(err))
	}
	replacement := proto.Clone(created).(*ateapipb.EgressPolicy)
	replacement.Rules = nil
	missingPreconditions := proto.Clone(replacement).(*ateapipb.EgressPolicy)
	missingPreconditions.Metadata.Uid = ""
	missingPreconditions.Metadata.Version = 0
	if _, err := service.UpdateActorEgressPolicy(t.Context(), &ateapipb.UpdateActorEgressPolicyRequest{
		Actor:        actorRef,
		EgressPolicy: missingPreconditions,
	}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("missing preconditions status = %v, want InvalidArgument", status.Code(err))
	}
	changedIdentity := proto.Clone(replacement).(*ateapipb.EgressPolicy)
	changedIdentity.Metadata.Atespace = "other"
	changedIdentity.Metadata.Name = "other"
	if _, err := service.UpdateActorEgressPolicy(t.Context(), &ateapipb.UpdateActorEgressPolicyRequest{
		Actor:        actorRef,
		EgressPolicy: changedIdentity,
	}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("changed identity status = %v, want InvalidArgument", status.Code(err))
	}
	updated, err := service.UpdateActorEgressPolicy(t.Context(), &ateapipb.UpdateActorEgressPolicyRequest{
		Actor:        actorRef,
		EgressPolicy: replacement,
	})
	if err != nil || updated.GetMetadata().GetVersion() != 2 || len(updated.GetRules()) != 0 {
		t.Fatalf("replacement = %v, %v; want empty version 2", updated, err)
	}
	if _, err := service.UpdateActorEgressPolicy(t.Context(), &ateapipb.UpdateActorEgressPolicyRequest{
		Actor:        actorRef,
		EgressPolicy: replacement,
	}); status.Code(err) != codes.Aborted {
		t.Fatalf("stale replacement status = %v, want Aborted", status.Code(err))
	}
	deleted, err := service.DeleteActorEgressPolicy(t.Context(), &ateapipb.DeleteActorEgressPolicyRequest{
		Actor: actorRef,
	})
	if err != nil || !proto.Equal(deleted, updated) {
		t.Fatalf("deleted policy = %v, %v; want %v", deleted, err, updated)
	}
	if _, err := service.GetActorEgressPolicy(t.Context(), &ateapipb.GetActorEgressPolicyRequest{
		Actor: actorRef,
	}); status.Code(err) != codes.NotFound {
		t.Fatalf("policy after delete status = %v, want NotFound", status.Code(err))
	}
}

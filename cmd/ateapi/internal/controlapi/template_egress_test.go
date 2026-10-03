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
	"context"
	"errors"
	"testing"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store/storetest"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func TestCreateActor_DefaultEgressPolicy(t *testing.T) {
	for _, tt := range []struct {
		name   string
		policy *ateapipb.EgressPolicyTemplate
	}{
		{name: "absent"},
		{name: "empty", policy: &ateapipb.EgressPolicyTemplate{}},
		{name: "configured", policy: &ateapipb.EgressPolicyTemplate{Rules: []*ateapipb.EgressRule{
			{Http: &ateapipb.HTTPRule{Hostnames: []string{"api.example.com"}}},
			{Https: &ateapipb.HTTPSRule{Hostnames: []string{"api.example.com"}}},
		}}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ctx := t.Context()
			persistence := newTestPersistence(t)
			svc := &RPCService{impl: newServiceImpl(persistence, nil), sandboxConfigLister: gvisorDefaultLister(t)}
			storetest.MustCreateAtespace(t, ctx, persistence, "templates")
			storetest.MustCreateAtespace(t, ctx, persistence, "actors")
			tmpl, err := svc.CreateActorTemplate(ctx, &ateapipb.CreateActorTemplateRequest{ActorTemplate: validActorTemplate(func(tmpl *ateapipb.ActorTemplate) {
				tmpl.Metadata = &ateapipb.ResourceMetadata{Atespace: "templates", Name: "tmpl"}
				tmpl.DefaultEgressPolicy = tt.policy
			})})
			if err != nil {
				t.Fatal(err)
			}
			tmplRef := resources.ActorTemplateRefFromActorTemplate(tmpl).ToObjectRef()
			var policies []*ateapipb.EgressPolicy
			for _, name := range []string{"alpha", "beta"} {
				actor, err := svc.CreateActor(ctx, &ateapipb.CreateActorRequest{Actor: &ateapipb.Actor{
					Metadata: &ateapipb.ResourceMetadata{Atespace: "actors", Name: name}, ActorTemplate: tmplRef,
				}})
				if err != nil {
					t.Fatal(err)
				}
				policy, err := svc.GetActorEgressPolicy(ctx, &ateapipb.GetActorEgressPolicyRequest{Actor: resources.ActorRefFromActor(actor).ToObjectRef()})
				if tt.policy == nil {
					if status.Code(err) != codes.NotFound {
						t.Fatalf("policy without default rules: %v, want NotFound", err)
					}
					continue
				}
				if err != nil {
					t.Fatal(err)
				}
				md := policy.GetMetadata()
				if md.GetAtespace() != "actors" || md.GetName() != "default" || md.GetUid() == "" || md.GetUid() == actor.GetMetadata().GetUid() || md.GetVersion() != 1 || md.GetCreateTime() == nil || md.GetUpdateTime() == nil {
					t.Errorf("unexpected policy metadata: %v", md)
				}
				want := &ateapipb.EgressPolicy{Metadata: md, Rules: tmpl.GetDefaultEgressPolicy().GetRules()}
				if !proto.Equal(policy, want) {
					t.Errorf("policy = %v, want %v", policy, want)
				}
				policies = append(policies, policy)
			}
			if len(policies) == 0 {
				return
			}
			if policies[0].GetMetadata().GetUid() == policies[1].GetMetadata().GetUid() {
				t.Fatal("actors share a policy UID")
			}

			alpha := &ateapipb.ObjectRef{Atespace: "actors", Name: "alpha"}
			policies[0].Rules = nil
			if _, err := svc.UpdateActorEgressPolicy(ctx, &ateapipb.UpdateActorEgressPolicyRequest{Actor: alpha, EgressPolicy: policies[0]}); err != nil {
				t.Fatal(err)
			}
			if _, err := svc.DeleteActorEgressPolicy(ctx, &ateapipb.DeleteActorEgressPolicyRequest{Actor: alpha}); err != nil {
				t.Fatal(err)
			}
			if _, err := svc.GetActorEgressPolicy(ctx, &ateapipb.GetActorEgressPolicyRequest{Actor: alpha}); status.Code(err) != codes.NotFound {
				t.Fatalf("deleted policy: %v, want NotFound", err)
			}
			betaPolicy, err := svc.GetActorEgressPolicy(ctx, &ateapipb.GetActorEgressPolicyRequest{Actor: &ateapipb.ObjectRef{Atespace: "actors", Name: "beta"}})
			if err != nil || !proto.Equal(betaPolicy, policies[1]) {
				t.Fatalf("editing alpha changed beta's policy: %v, %v", betaPolicy, err)
			}
			gotTemplate, err := svc.GetActorTemplate(ctx, &ateapipb.GetActorTemplateRequest{ActorTemplate: tmplRef})
			if err != nil || !proto.Equal(gotTemplate, tmpl) {
				t.Fatalf("editing alpha changed the template: %v, %v", gotTemplate, err)
			}
		})
	}
}

type goldenEgressControl struct {
	*RPCService
	resume func(context.Context, *ateapipb.ResumeActorRequest) (*ateapipb.ResumeActorResponse, error)
}

func (c *goldenEgressControl) ResumeActor(ctx context.Context, req *ateapipb.ResumeActorRequest) (*ateapipb.ResumeActorResponse, error) {
	return c.resume(ctx, req)
}

func TestReconcileOne_DefaultEgressPolicyBeforeResume(t *testing.T) {
	for _, tt := range []struct {
		name   string
		policy *ateapipb.EgressPolicyTemplate
	}{
		{name: "absent"},
		{name: "empty", policy: &ateapipb.EgressPolicyTemplate{}},
		{name: "configured", policy: &ateapipb.EgressPolicyTemplate{Rules: []*ateapipb.EgressRule{{Https: &ateapipb.HTTPSRule{Hostnames: []string{"api.example.com"}}}}}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ctx := t.Context()
			persistence := newTestPersistence(t)
			svc := &RPCService{impl: newServiceImpl(persistence, nil), sandboxConfigLister: gvisorDefaultLister(t)}
			storetest.MustCreateAtespace(t, ctx, persistence, "templates")
			tmpl, err := svc.CreateActorTemplate(ctx, &ateapipb.CreateActorTemplateRequest{ActorTemplate: validActorTemplate(func(tmpl *ateapipb.ActorTemplate) {
				tmpl.Metadata = &ateapipb.ResourceMetadata{Atespace: "templates", Name: "tmpl"}
				tmpl.DefaultEgressPolicy = tt.policy
			})})
			if err != nil {
				t.Fatal(err)
			}
			stop := errors.New("stop before running workload")
			control := &goldenEgressControl{RPCService: svc, resume: func(ctx context.Context, req *ateapipb.ResumeActorRequest) (*ateapipb.ResumeActorResponse, error) {
				ref := req.GetActor()
				if ref.GetAtespace() != resources.GoldenActorAtespace || ref.GetName() != tmpl.GetMetadata().GetUid() {
					t.Fatalf("unexpected golden actor: %v", ref)
				}
				policy, err := svc.GetActorEgressPolicy(ctx, &ateapipb.GetActorEgressPolicyRequest{Actor: ref})
				if tt.policy == nil {
					if status.Code(err) != codes.NotFound {
						t.Fatalf("golden without default policy: %v, want NotFound", err)
					}
					return nil, stop
				}
				if err != nil {
					t.Fatalf("golden policy must exist before resume: %v", err)
				}
				want := &ateapipb.EgressPolicy{Metadata: policy.Metadata, Rules: tmpl.GetDefaultEgressPolicy().GetRules()}
				if policy.GetMetadata().GetAtespace() != resources.GoldenActorAtespace || policy.GetMetadata().GetName() != "default" || !proto.Equal(policy, want) {
					t.Fatalf("golden policy = %v, want rules %v in ate-golden", policy, want.Rules)
				}
				return nil, stop
			}}
			r := newTestTemplateReconciler(persistence, control)
			defer r.queue.ShutDown()
			if _, err := r.reconcileOne(ctx, resources.ActorTemplateRefFromActorTemplate(tmpl)); !errors.Is(err, stop) {
				t.Fatalf("reconcile = %v, want to reach resume with the policy present", err)
			}
		})
	}
}

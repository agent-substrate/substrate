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
	"github.com/agent-substrate/substrate/internal/apierror"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
)

func TestCreateActor_DoesNotInheritGoldenEgressPolicy(t *testing.T) {
	for _, tt := range []struct {
		name   string
		policy *ateapipb.EgressPolicyTemplate
	}{
		{name: "absent"},
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
				tmpl.GoldenEgressPolicy = tt.policy
			})})
			if err != nil {
				t.Fatal(err)
			}
			tmplRef := resources.ActorTemplateRefFromActorTemplate(tmpl).ToObjectRef()
			for _, name := range []string{"alpha", tmpl.GetMetadata().GetUid()} {
				actor, err := svc.CreateActor(ctx, &ateapipb.CreateActorRequest{Actor: &ateapipb.Actor{
					Metadata: &ateapipb.ResourceMetadata{Atespace: "actors", Name: name}, ActorTemplate: tmplRef,
				}})
				if err != nil {
					t.Fatal(err)
				}
				ref := resources.ActorRefFromActor(actor).ToObjectRef()
				if _, err := svc.GetActorEgressPolicy(ctx, &ateapipb.GetActorEgressPolicyRequest{Actor: ref}); apierror.Code(err) != codes.NotFound {
					t.Fatalf("ordinary actor inherited golden policy: %v, want NotFound", err)
				}
				if _, err := svc.CreateActorEgressPolicy(ctx, &ateapipb.CreateActorEgressPolicyRequest{
					Actor: ref,
					EgressPolicy: &ateapipb.EgressPolicy{
						Metadata: &ateapipb.ResourceMetadata{Atespace: "actors", Name: "default"},
						Rules:    []*ateapipb.EgressRule{{Https: &ateapipb.HTTPSRule{Hostnames: []string{"runtime.example.com"}}}},
					},
				}); err != nil {
					t.Fatalf("creating the actor's runtime policy: %v", err)
				}
			}
			gotTemplate, err := svc.GetActorTemplate(ctx, &ateapipb.GetActorTemplateRequest{ActorTemplate: tmplRef})
			if err != nil || !proto.Equal(gotTemplate, tmpl) {
				t.Fatalf("creating runtime policies changed the template: %v, %v", gotTemplate, err)
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

func TestReconcileOne_GoldenEgressPolicyBeforeResume(t *testing.T) {
	for _, tt := range []struct {
		name   string
		policy *ateapipb.EgressPolicyTemplate
	}{
		{name: "absent"},
		{name: "configured", policy: &ateapipb.EgressPolicyTemplate{Rules: []*ateapipb.EgressRule{{Https: &ateapipb.HTTPSRule{Hostnames: []string{"api.example.com"}}}}}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ctx := t.Context()
			persistence := newTestPersistence(t)
			svc := &RPCService{impl: newServiceImpl(persistence, nil), sandboxConfigLister: gvisorDefaultLister(t)}
			storetest.MustCreateAtespace(t, ctx, persistence, "templates")
			tmpl, err := svc.CreateActorTemplate(ctx, &ateapipb.CreateActorTemplateRequest{ActorTemplate: validActorTemplate(func(tmpl *ateapipb.ActorTemplate) {
				tmpl.Metadata = &ateapipb.ResourceMetadata{Atespace: "templates", Name: "tmpl"}
				tmpl.GoldenEgressPolicy = tt.policy
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
					if apierror.Code(err) != codes.NotFound {
						t.Fatalf("golden without configured policy: %v, want NotFound", err)
					}
					return nil, stop
				}
				if err != nil {
					t.Fatalf("golden policy must exist before resume: %v", err)
				}
				want := &ateapipb.EgressPolicy{Metadata: policy.Metadata, Rules: tmpl.GetGoldenEgressPolicy().GetRules()}
				if policy.GetMetadata().GetAtespace() != resources.GoldenActorAtespace || policy.GetMetadata().GetName() != "default" || !proto.Equal(policy, want) {
					t.Fatalf("golden policy = %v, want rules %v in ate-golden", policy, want.Rules)
				}
				if md := policy.GetMetadata(); md.GetUid() == "" || md.GetVersion() != 1 || md.GetCreateTime() == nil || md.GetUpdateTime() == nil {
					t.Errorf("unexpected policy metadata: %v", md)
				}
				return nil, stop
			}}
			r := newTestTemplateReconciler(persistence, control)
			defer r.queue.ShutDown()
			for range 2 {
				if _, err := r.reconcileOne(ctx, resources.ActorTemplateRefFromActorTemplate(tmpl)); !errors.Is(err, stop) {
					t.Fatalf("reconcile = %v, want to reach resume with the policy present", err)
				}
			}
		})
	}
}

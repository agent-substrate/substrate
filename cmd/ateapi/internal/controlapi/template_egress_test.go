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
	"github.com/agent-substrate/substrate/internal/preview"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
)

func TestCreateActor_DoesNotInheritGoldenEgressPolicy(t *testing.T) {
	preview.InitForTest(t, "GoldenEgressPolicy")
	for _, tt := range []struct {
		name   string
		policy *ateapipb.EgressPolicyTemplate
	}{
		{name: "absent"},
		{name: "configured", policy: &ateapipb.EgressPolicyTemplate{Rules: []*ateapipb.EgressRule{
			{Http: &ateapipb.HttpRule{Hostnames: []string{"api.example.com"}}},
			{Https: &ateapipb.HttpsRule{Hostnames: []string{"api.example.com"}}},
		}}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ctx := t.Context()
			persistence := newTestPersistence(t)
			svc := &RPCService{impl: newServiceImpl(persistence, nil), sandboxConfigLister: gvisorDefaultLister(t)}
			storetest.MustCreateAtespace(t, ctx, persistence, "templates")
			storetest.MustCreateAtespace(t, ctx, persistence, "actors")
			storetest.MustCreateAtespace(t, ctx, persistence, resources.GoldenActorAtespace)
			tmpl, err := svc.CreateActorTemplate(ctx, &ateapipb.CreateActorTemplateRequest{ActorTemplate: validActorTemplate(func(tmpl *ateapipb.ActorTemplate) {
				tmpl.Metadata = &ateapipb.ResourceMetadata{Atespace: "templates", Name: "tmpl"}
				tmpl.GoldenEgressPolicy = tt.policy
			})})
			if err != nil {
				t.Fatal(err)
			}
			tmplRef := resources.ActorTemplateRefFromActorTemplate(tmpl).ToObjectRef()
			// Existing templates remain usable without the preview gate.
			preview.InitForTest(t)
			for _, target := range []resources.ActorRef{
				{Atespace: "actors", Name: "alpha"},
				{Atespace: "actors", Name: tmpl.GetMetadata().GetUid()},
				{Atespace: resources.GoldenActorAtespace, Name: tmpl.GetMetadata().GetUid()},
			} {
				actor, err := svc.CreateActor(ctx, &ateapipb.CreateActorRequest{Actor: &ateapipb.Actor{
					Metadata: &ateapipb.ResourceMetadata{Atespace: target.Atespace, Name: target.Name}, ActorTemplate: tmplRef,
				}})
				if err != nil {
					t.Fatal(err)
				}
				ref := resources.ActorRefFromActor(actor).ToObjectRef()
				if _, err := svc.GetActorEgressPolicy(ctx, &ateapipb.GetActorEgressPolicyRequest{Actor: ref, Name: resources.SingletonName}); apierror.Code(err) != codes.NotFound {
					t.Fatalf("actor %v inherited golden policy: %v, want NotFound", target, err)
				}
				if _, err := svc.CreateActorEgressPolicy(ctx, &ateapipb.CreateActorEgressPolicyRequest{
					Actor: ref,
					EgressPolicy: &ateapipb.EgressPolicy{
						Metadata: &ateapipb.ResourceMetadata{Atespace: target.Atespace, Name: resources.SingletonName},
						Rules:    []*ateapipb.EgressRule{{Https: &ateapipb.HttpsRule{Hostnames: []string{"runtime.example.com"}}}},
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
	preview.InitForTest(t, "GoldenEgressPolicy")
	for _, tt := range []struct {
		name   string
		policy *ateapipb.EgressPolicyTemplate
	}{
		{name: "absent"},
		{name: "configured", policy: &ateapipb.EgressPolicyTemplate{Rules: []*ateapipb.EgressRule{{Https: &ateapipb.HttpsRule{Hostnames: []string{"api.example.com"}}}}}},
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
				policy, err := svc.GetActorEgressPolicy(ctx, &ateapipb.GetActorEgressPolicyRequest{Actor: ref, Name: resources.SingletonName})
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
			// Turning the gate off must not drop an existing template's policy.
			preview.InitForTest(t)
			for range 2 {
				if _, err := r.reconcileOne(ctx, resources.ActorTemplateRefFromActorTemplate(tmpl)); !errors.Is(err, stop) {
					t.Fatalf("reconcile = %v, want to reach resume with the policy present", err)
				}
			}
		})
	}
}

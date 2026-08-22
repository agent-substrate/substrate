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
	"testing"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store/storetest"
	"github.com/agent-substrate/substrate/internal/resources"
	atev1alpha1 "github.com/agent-substrate/substrate/pkg/api/v1alpha1"
	listersv1alpha1 "github.com/agent-substrate/substrate/pkg/client/listers/api/v1alpha1"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/testing/protocmp"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/client-go/tools/cache"
)

type createActorErrorStore struct {
	serviceStore
	err error
}

func (s *createActorErrorStore) CreateActor(context.Context, *ateapipb.Actor) (*ateapipb.Actor, error) {
	return nil, s.err
}

func TestCreateActor_AtespaceDeletedAfterPrecheck(t *testing.T) {
	const ns = "ns-create-atespace-race"
	indexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{})
	if err := indexer.Add(&atev1alpha1.ActorTemplate{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "tmpl1"}}); err != nil {
		t.Fatalf("add ActorTemplate: %v", err)
	}
	s := &RPCService{
		persistence:         &createActorErrorStore{err: store.ErrFailedPrecondition},
		actorTemplateLister: listersv1alpha1.NewActorTemplateLister(indexer),
	}

	_, err := s.CreateActor(context.Background(), &ateapipb.CreateActorRequest{Actor: &ateapipb.Actor{
		Metadata:               &ateapipb.ResourceMetadata{Atespace: "team-a", Name: "racing-create"},
		ActorTemplateNamespace: ns,
		ActorTemplateName:      "tmpl1",
	}})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("CreateActor status = %v, want FailedPrecondition (error: %v)", status.Code(err), err)
	}
}

func TestValidateCreateActorRequest(t *testing.T) {
	validActor := func(mutate func(*ateapipb.Actor)) *ateapipb.CreateActorRequest {
		a := &ateapipb.Actor{
			Metadata:               &ateapipb.ResourceMetadata{Atespace: "ns1", Name: "id1"},
			ActorTemplateNamespace: "ns1",
			ActorTemplateName:      "tmpl1",
		}
		if mutate != nil {
			mutate(a)
		}
		return &ateapipb.CreateActorRequest{Actor: a}
	}

	tests := []struct {
		name string
		req  *ateapipb.CreateActorRequest
		want field.ErrorList
	}{{
		"valid",
		validActor(nil),
		nil,
	}, {
		"missing actor",
		&ateapipb.CreateActorRequest{},
		field.ErrorList{field.Required(field.NewPath("actor"), "")},
	}, {
		"missing actor.metadata.atespace",
		validActor(func(a *ateapipb.Actor) { a.Metadata.Atespace = "" }),
		field.ErrorList{field.Required(field.NewPath("actor", "metadata", "atespace"), "")},
	}, {
		"invalid actor.metadata.atespace",
		validActor(func(a *ateapipb.Actor) { a.Metadata.Atespace = "NS1" }),
		field.ErrorList{field.Invalid(field.NewPath("actor", "metadata", "atespace"), "NS1", "")},
	}, {
		"missing actor.metadata.name",
		validActor(func(a *ateapipb.Actor) { a.Metadata.Name = "" }),
		field.ErrorList{field.Required(field.NewPath("actor", "metadata", "name"), "")},
	}, {
		"invalid actor.metadata.name",
		validActor(func(a *ateapipb.Actor) { a.Metadata.Name = "ID1" }),
		field.ErrorList{field.Invalid(field.NewPath("actor", "metadata", "name"), "ID1", "")},
	}, {
		"missing actor_template_namespace",
		validActor(func(a *ateapipb.Actor) { a.ActorTemplateNamespace = "" }),
		field.ErrorList{field.Required(field.NewPath("actor", "actor_template_namespace"), "")},
	}, {
		"invalid actor_template_namespace",
		validActor(func(a *ateapipb.Actor) { a.ActorTemplateNamespace = "invalid value" }),
		field.ErrorList{field.Invalid(field.NewPath("actor", "actor_template_namespace"), "invalid value", "")},
	}, {
		"missing actor_template_name",
		validActor(func(a *ateapipb.Actor) { a.ActorTemplateName = "" }),
		field.ErrorList{field.Required(field.NewPath("actor", "actor_template_name"), "")},
	}, {
		"invalid actor_template_name",
		validActor(func(a *ateapipb.Actor) { a.ActorTemplateName = "invalid value" }),
		field.ErrorList{field.Invalid(field.NewPath("actor", "actor_template_name"), "invalid value", "")},
	}, {
		"worker_selector with nil match_labels",
		validActor(func(a *ateapipb.Actor) { a.WorkerSelector = &ateapipb.Selector{} }),
		nil,
	}, {
		"worker_selector with empty match_labels",
		validActor(func(a *ateapipb.Actor) { a.WorkerSelector = &ateapipb.Selector{MatchLabels: map[string]string{}} }),
		nil,
	}, {
		"valid worker_selector",
		validActor(func(a *ateapipb.Actor) {
			a.WorkerSelector = &ateapipb.Selector{MatchLabels: map[string]string{"tier": "1"}}
		}),
		nil,
	}, {
		"worker_selector with exactly max match_labels",
		validActor(func(a *ateapipb.Actor) { a.WorkerSelector = &ateapipb.Selector{MatchLabels: selectorLabelsOfSize(10)} }),
		nil,
	}, {
		"invalid worker_selector label key",
		validActor(func(a *ateapipb.Actor) {
			a.WorkerSelector = &ateapipb.Selector{MatchLabels: map[string]string{"bad key!": "1"}}
		}),
		field.ErrorList{field.Invalid(field.NewPath("actor", "worker_selector", "match_labels").Key("bad key!"), "bad key!", "")},
	}, {
		"invalid worker_selector label value",
		validActor(func(a *ateapipb.Actor) {
			a.WorkerSelector = &ateapipb.Selector{MatchLabels: map[string]string{"tier": "not valid!"}}
		}),
		field.ErrorList{field.Invalid(field.NewPath("actor", "worker_selector", "match_labels").Key("tier"), "not valid!", "")},
	}, {
		"too many worker_selector.match_labels",
		validActor(func(a *ateapipb.Actor) { a.WorkerSelector = &ateapipb.Selector{MatchLabels: selectorLabelsOfSize(11)} }),
		field.ErrorList{field.TooMany(field.NewPath("actor", "worker_selector", "match_labels"), 11, 10)},
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertValidateErr(t, validateCreateActorRequest(tt.req), tt.want)
		})
	}
}

func TestValidateGetActorRequest(t *testing.T) {
	tests := []struct {
		name string
		req  *ateapipb.GetActorRequest
		want field.ErrorList
	}{{
		"valid",
		&ateapipb.GetActorRequest{Actor: &ateapipb.ObjectRef{Atespace: "ns1", Name: "id1"}},
		nil,
	}, {
		"missing actor",
		&ateapipb.GetActorRequest{},
		field.ErrorList{field.Required(field.NewPath("actor"), "")},
	}, {
		"missing actor.atespace",
		&ateapipb.GetActorRequest{Actor: &ateapipb.ObjectRef{Name: "id1"}},
		field.ErrorList{field.Required(field.NewPath("actor", "atespace"), "")},
	}, {
		"invalid actor.atespace",
		&ateapipb.GetActorRequest{Actor: &ateapipb.ObjectRef{Atespace: "NS1", Name: "id1"}},
		field.ErrorList{field.Invalid(field.NewPath("actor", "atespace"), "NS1", "")},
	}, {
		"missing actor.name",
		&ateapipb.GetActorRequest{Actor: &ateapipb.ObjectRef{Atespace: "ns1"}},
		field.ErrorList{field.Required(field.NewPath("actor", "name"), "")},
	}, {
		"invalid actor.name",
		&ateapipb.GetActorRequest{Actor: &ateapipb.ObjectRef{Atespace: "ns1", Name: "ID1"}},
		field.ErrorList{field.Invalid(field.NewPath("actor", "name"), "ID1", "")},
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertValidateErr(t, validateGetActorRequest(tt.req), tt.want)
		})
	}
}

func TestValidateListActorsRequest(t *testing.T) {
	tests := []struct {
		name string
		req  *ateapipb.ListActorsRequest
		want field.ErrorList
	}{{
		"valid, atespace scoped",
		&ateapipb.ListActorsRequest{Atespace: "ns1"},
		nil,
	}, {
		// Empty atespace means "all atespaces" (kubectl ate get actors -A).
		"valid, empty atespace means all atespaces",
		&ateapipb.ListActorsRequest{},
		nil,
	}, {
		"invalid atespace",
		&ateapipb.ListActorsRequest{Atespace: "NS1"},
		field.ErrorList{field.Invalid(field.NewPath("atespace"), "NS1", "")},
	}, {
		"valid, positive page_size",
		&ateapipb.ListActorsRequest{Atespace: "ns1", PageSize: 10},
		nil,
	}, {
		"negative page_size",
		&ateapipb.ListActorsRequest{Atespace: "ns1", PageSize: -1},
		field.ErrorList{field.Invalid(field.NewPath("page_size"), int32(-1), "")},
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertValidateErr(t, validateListActorsRequest(tt.req), tt.want)
		})
	}
}

func TestValidateUpdateActorRequest(t *testing.T) {
	tests := []struct {
		name string
		req  *ateapipb.UpdateActorRequest
		want field.ErrorList
	}{{
		"valid",
		updateActorReq(),
		nil,
	}, {
		"missing actor",
		&ateapipb.UpdateActorRequest{},
		field.ErrorList{field.Required(field.NewPath("actor"), "")},
	}, {
		"missing actor.metadata.atespace",
		updateActorReq(withMetadata(func(m *ateapipb.ResourceMetadata) { m.Atespace = "" })),
		field.ErrorList{field.Required(field.NewPath("actor", "metadata", "atespace"), "")},
	}, {
		"invalid actor.metadata.atespace",
		updateActorReq(withMetadata(func(m *ateapipb.ResourceMetadata) { m.Atespace = "NS1" })),
		field.ErrorList{field.Invalid(field.NewPath("actor", "metadata", "atespace"), "NS1", "")},
	}, {
		"missing actor.metadata.name",
		updateActorReq(withMetadata(func(m *ateapipb.ResourceMetadata) { m.Name = "" })),
		field.ErrorList{field.Required(field.NewPath("actor", "metadata", "name"), "")},
	}, {
		"invalid actor.metadata.name",
		updateActorReq(withMetadata(func(m *ateapipb.ResourceMetadata) { m.Name = "ID1" })),
		field.ErrorList{field.Invalid(field.NewPath("actor", "metadata", "name"), "ID1", "")},
	}, {
		"missing actor.metadata.uid precondition",
		updateActorReq(withMetadata(func(m *ateapipb.ResourceMetadata) { m.Uid = "" })),
		field.ErrorList{field.Required(field.NewPath("actor", "metadata", "uid"), "")},
	}, {
		"invalid actor.metadata.uid precondition",
		updateActorReq(withMetadata(func(m *ateapipb.ResourceMetadata) { m.Uid = "not-a-uuid" })),
		field.ErrorList{field.Invalid(field.NewPath("actor", "metadata", "uid"), "not-a-uuid", "")},
	}, {
		"missing actor.metadata.version precondition",
		updateActorReq(withMetadata(func(m *ateapipb.ResourceMetadata) { m.Version = 0 })),
		field.ErrorList{field.Required(field.NewPath("actor", "metadata", "version"), "")},
	}, {
		"negative actor.metadata.version precondition",
		updateActorReq(withMetadata(func(m *ateapipb.ResourceMetadata) { m.Version = -1 })),
		field.ErrorList{field.Invalid(field.NewPath("actor", "metadata", "version"), int64(-1), "")},
	}, {
		"missing actor.metadata.version and actor.metadata.uid",
		updateActorReq(withMetadata(func(m *ateapipb.ResourceMetadata) {
			m.Uid = ""
			m.Version = 0
		})),
		field.ErrorList{
			field.Required(field.NewPath("actor", "metadata", "uid"), ""),
			field.Required(field.NewPath("actor", "metadata", "version"), ""),
		},
	}, {
		"nil worker_selector",
		updateActorReq(),
		nil,
	}, {
		"valid worker_selector",
		updateActorReq(withSelector(map[string]string{"tier": "1"})),
		nil,
	}, {
		"invalid worker_selector label key",
		updateActorReq(withSelector(map[string]string{"bad key!": "1"})),
		field.ErrorList{field.Invalid(field.NewPath("actor", "worker_selector", "match_labels").Key("bad key!"), "bad key!", "")},
	}, {
		"invalid worker_selector label value",
		updateActorReq(withSelector(map[string]string{"tier": "not valid!"})),
		field.ErrorList{field.Invalid(field.NewPath("actor", "worker_selector", "match_labels").Key("tier"), "not valid!", "")},
	}, {
		"too many worker_selector.match_labels",
		updateActorReq(withSelector(selectorLabelsOfSize(11))),
		field.ErrorList{field.TooMany(field.NewPath("actor", "worker_selector", "match_labels"), 11, 10)},
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertValidateErr(t, validateUpdateActorRequest(tt.req), tt.want)
		})
	}
}

func TestUpdateActor(t *testing.T) {
	const templateNS, templateName = "ns1", "tmpl1"

	tests := []struct {
		name     string
		stored   *ateapipb.Actor
		req      *ateapipb.Actor
		want     *ateapipb.Actor
		wantCode codes.Code
	}{
		{
			name:   "sets a worker_selector the stored actor does not have",
			stored: &ateapipb.Actor{},
			req: &ateapipb.Actor{
				ActorTemplateNamespace: templateNS,
				ActorTemplateName:      templateName,
				WorkerSelector:         &ateapipb.Selector{MatchLabels: map[string]string{"tier": "paid"}},
			},
			want: &ateapipb.Actor{WorkerSelector: &ateapipb.Selector{MatchLabels: map[string]string{"tier": "paid"}}},
		},
		{
			name:   "overwrites an existing worker_selector",
			stored: &ateapipb.Actor{WorkerSelector: &ateapipb.Selector{MatchLabels: map[string]string{"tier": "free"}}},
			req: &ateapipb.Actor{
				ActorTemplateNamespace: templateNS,
				ActorTemplateName:      templateName,
				WorkerSelector:         &ateapipb.Selector{MatchLabels: map[string]string{"tier": "paid"}},
			},
			want: &ateapipb.Actor{WorkerSelector: &ateapipb.Selector{MatchLabels: map[string]string{"tier": "paid"}}},
		},
		{
			name:   "an omitted worker_selector is cleared",
			stored: &ateapipb.Actor{WorkerSelector: &ateapipb.Selector{MatchLabels: map[string]string{"tier": "free"}}},
			req: &ateapipb.Actor{
				ActorTemplateNamespace: templateNS,
				ActorTemplateName:      templateName,
			},
			want: &ateapipb.Actor{},
		},
		{
			name:   "SourceSnapshotTag immutable field is kept",
			stored: &ateapipb.Actor{SourceSnapshotTag: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "tag1"}},
			req: &ateapipb.Actor{
				ActorTemplateNamespace: templateNS,
				ActorTemplateName:      templateName,
				SourceSnapshotTag:      &ateapipb.ObjectRef{Atespace: testAtespace, Name: "tag1"},
				WorkerSelector:         &ateapipb.Selector{MatchLabels: map[string]string{"tier": "paid"}},
			},
			want: &ateapipb.Actor{
				SourceSnapshotTag: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "tag1"},
				WorkerSelector:    &ateapipb.Selector{MatchLabels: map[string]string{"tier": "paid"}},
			},
		},
		{
			name:   "changes to status in the request are ignored",
			stored: &ateapipb.Actor{},
			req: &ateapipb.Actor{
				ActorTemplateNamespace: templateNS,
				ActorTemplateName:      templateName,
				Status:                 &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_RUNNING},
			},
			want: &ateapipb.Actor{},
		},
		{
			name:   "an omitted immutable field is rejected",
			stored: &ateapipb.Actor{SourceSnapshotTag: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "tag1"}},
			req: &ateapipb.Actor{
				ActorTemplateNamespace: templateNS,
				ActorTemplateName:      templateName,
				// Omitted SourceSnapshotTag
			},
			wantCode: codes.InvalidArgument,
		},
		{
			name:   "an immutable field the request rewrites is rejected",
			stored: &ateapipb.Actor{SourceSnapshotTag: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "tag1"}},
			req: &ateapipb.Actor{
				ActorTemplateNamespace: "attacker-ns",
				ActorTemplateName:      "attacker-tmpl",
				SourceSnapshotTag:      &ateapipb.ObjectRef{Atespace: testAtespace, Name: "tag2"},
			},
			wantCode: codes.InvalidArgument,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.stored.Metadata = &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: testActorID}
			tt.stored.ActorTemplateNamespace = templateNS
			tt.stored.ActorTemplateName = templateName
			svc, created := rpcServiceWithActor(t, tt.stored)

			tt.req.Metadata = created.GetMetadata()
			updated, err := svc.UpdateActor(context.Background(), &ateapipb.UpdateActorRequest{Actor: tt.req})

			if tt.wantCode != codes.OK {
				if code := status.Code(err); code != tt.wantCode {
					t.Errorf("UpdateActor error = %v (code %v), want code %v", err, code, tt.wantCode)
				}
				return
			}
			if err != nil {
				t.Fatalf("UpdateActor failed: %v", err)
			}

			tt.want.Metadata = &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: testActorID, Version: 2}
			tt.want.ActorTemplateNamespace = templateNS
			tt.want.ActorTemplateName = templateName
			if diff := cmp.Diff(tt.want, updated, protocmp.Transform(), ignoreUID, ignoreTimestamps); diff != "" {
				t.Errorf("UpdateActor response mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

// TestUpdateActor_DeleteRecreateRace checks that an update is not applied
// if an actor was deleted and recreated during the update operation.
func TestUpdateActor_DeleteRecreateRace(t *testing.T) {
	ctx := context.Background()
	persistence, cleanup := storetest.SetupTestStore(t)
	t.Cleanup(cleanup)

	actorRef := resources.ActorRef{Atespace: testAtespace, Name: testActorID}

	atespace := &ateapipb.Atespace{Metadata: &ateapipb.ResourceMetadata{Name: actorRef.Atespace}}
	if _, err := persistence.CreateAtespace(ctx, atespace); err != nil {
		t.Fatalf("seed CreateAtespace: %v", err)
	}

	// Actor A: what the client reads, and what its uid precondition names.
	// Freshly created, so it sits at version 1.
	original, err := persistence.CreateActor(ctx, &ateapipb.Actor{
		Metadata:               &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: testActorID},
		ActorTemplateNamespace: "ns1",
		ActorTemplateName:      "tmpl1",
		Status: &ateapipb.ActorStatus{
			State:            ateapipb.ActorState_ACTOR_STATE_RUNNING,
			WorkerAssignment: &ateapipb.WorkerAssignment{WorkerPod: "pod-a"},
		},
	})
	if err != nil {
		t.Fatalf("seed CreateActor: %v", err)
	}

	// A concurrent client deletes A and recreates the same atespace/name as a
	// brand new actor B, in the window the handler used to leave open between
	// its own read and the store's WATCH.
	var recreated *ateapipb.Actor
	racing := &conflictInjectingStore{
		Interface: persistence,
		inject: func() {
			if _, err := persistence.UpdateActor(ctx, actorRef, store.PreconditionFrom(original), func(toUpdate *ateapipb.Actor) error {
				toUpdate.Status.State = ateapipb.ActorState_ACTOR_STATE_DELETING
				return nil
			}); err != nil {
				t.Fatalf("racing writer: mark deleting: %v", err)
			}
			if _, err := persistence.DeleteActor(ctx, actorRef); err != nil {
				t.Fatalf("racing writer: DeleteActor: %v", err)
			}
			recreated, err = persistence.CreateActor(ctx, &ateapipb.Actor{
				Metadata:               &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: testActorID},
				ActorTemplateNamespace: "ns1",
				ActorTemplateName:      "tmpl1",
				Status:                 &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_SUSPENDED},
			})
			if err != nil {
				t.Fatalf("racing writer: recreate CreateActor: %v", err)
			}
		},
	}
	svc := &RPCService{persistence: racing}

	// The client asserts "only update the actor with uid A".
	original.WorkerSelector = &ateapipb.Selector{MatchLabels: map[string]string{"tier": "paid"}}
	_, err = svc.UpdateActor(ctx, &ateapipb.UpdateActorRequest{Actor: original})
	if code := status.Code(err); code != codes.Aborted {
		t.Errorf("UpdateActor error = %v (code %v), want code Aborted: the actor holding uid %s was deleted mid-update",
			err, code, original.GetMetadata().GetUid())
	}

	stored, err := persistence.GetActor(ctx, actorRef)
	if err != nil {
		t.Fatalf("GetActor: %v", err)
	}
	if got, want := stored.GetMetadata().GetUid(), recreated.GetMetadata().GetUid(); got != want {
		t.Fatalf("stored uid = %s, want recreated actor's uid %s", got, want)
	}
	// The stored record must still be actor B as its creator left it. Any of A's
	// state showing up here is the clobber.
	if got := stored.GetStatus().GetState(); got != ateapipb.ActorState_ACTOR_STATE_SUSPENDED {
		t.Errorf("stored state = %v, want %v: recreated actor was overwritten with the deleted actor's state",
			got, ateapipb.ActorState_ACTOR_STATE_SUSPENDED)
	}
	if got := stored.GetStatus().GetWorkerAssignment(); got != nil {
		t.Errorf("stored worker_assignment = %v, want nil: recreated actor inherited the deleted actor's worker", got)
	}
	if got := stored.GetWorkerSelector(); got != nil {
		t.Errorf("stored worker_selector = %v, want nil: update meant for the deleted actor was applied", got)
	}
}

// TestUpdateActor_ConcurrentDisjointUpdates checks that a concurrent write is
// reported even when it touched a field the update does not. The version guards
// the whole actor, not a single field, so the server cannot know the two
// writes commute: it reports the conflict and leaves reconciling to the client.
func TestUpdateActor_ConcurrentDisjointUpdates(t *testing.T) {
	ctx := context.Background()
	persistence, cleanup := storetest.SetupTestStore(t)
	t.Cleanup(cleanup)

	actorRef := resources.ActorRef{Atespace: testAtespace, Name: testActorID}

	atespace := &ateapipb.Atespace{Metadata: &ateapipb.ResourceMetadata{Name: actorRef.Atespace}}
	if _, err := persistence.CreateAtespace(ctx, atespace); err != nil {
		t.Fatalf("seed CreateAtespace: %v", err)
	}
	original, err := persistence.CreateActor(ctx, &ateapipb.Actor{
		Metadata:               &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: testActorID},
		ActorTemplateNamespace: "ns1",
		ActorTemplateName:      "tmpl1",
		Status:                 &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_RUNNING},
	})
	if err != nil {
		t.Fatalf("seed CreateActor: %v", err)
	}

	// A suspend workflow bumps state (a field that a later update operation will not touch)
	// inside the handler's read-modify-write window.
	racing := &conflictInjectingStore{
		Interface: persistence,
		inject: func() {
			if _, err := persistence.UpdateActor(ctx, actorRef, store.PreconditionFrom(original), func(toUpdate *ateapipb.Actor) error {
				toUpdate.Status.State = ateapipb.ActorState_ACTOR_STATE_SUSPENDING
				return nil
			}); err != nil {
				t.Fatalf("racing writer: mark suspending: %v", err)
			}
		},
	}
	svc := &RPCService{persistence: racing}

	// Update operation is changing the worker_selector field, not the actor's state (like the concurrent op)
	// This update must fail: the racing update bumped the version.
	original.WorkerSelector = &ateapipb.Selector{MatchLabels: map[string]string{"tier": "paid"}}
	_, err = svc.UpdateActor(ctx, &ateapipb.UpdateActorRequest{Actor: original})
	if code := status.Code(err); code != codes.Aborted {
		t.Errorf("UpdateActor error = %v (code %v), want code Aborted: the guarded version moved under the update", err, code)
	}

	stored, err := persistence.GetActor(ctx, actorRef)
	if err != nil {
		t.Fatalf("GetActor: %v", err)
	}
	// The concurrent writer's field survives; the rejected update wrote nothing.
	if got := stored.GetWorkerSelector(); got != nil {
		t.Errorf("stored worker_selector = %v, want nil: the rejected update was applied anyway", got)
	}
	if got := stored.GetStatus().GetState(); got != ateapipb.ActorState_ACTOR_STATE_SUSPENDING {
		t.Errorf("stored state = %v, want %v: the concurrent writer's field must survive", got, ateapipb.ActorState_ACTOR_STATE_SUSPENDING)
	}
}

// updateActorReq builds a minimal valid UpdateActorRequest, then applies the
// given mutations. The metadata carries a uid and version guard because an
// update that carries neither is rejected as a blind write.
func updateActorReq(mutate ...func(*ateapipb.UpdateActorRequest)) *ateapipb.UpdateActorRequest {
	req := &ateapipb.UpdateActorRequest{
		Actor: &ateapipb.Actor{Metadata: &ateapipb.ResourceMetadata{
			Atespace: "ns1",
			Name:     "id1",
			// Well-formed uid and version to pass validation.
			Uid:     "2a5f8c1e-9b3d-4f7a-8e6c-1d0b4a7f2e93",
			Version: 7,
		}},
	}
	for _, m := range mutate {
		m(req)
	}
	return req
}

func withMetadata(mutate func(*ateapipb.ResourceMetadata)) func(*ateapipb.UpdateActorRequest) {
	return func(req *ateapipb.UpdateActorRequest) { mutate(req.GetActor().GetMetadata()) }
}

func withSelector(labels map[string]string) func(*ateapipb.UpdateActorRequest) {
	return func(req *ateapipb.UpdateActorRequest) {
		req.GetActor().WorkerSelector = &ateapipb.Selector{MatchLabels: labels}
	}
}

// rpcServiceWithActor seeds one actor in a miniredis-backed store and returns a
// RPCService over it.
func rpcServiceWithActor(t *testing.T, actor *ateapipb.Actor) (*RPCService, *ateapipb.Actor) {
	t.Helper()
	persistence, cleanup := storetest.SetupTestStore(t)
	t.Cleanup(cleanup)

	atespace := &ateapipb.Atespace{
		Metadata: &ateapipb.ResourceMetadata{Name: actor.Metadata.Atespace},
	}
	_, err := persistence.CreateAtespace(context.Background(), atespace)
	if err != nil {
		t.Fatalf("Failed to CreateAtespace: %v", err)
	}

	created, err := persistence.CreateActor(context.Background(), actor)
	if err != nil {
		t.Fatalf("Failed to CreateActor: %v", err)
	}
	return &RPCService{persistence: persistence}, created
}

func TestValidateDeleteActorRequest(t *testing.T) {
	tests := []struct {
		name string
		req  *ateapipb.DeleteActorRequest
		want field.ErrorList
	}{{
		"valid",
		&ateapipb.DeleteActorRequest{Actor: &ateapipb.ObjectRef{Atespace: "ns1", Name: "id1"}},
		nil,
	}, {
		"missing actor",
		&ateapipb.DeleteActorRequest{},
		field.ErrorList{field.Required(field.NewPath("actor"), "")},
	}, {
		"missing actor.atespace",
		&ateapipb.DeleteActorRequest{Actor: &ateapipb.ObjectRef{Name: "id1"}},
		field.ErrorList{field.Required(field.NewPath("actor", "atespace"), "")},
	}, {
		"invalid actor.atespace",
		&ateapipb.DeleteActorRequest{Actor: &ateapipb.ObjectRef{Atespace: "NS1", Name: "id1"}},
		field.ErrorList{field.Invalid(field.NewPath("actor", "atespace"), "NS1", "")},
	}, {
		"missing actor.name",
		&ateapipb.DeleteActorRequest{Actor: &ateapipb.ObjectRef{Atespace: "ns1"}},
		field.ErrorList{field.Required(field.NewPath("actor", "name"), "")},
	}, {
		"invalid actor.name",
		&ateapipb.DeleteActorRequest{Actor: &ateapipb.ObjectRef{Atespace: "ns1", Name: "ID1"}},
		field.ErrorList{field.Invalid(field.NewPath("actor", "name"), "ID1", "")},
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertValidateErr(t, validateDeleteActorRequest(tt.req), tt.want)
		})
	}
}

func TestValidatePauseActorRequest(t *testing.T) {
	tests := []struct {
		name string
		req  *ateapipb.PauseActorRequest
		want field.ErrorList
	}{{
		"valid",
		&ateapipb.PauseActorRequest{Actor: &ateapipb.ObjectRef{Atespace: "ns1", Name: "id1"}},
		nil,
	}, {
		"missing actor",
		&ateapipb.PauseActorRequest{},
		field.ErrorList{field.Required(field.NewPath("actor"), "")},
	}, {
		"missing actor.atespace",
		&ateapipb.PauseActorRequest{Actor: &ateapipb.ObjectRef{Name: "id1"}},
		field.ErrorList{field.Required(field.NewPath("actor", "atespace"), "")},
	}, {
		"invalid actor.atespace",
		&ateapipb.PauseActorRequest{Actor: &ateapipb.ObjectRef{Atespace: "NS1", Name: "id1"}},
		field.ErrorList{field.Invalid(field.NewPath("actor", "atespace"), "NS1", "")},
	}, {
		"missing actor.name",
		&ateapipb.PauseActorRequest{Actor: &ateapipb.ObjectRef{Atespace: "ns1"}},
		field.ErrorList{field.Required(field.NewPath("actor", "name"), "")},
	}, {
		"invalid actor.name",
		&ateapipb.PauseActorRequest{Actor: &ateapipb.ObjectRef{Atespace: "ns1", Name: "ID1"}},
		field.ErrorList{field.Invalid(field.NewPath("actor", "name"), "ID1", "")},
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertValidateErr(t, validatePauseActorRequest(tt.req), tt.want)
		})
	}
}

func TestValidateResumeActorRequest(t *testing.T) {
	tests := []struct {
		name string
		req  *ateapipb.ResumeActorRequest
		want field.ErrorList
	}{{
		"valid",
		&ateapipb.ResumeActorRequest{Actor: &ateapipb.ObjectRef{Atespace: "ns1", Name: "id1"}},
		nil,
	}, {
		"missing actor",
		&ateapipb.ResumeActorRequest{},
		field.ErrorList{field.Required(field.NewPath("actor"), "")},
	}, {
		"missing actor.atespace",
		&ateapipb.ResumeActorRequest{Actor: &ateapipb.ObjectRef{Name: "id1"}},
		field.ErrorList{field.Required(field.NewPath("actor", "atespace"), "")},
	}, {
		"invalid actor.atespace",
		&ateapipb.ResumeActorRequest{Actor: &ateapipb.ObjectRef{Atespace: "NS1", Name: "id1"}},
		field.ErrorList{field.Invalid(field.NewPath("actor", "atespace"), "NS1", "")},
	}, {
		"missing actor.name",
		&ateapipb.ResumeActorRequest{Actor: &ateapipb.ObjectRef{Atespace: "ns1"}},
		field.ErrorList{field.Required(field.NewPath("actor", "name"), "")},
	}, {
		"invalid actor.name",
		&ateapipb.ResumeActorRequest{Actor: &ateapipb.ObjectRef{Atespace: "ns1", Name: "ID1"}},
		field.ErrorList{field.Invalid(field.NewPath("actor", "name"), "ID1", "")},
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertValidateErr(t, validateResumeActorRequest(tt.req), tt.want)
		})
	}
}

func TestValidateSuspendActorRequest(t *testing.T) {
	tests := []struct {
		name string
		req  *ateapipb.SuspendActorRequest
		want field.ErrorList
	}{{
		"valid",
		&ateapipb.SuspendActorRequest{Actor: &ateapipb.ObjectRef{Atespace: "ns1", Name: "id1"}},
		nil,
	}, {
		"missing actor",
		&ateapipb.SuspendActorRequest{},
		field.ErrorList{field.Required(field.NewPath("actor"), "")},
	}, {
		"missing actor.atespace",
		&ateapipb.SuspendActorRequest{Actor: &ateapipb.ObjectRef{Name: "id1"}},
		field.ErrorList{field.Required(field.NewPath("actor", "atespace"), "")},
	}, {
		"invalid actor.atespace",
		&ateapipb.SuspendActorRequest{Actor: &ateapipb.ObjectRef{Atespace: "NS1", Name: "id1"}},
		field.ErrorList{field.Invalid(field.NewPath("actor", "atespace"), "NS1", "")},
	}, {
		"missing actor.name",
		&ateapipb.SuspendActorRequest{Actor: &ateapipb.ObjectRef{Atespace: "ns1"}},
		field.ErrorList{field.Required(field.NewPath("actor", "name"), "")},
	}, {
		"invalid actor.name",
		&ateapipb.SuspendActorRequest{Actor: &ateapipb.ObjectRef{Atespace: "ns1", Name: "ID1"}},
		field.ErrorList{field.Invalid(field.NewPath("actor", "name"), "ID1", "")},
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertValidateErr(t, validateSuspendActorRequest(tt.req), tt.want)
		})
	}
}

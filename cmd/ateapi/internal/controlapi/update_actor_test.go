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

	"go.opentelemetry.io/otel/attribute"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
	"k8s.io/apimachinery/pkg/util/validation/field"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store/storetest"
	"github.com/agent-substrate/substrate/internal/ateattr"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

func TestValidateUpdateActorRequest(t *testing.T) {
	mutableFields := []string{"worker_selector"}

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
		&ateapipb.UpdateActorRequest{UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"worker_selector"}}},
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
		"valid actor.metadata.uid precondition",
		updateActorReq(withMetadata(func(m *ateapipb.ResourceMetadata) {
			m.Uid = "2a5f8c1e-9b3d-4f7a-8e6c-1d0b4a7f2e93"
		})),
		nil,
	}, {
		"invalid actor.metadata.uid precondition",
		updateActorReq(withMetadata(func(m *ateapipb.ResourceMetadata) { m.Uid = "not-a-uuid" })),
		field.ErrorList{field.Invalid(field.NewPath("actor", "metadata", "uid"), "not-a-uuid", "")},
	}, {
		"valid actor.metadata.version precondition",
		updateActorReq(withMetadata(func(m *ateapipb.ResourceMetadata) { m.Version = 7 })),
		nil,
	}, {
		"negative actor.metadata.version precondition",
		updateActorReq(withMetadata(func(m *ateapipb.ResourceMetadata) { m.Version = -1 })),
		field.ErrorList{field.Invalid(field.NewPath("actor", "metadata", "version"), int64(-1), "")},
	}, {
		"missing update_mask",
		updateActorReq(func(req *ateapipb.UpdateActorRequest) { req.UpdateMask = nil }),
		field.ErrorList{field.Required(field.NewPath("update_mask"), "")},
	}, {
		"empty update_mask",
		updateActorReq(withMaskPaths()),
		field.ErrorList{field.Required(field.NewPath("update_mask"), "")},
	}, {
		"wildcard update_mask",
		updateActorReq(withMaskPaths("*")),
		field.ErrorList{field.NotSupported(field.NewPath("update_mask"), "*", mutableFields)},
	}, {
		"output-only field in update_mask",
		updateActorReq(withMaskPaths("status")),
		field.ErrorList{field.NotSupported(field.NewPath("update_mask"), "status", mutableFields)},
	}, {
		"immutable field in update_mask",
		updateActorReq(withMaskPaths("metadata.name")),
		field.ErrorList{field.NotSupported(field.NewPath("update_mask"), "metadata.name", mutableFields)},
	}, {
		"nested path in update_mask",
		updateActorReq(withMaskPaths("worker_selector.match_labels")),
		field.ErrorList{field.NotSupported(field.NewPath("update_mask"), "worker_selector.match_labels", mutableFields)},
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

// TestUpdateActor_ClearsMaskedField verifies that naming a field in the mask
// while leaving it unset on the request clears it, which is the whole point of
// requiring an explicit mask.
func TestUpdateActor_ClearsMaskedField(t *testing.T) {
	svc, _ := serviceWithActor(t, &ateapipb.Actor{
		Metadata:               &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: testActorID},
		ActorTemplateNamespace: "ns1",
		ActorTemplateName:      "tmpl1",
		WorkerSelector:         &ateapipb.Selector{MatchLabels: map[string]string{"tier": "free"}},
	})

	updated, err := svc.UpdateActor(context.Background(), &ateapipb.UpdateActorRequest{
		Actor:      &ateapipb.Actor{Metadata: &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: testActorID}},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"worker_selector"}},
	})
	if err != nil {
		t.Fatalf("UpdateActor failed: %v", err)
	}
	if got := updated.GetWorkerSelector(); got != nil {
		t.Errorf("worker_selector = %v, want nil after masked clear", got)
	}
}

func TestUpdateActor_StampsFullSpanIdentity(t *testing.T) {
	ns := namespaceForTest("ns-span-update")
	tc := setupTest(t, ns)
	defer tc.cleanup()
	createTemplate(t, tc, ns)

	if _, err := tc.service.CreateActor(context.Background(), &ateapipb.CreateActorRequest{
		Actor: &ateapipb.Actor{
			Metadata:               &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: testActorID},
			ActorTemplateNamespace: ns,
			ActorTemplateName:      "tmpl1",
		},
	}); err != nil {
		t.Fatalf("seed CreateActor: %v", err)
	}

	attrs := recordRootSpanAttrs(t, func(ctx context.Context) {
		if _, err := tc.service.UpdateActor(ctx, &ateapipb.UpdateActorRequest{
			Actor: &ateapipb.Actor{
				Metadata: &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: testActorID},
				WorkerSelector: &ateapipb.Selector{
					MatchLabels: map[string]string{"env": "prod"},
				},
			},
			UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"worker_selector"}},
		}); err != nil {
			t.Fatalf("UpdateActor: %v", err)
		}
	})

	assertSpanStr(t, attrs, ateattr.AtespaceKey, testAtespace)
	assertSpanStr(t, attrs, ateattr.ActorNameKey, testActorID)
	assertSpanStr(t, attrs, ateattr.TemplateNameKey, "tmpl1")
	assertSpanStr(t, attrs, ateattr.TemplateNamespaceKey, ns)
	if v, ok := attrs[ateattr.ActorUIDKey]; !ok || v.Type() != attribute.STRING || v.AsString() == "" {
		t.Errorf("%s = %v, want non-empty server-assigned uid", ateattr.ActorUIDKey, v.Emit())
	}
	if v, ok := attrs[ateattr.ActorVersionKey]; !ok || v.Type() != attribute.INT64 || v.AsInt64() != 2 {
		t.Errorf("%s = %v, want int64 2 (updated version)", ateattr.ActorVersionKey, v.Emit())
	}
}

func TestUpdateActor_FailedLookupStampsRefIdentityOnly(t *testing.T) {
	ns := namespaceForTest("ns-span-update-err")
	tc := setupTest(t, ns)
	defer tc.cleanup()

	attrs := recordRootSpanAttrs(t, func(ctx context.Context) {
		if _, err := tc.service.UpdateActor(ctx, &ateapipb.UpdateActorRequest{
			Actor:      &ateapipb.Actor{Metadata: &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: testActorID}},
			UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"worker_selector"}},
		}); status.Code(err) != codes.NotFound {
			t.Fatalf("UpdateActor(missing) error = %v, want code NotFound", err)
		}
	})

	assertSpanStr(t, attrs, ateattr.AtespaceKey, testAtespace)
	assertSpanStr(t, attrs, ateattr.ActorNameKey, testActorID)
	for _, k := range []attribute.Key{ateattr.ActorUIDKey, ateattr.TemplateNameKey, ateattr.TemplateNamespaceKey, ateattr.ActorVersionKey} {
		if _, ok := attrs[k]; ok {
			t.Errorf("unexpected %s on failed-update span", k)
		}
	}
}

// updateActorReq builds a minimal valid UpdateActorRequest, then applies the
// given mutations.
func updateActorReq(mutate ...func(*ateapipb.UpdateActorRequest)) *ateapipb.UpdateActorRequest {
	req := &ateapipb.UpdateActorRequest{
		Actor:      &ateapipb.Actor{Metadata: &ateapipb.ResourceMetadata{Atespace: "ns1", Name: "id1"}},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"worker_selector"}},
	}
	for _, m := range mutate {
		m(req)
	}
	return req
}

func withMetadata(mutate func(*ateapipb.ResourceMetadata)) func(*ateapipb.UpdateActorRequest) {
	return func(req *ateapipb.UpdateActorRequest) { mutate(req.GetActor().GetMetadata()) }
}

func withMaskPaths(paths ...string) func(*ateapipb.UpdateActorRequest) {
	return func(req *ateapipb.UpdateActorRequest) { req.UpdateMask = &fieldmaskpb.FieldMask{Paths: paths} }
}

func withSelector(labels map[string]string) func(*ateapipb.UpdateActorRequest) {
	return func(req *ateapipb.UpdateActorRequest) {
		req.GetActor().WorkerSelector = &ateapipb.Selector{MatchLabels: labels}
	}
}

// serviceWithActor seeds one actor in a miniredis-backed store and returns a
// Service over it.
func serviceWithActor(t *testing.T, actor *ateapipb.Actor) (*Service, *ateapipb.Actor) {
	t.Helper()
	persistence, cleanup := storetest.SetupTestStore(t)
	t.Cleanup(cleanup)

	created, err := persistence.CreateActor(context.Background(), actor)
	if err != nil {
		t.Fatalf("Failed to CreateActor: %v", err)
	}
	return &Service{persistence: persistence}, created
}

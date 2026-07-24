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
	"k8s.io/apimachinery/pkg/util/validation/field"

	"github.com/agent-substrate/substrate/internal/ateattr"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

func TestValidateUpdateActorRequest(t *testing.T) {
	tests := []struct {
		name string
		req  *ateapipb.UpdateActorRequest
		want field.ErrorList
	}{{
		"valid",
		&ateapipb.UpdateActorRequest{Actor: &ateapipb.ObjectRef{Atespace: "ns1", Name: "id1"}},
		nil,
	}, {
		"missing actor",
		&ateapipb.UpdateActorRequest{},
		field.ErrorList{field.Required(field.NewPath("actor"), "")},
	}, {
		"missing actor.atespace",
		&ateapipb.UpdateActorRequest{Actor: &ateapipb.ObjectRef{Name: "id1"}},
		field.ErrorList{field.Required(field.NewPath("actor", "atespace"), "")},
	}, {
		"invalid actor.atespace",
		&ateapipb.UpdateActorRequest{Actor: &ateapipb.ObjectRef{Atespace: "NS1", Name: "id1"}},
		field.ErrorList{field.Invalid(field.NewPath("actor", "atespace"), "NS1", "")},
	}, {
		"missing actor.name",
		&ateapipb.UpdateActorRequest{Actor: &ateapipb.ObjectRef{Atespace: "ns1"}},
		field.ErrorList{field.Required(field.NewPath("actor", "name"), "")},
	}, {
		"invalid actor.name",
		&ateapipb.UpdateActorRequest{Actor: &ateapipb.ObjectRef{Atespace: "ns1", Name: "ID1"}},
		field.ErrorList{field.Invalid(field.NewPath("actor", "name"), "ID1", "")},
	}, {
		"nil worker_selector",
		&ateapipb.UpdateActorRequest{Actor: &ateapipb.ObjectRef{Atespace: "ns1", Name: "id1"}, WorkerSelector: nil},
		nil,
	}, {
		"valid worker_selector",
		&ateapipb.UpdateActorRequest{
			Actor:          &ateapipb.ObjectRef{Atespace: "ns1", Name: "id1"},
			WorkerSelector: &ateapipb.Selector{MatchLabels: map[string]string{"tier": "1"}},
		},
		nil,
	}, {
		"invalid worker_selector label key",
		&ateapipb.UpdateActorRequest{
			Actor:          &ateapipb.ObjectRef{Atespace: "ns1", Name: "id1"},
			WorkerSelector: &ateapipb.Selector{MatchLabels: map[string]string{"bad key!": "1"}},
		},
		field.ErrorList{field.Invalid(field.NewPath("worker_selector", "match_labels").Key("bad key!"), "bad key!", "")},
	}, {
		"invalid worker_selector label value",
		&ateapipb.UpdateActorRequest{
			Actor:          &ateapipb.ObjectRef{Atespace: "ns1", Name: "id1"},
			WorkerSelector: &ateapipb.Selector{MatchLabels: map[string]string{"tier": "not valid!"}},
		},
		field.ErrorList{field.Invalid(field.NewPath("worker_selector", "match_labels").Key("tier"), "not valid!", "")},
	}, {
		"too many worker_selector.match_labels",
		&ateapipb.UpdateActorRequest{
			Actor:          &ateapipb.ObjectRef{Atespace: "ns1", Name: "id1"},
			WorkerSelector: &ateapipb.Selector{MatchLabels: selectorLabelsOfSize(11)},
		},
		field.ErrorList{field.TooMany(field.NewPath("worker_selector", "match_labels"), 11, 10)},
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertValidateErr(t, validateUpdateActorRequest(tt.req), tt.want)
		})
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
			Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: testActorID},
			WorkerSelector: &ateapipb.Selector{
				MatchLabels: map[string]string{"env": "prod"},
			},
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
			Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: testActorID},
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

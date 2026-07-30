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

	"github.com/agent-substrate/substrate/internal/ateattr"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

// Pause stamps the ref identity before resolving the Actor record, so a failed
// lookup still carries who/where; it must not invent template/version, which are
// known only once the record resolves (and stamped on success).
func TestPauseActor_FailedLookupStampsRefIdentityOnly(t *testing.T) {
	ns := namespaceForTest("ns-span-pause-err")
	tc := setupTest(t, ns)
	defer tc.cleanup()

	attrs := recordRootSpanAttrs(t, func(ctx context.Context) {
		if _, err := tc.service.PauseActor(ctx, &ateapipb.PauseActorRequest{
			Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: testActorID},
		}); status.Code(err) != codes.NotFound {
			t.Fatalf("PauseActor(missing) error = %v, want code NotFound", err)
		}
	})

	assertSpanStr(t, attrs, ateattr.AtespaceKey, testAtespace)
	assertSpanStr(t, attrs, ateattr.ActorNameKey, testActorID)
	for _, k := range []attribute.Key{ateattr.ActorUIDKey, ateattr.TemplateNameKey, ateattr.TemplateNamespaceKey, ateattr.ActorVersionKey} {
		if _, ok := attrs[k]; ok {
			t.Errorf("unexpected %s on failed-pause span", k)
		}
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

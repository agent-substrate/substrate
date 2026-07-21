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

	"github.com/agent-substrate/substrate/internal/ateattr"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

// Delete addresses the actor by ref (atespace + id) and does not resolve the
// template/version, so only the ref identity is stamped.
func TestDeleteActor_StampsRefSpanIdentity(t *testing.T) {
	ns := namespaceForTest("ns-span-delete")
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
		if _, err := tc.service.DeleteActor(ctx, &ateapipb.DeleteActorRequest{
			Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: testActorID},
		}); err != nil {
			t.Fatalf("DeleteActor: %v", err)
		}
	})

	assertSpanStr(t, attrs, ateattr.AtespaceKey, testAtespace)
	assertSpanStr(t, attrs, ateattr.ActorNameKey, testActorID)
}

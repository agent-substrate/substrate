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

// The early ref stamp must land on the span even when the op fails, so a failed
// resume is still attributable to who/where.
func TestResumeActor_ErrorStillStampsRefSpanIdentity(t *testing.T) {
	ns := namespaceForTest("ns-span-resume-err")
	tc := setupTest(t, ns)
	defer tc.cleanup()

	attrs := recordRootSpanAttrs(t, func(ctx context.Context) {
		if _, err := tc.service.ResumeActor(ctx, &ateapipb.ResumeActorRequest{
			Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "missing"},
		}); err == nil {
			t.Fatal("expected error resuming missing actor")
		}
	})

	assertSpanStr(t, attrs, ateattr.AtespaceKey, testAtespace)
	assertSpanStr(t, attrs, ateattr.ActorNameKey, "missing")
}

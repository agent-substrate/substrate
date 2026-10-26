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

package authz

import (
	"context"
	"strings"
	"testing"

	"go.ate.dev/substrate/api/ateapi/v1"
)

// testWhoAmI checks the principal ateapi reports for a ServiceAccount's
// token.
func testWhoAmI(t *testing.T, ctx context.Context, f *fixture) {
	p := newPrincipal(t, ctx, f.clients, f.atespace, "whoami")
	resp, err := p.api.Authentication().WhoAmI(ctx, &ateapi.WhoAmIRequest{})
	if err != nil {
		t.Fatalf("WhoAmI: %v", err)
	}
	if want := strings.TrimPrefix(p.member, "user:"); resp.GetId() != want || resp.GetKind() != ateapi.PrincipalKind_PRINCIPAL_KIND_JWT || resp.GetIssuer() == "" {
		t.Errorf("WhoAmI = %v, want id %q, kind JWT, and an issuer", resp, want)
	}
}

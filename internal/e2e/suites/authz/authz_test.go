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

// Package authz runs against an install that enforces ateapi's runtime
// authorization with the ate-client ServiceAccount, the identity the e2e
// clients use, among its bootstrap owners, as the default install does.
//
// The unit tests in cmd/ateapi/internal/authz cover which role may make
// which call. This suite covers what only an installed system shows: a user
// granted access through an AccessPolicy using Substrate end to end, and
// the security boundaries that depend on the install, real credentials,
// stored state, and policy changes reaching every ateapi replica.
package authz

import (
	"context"
	"testing"

	"go.ate.dev/substrate/internal/e2e"
)

// TestAuthz deploys one fixture, the expensive part, and runs every check
// against it. The subtests run in order, not in parallel: each sets the
// fixture atespace's AccessPolicy for its own principals.
func TestAuthz(t *testing.T) {
	env, err := e2e.CheckEnv("BUCKET_NAME", "KO_DOCKER_REPO")
	if err != nil {
		t.Fatalf("CheckEnv failed: %v", err)
	}
	ctx := context.Background()
	f := deployFixture(t, ctx, env["BUCKET_NAME"])

	t.Run("BasicFlow", func(t *testing.T) { testBasicFlow(t, ctx, f) })
	t.Run("SecurityRegression", func(t *testing.T) { testSecurityRegression(t, ctx, f) })
}

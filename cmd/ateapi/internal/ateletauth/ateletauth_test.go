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

package ateletauth_test

import (
	"testing"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/ateletauth"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/ateletauth/ateletauthtest"
	"github.com/agent-substrate/substrate/internal/installdefaults"
)

// TestAuthenticateHonorsConfiguredIdentity checks that the identity
// Authenticate is given is the one it accepts, and that the canonical
// identity is rejected when the install lives elsewhere.
//
// The authorization table test in actoridentity covers a caller with the
// wrong identity, but it always expects the default identity, so it holds
// against a hardcoded "ate-system"/"atelet" too. Only the relocated case
// distinguishes "reads its configuration" from "happens to agree with the
// constant".
func TestAuthenticateHonorsConfiguredIdentity(t *testing.T) {
	const relocated = "substrate-test"
	const node = "test-node"
	relocatedID := installdefaults.AteletSPIFFEID(relocated)

	t.Run("accepts atelet with the configured identity", func(t *testing.T) {
		ctx := ateletauthtest.ContextWith(ateletauthtest.CertIn(t, relocated, node))
		if _, err := ateletauth.Authenticate(ctx, relocatedID); err != nil {
			t.Errorf("Authenticate() = %v, want success for an atelet in %q", err, relocated)
		}
	})

	t.Run("rejects atelet with the canonical identity", func(t *testing.T) {
		ctx := ateletauthtest.ContextWith(ateletauthtest.CertIn(t, installdefaults.SystemNamespace, node))
		if _, err := ateletauth.Authenticate(ctx, relocatedID); err == nil {
			t.Errorf("Authenticate() accepted an atelet from %q, want rejection when configured for %q",
				installdefaults.SystemNamespace, relocated)
		}
	})
}

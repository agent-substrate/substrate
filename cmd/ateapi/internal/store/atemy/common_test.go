// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package atemy

import (
	"context"
	"testing"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/authz"
)

// clearAll empties every table so the next test starts from an empty store
// without paying for a fresh database. Nothing in production mass-deletes
// state, so the statements live here rather than on Persistence.
func clearAll(t *testing.T, p *Persistence) {
	t.Helper()
	for _, stmt := range []string{
		`DELETE FROM atespaces`,
		`DELETE FROM global_access_policy`,
		`DELETE FROM atespace_access_policies`,
		`DELETE FROM actors`,
		`DELETE FROM actor_egress_policies`,
		`DELETE FROM actor_templates`,
		`DELETE FROM tags`,
		`DELETE FROM workers`,
		`DELETE FROM worker_assignments`,
		`DELETE FROM leases`,
		`DELETE FROM worker_outbox`,
		`UPDATE worker_outbox_trim SET seq = 0`,
		`DELETE FROM tuple`,
		`DELETE FROM changelog`,
	} {
		if _, err := p.db.ExecContext(context.Background(), stmt); err != nil {
			t.Fatalf("clearing tables (%s): %v", stmt, err)
		}
	}
}

func setupMySQLPersistence(t *testing.T) *Persistence {
	t.Helper()
	ctx := context.Background()
	p, err := NewPersistence(ctx, requireDB(t))
	if err != nil {
		t.Fatalf("NewPersistence failed: %v", err)
	}
	t.Cleanup(p.Close)
	clearAll(t, p)
	setTestPolicyManager(t, p)
	return p
}

// setTestPolicyManager gives p an OpenFGA-backed PolicyManager, as the server
// always does.
func setTestPolicyManager(t *testing.T, p *Persistence) {
	t.Helper()
	fgaServer, err := authz.NewOpenFGAServer(authz.MySQLBackend(p.db))
	if err != nil {
		t.Fatalf("NewOpenFGAServer failed: %v", err)
	}
	t.Cleanup(fgaServer.Close)
	_, policyManager, err := authz.New(t.Context(), authz.MySQLBackend(p.db), fgaServer, nil)
	if err != nil {
		t.Fatalf("authz.New failed: %v", err)
	}
	p.SetPolicyManager(policyManager)
}

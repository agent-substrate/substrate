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

package atemy

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/authz"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store/storesql"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/protobuf/proto"
)

// globalAccessPolicyID is the key of the global_access_policy singleton row.
const globalAccessPolicyID = 1

func (p *Persistence) CreateGlobalAccessPolicy(ctx context.Context, policy *ateapipb.AccessPolicy) (*ateapipb.AccessPolicy, error) {
	dbPolicy := proto.Clone(policy).(*ateapipb.AccessPolicy)
	dbPolicy.Metadata = &ateapipb.ResourceMetadata{Name: "default"}
	storesql.SetCreateMetadata(dbPolicy.Metadata)
	protoBytes, err := proto.Marshal(dbPolicy)
	if err != nil {
		return nil, fmt.Errorf("marshaling global access policy: %w", err)
	}
	err = inTx(ctx, p.db, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO global_access_policy (id, uid, version, proto)
			VALUES (?, ?, ?, ?)`, globalAccessPolicyID, dbPolicy.GetMetadata().GetUid(), dbPolicy.GetMetadata().GetVersion(), protoBytes); err != nil {
			if isUniqueViolation(err) {
				return store.ErrAlreadyExists
			}
			return fmt.Errorf("inserting global access policy: %w", err)
		}
		if err := p.policyManager.ReconcileGlobalBindings(ctx, authz.SQLTx(tx), dbPolicy.GetBindings()); err != nil {
			return fmt.Errorf("reconciling global access policy bindings: %w", err)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return dbPolicy, nil
}

func (p *Persistence) GetGlobalAccessPolicy(ctx context.Context) (*ateapipb.AccessPolicy, error) {
	return getAccessPolicyRow(ctx, p.db, `
		SELECT uid, version, proto FROM global_access_policy
		WHERE id = ?`, globalAccessPolicyID)
}

func (p *Persistence) UpdateGlobalAccessPolicy(ctx context.Context, precondition store.Precondition, mutate func(*ateapipb.AccessPolicy) error) (*ateapipb.AccessPolicy, error) {
	if err := precondition.Validate(); err != nil {
		return nil, err
	}
	var dbPolicy *ateapipb.AccessPolicy
	err := inTx(ctx, p.db, func(tx *sql.Tx) error {
		var err error
		dbPolicy, err = getAccessPolicyRow(ctx, tx, `
			SELECT uid, version, proto FROM global_access_policy
			WHERE id = ? FOR UPDATE`, globalAccessPolicyID)
		if err != nil {
			return err
		}
		if err := mutateAccessPolicy(dbPolicy, precondition, mutate); err != nil {
			return err
		}
		protoBytes, err := proto.Marshal(dbPolicy)
		if err != nil {
			return fmt.Errorf("marshaling updated global access policy: %w", err)
		}
		if _, err := tx.ExecContext(ctx, `
			UPDATE global_access_policy SET version = ?, proto = ?
			WHERE id = ?`, dbPolicy.GetMetadata().GetVersion(), protoBytes, globalAccessPolicyID); err != nil {
			return fmt.Errorf("updating global access policy: %w", err)
		}
		if err := p.policyManager.ReconcileGlobalBindings(ctx, authz.SQLTx(tx), dbPolicy.GetBindings()); err != nil {
			return fmt.Errorf("reconciling global access policy bindings: %w", err)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return dbPolicy, nil
}

// CreateAtespaceAccessPolicy returns ErrFailedPrecondition if the atespace
// does not exist.
func (p *Persistence) CreateAtespaceAccessPolicy(ctx context.Context, name string, policy *ateapipb.AccessPolicy) (*ateapipb.AccessPolicy, error) {
	dbPolicy := proto.Clone(policy).(*ateapipb.AccessPolicy)
	dbPolicy.Metadata = &ateapipb.ResourceMetadata{Name: "default"}
	storesql.SetCreateMetadata(dbPolicy.Metadata)
	protoBytes, err := proto.Marshal(dbPolicy)
	if err != nil {
		return nil, fmt.Errorf("marshaling access policy: %w", err)
	}
	err = inTx(ctx, p.db, func(tx *sql.Tx) error {
		if err := lockAtespace(ctx, tx, name); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO atespace_access_policies (atespace_name, uid, version, proto)
			VALUES (?, ?, ?, ?)`, name, dbPolicy.GetMetadata().GetUid(), dbPolicy.GetMetadata().GetVersion(), protoBytes); err != nil {
			if isUniqueViolation(err) {
				return store.ErrAlreadyExists
			}
			return fmt.Errorf("inserting access policy for %s: %w", name, err)
		}
		if err := p.policyManager.ReconcileAtespaceBindings(ctx, authz.SQLTx(tx), name, dbPolicy.GetBindings()); err != nil {
			return fmt.Errorf("reconciling access policy bindings for %s: %w", name, err)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return dbPolicy, nil
}

func (p *Persistence) GetAtespaceAccessPolicy(ctx context.Context, name string) (*ateapipb.AccessPolicy, error) {
	return getAccessPolicyRow(ctx, p.db, `
		SELECT uid, version, proto FROM atespace_access_policies
		WHERE atespace_name = ?`, name)
}

func (p *Persistence) UpdateAtespaceAccessPolicy(ctx context.Context, name string, precondition store.Precondition, mutate func(*ateapipb.AccessPolicy) error) (*ateapipb.AccessPolicy, error) {
	if err := precondition.Validate(); err != nil {
		return nil, err
	}
	var dbPolicy *ateapipb.AccessPolicy
	err := inTx(ctx, p.db, func(tx *sql.Tx) error {
		var err error
		dbPolicy, err = getAccessPolicyRow(ctx, tx, `
			SELECT uid, version, proto FROM atespace_access_policies
			WHERE atespace_name = ? FOR UPDATE`, name)
		if err != nil {
			return err
		}
		if err := mutateAccessPolicy(dbPolicy, precondition, mutate); err != nil {
			return err
		}
		protoBytes, err := proto.Marshal(dbPolicy)
		if err != nil {
			return fmt.Errorf("marshaling updated access policy: %w", err)
		}
		if _, err := tx.ExecContext(ctx, `
			UPDATE atespace_access_policies SET version = ?, proto = ?
			WHERE atespace_name = ?`, dbPolicy.GetMetadata().GetVersion(), protoBytes, name); err != nil {
			return fmt.Errorf("updating access policy for %s: %w", name, err)
		}
		if err := p.policyManager.ReconcileAtespaceBindings(ctx, authz.SQLTx(tx), name, dbPolicy.GetBindings()); err != nil {
			return fmt.Errorf("reconciling access policy bindings for %s: %w", name, err)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return dbPolicy, nil
}

func (p *Persistence) DeleteAtespaceAccessPolicy(ctx context.Context, name string, precondition store.DeletePreconditions) (*ateapipb.AccessPolicy, error) {
	var deleted *ateapipb.AccessPolicy
	err := inTx(ctx, p.db, func(tx *sql.Tx) error {
		uid, version, protoBytes, err := deleteLockedRow(ctx, tx, "atespace_access_policies", "atespace_name = ?", precondition, name)
		if err != nil {
			return err
		}
		if deleted, err = unmarshalAccessPolicy(uid, version, protoBytes); err != nil {
			return err
		}
		if err := p.policyManager.DeleteAtespacePolicies(ctx, authz.SQLTx(tx), name); err != nil {
			return fmt.Errorf("deleting authorization tuples for %s: %w", name, err)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return deleted, nil
}

// mutateAccessPolicy checks precondition against the locked policy, applies
// mutate, and advances the stored metadata, discarding any metadata edits.
func mutateAccessPolicy(dbPolicy *ateapipb.AccessPolicy, precondition store.Precondition, mutate func(*ateapipb.AccessPolicy) error) error {
	if err := precondition.Check(dbPolicy.GetMetadata()); err != nil {
		return err
	}
	oldMeta := proto.CloneOf(dbPolicy.Metadata)
	if err := mutate(dbPolicy); err != nil {
		return err
	}
	dbPolicy.Metadata = oldMeta
	storesql.SetUpdateMetadata(dbPolicy.Metadata, oldMeta)
	return nil
}

func getAccessPolicyRow(ctx context.Context, q querier, query string, args ...any) (*ateapipb.AccessPolicy, error) {
	var uid string
	var version int64
	var protoBytes []byte
	if err := q.QueryRowContext(ctx, query, args...).Scan(&uid, &version, &protoBytes); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, store.ErrNotFound
		}
		return nil, fmt.Errorf("getting access policy: %w", err)
	}
	return unmarshalAccessPolicy(uid, version, protoBytes)
}

func unmarshalAccessPolicy(uid string, version int64, protoBytes []byte) (*ateapipb.AccessPolicy, error) {
	policy := &ateapipb.AccessPolicy{}
	if err := storesql.UnmarshalStored(protoBytes, policy); err != nil {
		return nil, fmt.Errorf("unmarshaling access policy: %w", err)
	}
	if err := storesql.ValidateProtoMetadataMatchesColumns("access policy", policy.GetMetadata(), uid, version); err != nil {
		return nil, err
	}
	return policy, nil
}

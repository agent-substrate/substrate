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

package atepg

import (
	"context"
	"errors"
	"fmt"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/jackc/pgx/v5"
	"google.golang.org/protobuf/proto"
)

func (p *Persistence) CreateGlobalAccessPolicy(ctx context.Context, policy *ateapipb.AccessPolicy) (*ateapipb.AccessPolicy, error) {
	dbPolicy := proto.Clone(policy).(*ateapipb.AccessPolicy)
	setCreateMetadata(dbPolicy.Metadata)
	protoBytes, err := proto.Marshal(dbPolicy)
	if err != nil {
		return nil, fmt.Errorf("marshaling global access policy: %w", err)
	}

	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("beginning global access policy create: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck // no-op once committed

	_, err = tx.Exec(ctx, `
		INSERT INTO global_access_policy (id, uid, version, proto)
		VALUES (true, $1, $2, $3)`, dbPolicy.GetMetadata().GetUid(), dbPolicy.GetMetadata().GetVersion(), protoBytes)
	if err != nil {
		if isUniqueViolation(err) {
			return nil, store.ErrAlreadyExists
		}
		return nil, fmt.Errorf("inserting global access policy: %w", err)
	}
	if err := p.policyManager.ReconcileGlobalBindings(ctx, tx, dbPolicy.GetBindings()); err != nil {
		return nil, fmt.Errorf("reconciling global access policy bindings: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("committing global access policy create: %w", err)
	}
	return dbPolicy, nil
}

func (p *Persistence) GetGlobalAccessPolicy(ctx context.Context) (*ateapipb.AccessPolicy, error) {
	return getAccessPolicyRow(ctx, p.pool, `
		SELECT uid, version, proto FROM global_access_policy
		WHERE id = true`)
}

func (p *Persistence) UpdateGlobalAccessPolicy(ctx context.Context, precondition store.Precondition, mutate func(*ateapipb.AccessPolicy) error) (*ateapipb.AccessPolicy, error) {
	if err := precondition.Validate(); err != nil {
		return nil, err
	}

	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("beginning global access policy update: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck // no-op once committed

	dbPolicy, err := getAccessPolicyRow(ctx, tx, `
		SELECT uid, version, proto FROM global_access_policy
		WHERE id = true FOR UPDATE`)
	if err != nil {
		return nil, err
	}
	if err := precondition.Check(dbPolicy.GetMetadata()); err != nil {
		return nil, err
	}
	oldMeta := proto.CloneOf(dbPolicy.Metadata)
	if err := mutate(dbPolicy); err != nil {
		return nil, err
	}
	dbPolicy.Metadata = oldMeta
	setUpdateMetadata(dbPolicy.Metadata, oldMeta)
	protoBytes, err := proto.Marshal(dbPolicy)
	if err != nil {
		return nil, fmt.Errorf("marshaling updated global access policy: %w", err)
	}
	_, err = tx.Exec(ctx, `
		UPDATE global_access_policy SET version = $1, proto = $2
		WHERE id = true`, dbPolicy.GetMetadata().GetVersion(), protoBytes)
	if err != nil {
		return nil, fmt.Errorf("updating global access policy: %w", err)
	}
	if err := p.policyManager.ReconcileGlobalBindings(ctx, tx, dbPolicy.GetBindings()); err != nil {
		return nil, fmt.Errorf("reconciling global access policy bindings: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("committing global access policy update: %w", err)
	}
	return dbPolicy, nil
}

func (p *Persistence) CreateAtespaceAccessPolicy(ctx context.Context, name string, policy *ateapipb.AccessPolicy) (*ateapipb.AccessPolicy, error) {
	dbPolicy := proto.Clone(policy).(*ateapipb.AccessPolicy)
	setCreateMetadata(dbPolicy.Metadata)
	protoBytes, err := proto.Marshal(dbPolicy)
	if err != nil {
		return nil, fmt.Errorf("marshaling access policy: %w", err)
	}

	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("beginning access policy create for %s: %w", name, err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck // no-op once committed

	_, err = tx.Exec(ctx, `
		INSERT INTO atespace_access_policies (atespace_name, uid, version, proto)
		VALUES ($1, $2, $3, $4)`, name, dbPolicy.GetMetadata().GetUid(), dbPolicy.GetMetadata().GetVersion(), protoBytes)
	if err != nil {
		if isUniqueViolation(err) {
			return nil, store.ErrAlreadyExists
		}
		if isForeignKeyViolation(err) {
			return nil, store.ErrFailedPrecondition
		}
		return nil, fmt.Errorf("inserting access policy for %s: %w", name, err)
	}
	if err := p.policyManager.ReconcileAtespaceBindings(ctx, tx, name, dbPolicy.GetBindings()); err != nil {
		return nil, fmt.Errorf("reconciling access policy bindings for %s: %w", name, err)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("committing access policy create for %s: %w", name, err)
	}
	return dbPolicy, nil
}

func (p *Persistence) GetAtespaceAccessPolicy(ctx context.Context, name string) (*ateapipb.AccessPolicy, error) {
	return getAccessPolicyRow(ctx, p.pool, `
		SELECT uid, version, proto FROM atespace_access_policies
		WHERE atespace_name = $1`, name)
}

func (p *Persistence) UpdateAtespaceAccessPolicy(ctx context.Context, name string, precondition store.Precondition, mutate func(*ateapipb.AccessPolicy) error) (*ateapipb.AccessPolicy, error) {
	if err := precondition.Validate(); err != nil {
		return nil, err
	}

	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("beginning access policy update for %s: %w", name, err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck // no-op once committed

	dbPolicy, err := getAccessPolicyRow(ctx, tx, `
		SELECT uid, version, proto FROM atespace_access_policies
		WHERE atespace_name = $1 FOR UPDATE`, name)
	if err != nil {
		return nil, err
	}
	if err := precondition.Check(dbPolicy.GetMetadata()); err != nil {
		return nil, err
	}
	oldMeta := proto.CloneOf(dbPolicy.Metadata)
	if err := mutate(dbPolicy); err != nil {
		return nil, err
	}
	dbPolicy.Metadata = oldMeta
	setUpdateMetadata(dbPolicy.Metadata, oldMeta)
	protoBytes, err := proto.Marshal(dbPolicy)
	if err != nil {
		return nil, fmt.Errorf("marshaling updated access policy: %w", err)
	}
	_, err = tx.Exec(ctx, `
		UPDATE atespace_access_policies SET version = $1, proto = $2
		WHERE atespace_name = $3`, dbPolicy.GetMetadata().GetVersion(), protoBytes, name)
	if err != nil {
		return nil, fmt.Errorf("updating access policy for %s: %w", name, err)
	}
	if err := p.policyManager.ReconcileAtespaceBindings(ctx, tx, name, dbPolicy.GetBindings()); err != nil {
		return nil, fmt.Errorf("reconciling access policy bindings for %s: %w", name, err)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("committing access policy update for %s: %w", name, err)
	}
	return dbPolicy, nil
}

func (p *Persistence) DeleteAtespaceAccessPolicy(ctx context.Context, name string, precondition store.DeletePreconditions) (*ateapipb.AccessPolicy, error) {
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("beginning access policy delete for %s: %w", name, err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck // no-op once committed

	var version int64
	var uid string
	var protoBytes []byte
	err = tx.QueryRow(ctx, `
		DELETE FROM atespace_access_policies
		WHERE atespace_name = $1
		  AND ($2::text = '' OR uid = $2::text)
		  AND ($3::bigint = 0 OR version = $3::bigint)
		RETURNING uid, version, proto`, name, precondition.UID, precondition.Version).Scan(&uid, &version, &protoBytes)
	if errors.Is(err, pgx.ErrNoRows) {
		err := tx.QueryRow(ctx, `SELECT uid, version FROM atespace_access_policies WHERE atespace_name = $1`, name).Scan(&uid, &version)
		return nil, mapDeleteError(err, uid, version, precondition)
	}
	if err != nil {
		return nil, fmt.Errorf("deleting access policy for %s: %w", name, err)
	}
	deleted, err := unmarshalAccessPolicy(uid, version, protoBytes)
	if err != nil {
		return nil, err
	}
	if err := p.policyManager.DeleteAtespacePolicies(ctx, tx, name); err != nil {
		return nil, fmt.Errorf("deleting authorization tuples for %s: %w", name, err)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("committing access policy delete for %s: %w", name, err)
	}
	return deleted, nil
}

func getAccessPolicyRow(ctx context.Context, q querier, query string, args ...any) (*ateapipb.AccessPolicy, error) {
	var uid string
	var version int64
	var protoBytes []byte
	if err := q.QueryRow(ctx, query, args...).Scan(&uid, &version, &protoBytes); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, store.ErrNotFound
		}
		return nil, fmt.Errorf("getting access policy: %w", err)
	}
	return unmarshalAccessPolicy(uid, version, protoBytes)
}

func unmarshalAccessPolicy(uid string, version int64, protoBytes []byte) (*ateapipb.AccessPolicy, error) {
	policy := &ateapipb.AccessPolicy{}
	if err := unmarshalStored(protoBytes, policy); err != nil {
		return nil, fmt.Errorf("unmarshaling access policy: %w", err)
	}
	if err := validateProtoMetadataMatchesColumns("access policy", policy.GetMetadata(), uid, version); err != nil {
		return nil, err
	}
	return policy, nil
}

// Copyright 2026 The Agent Substrate Authors
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
	"slices"
	"strings"

	"github.com/jackc/pgx/v5"
	"go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/cmd/ateapi/internal/authz"
	"go.ate.dev/substrate/cmd/ateapi/internal/store"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func (p *Persistence) CreateExternalVolumeClassAccessPolicy(ctx context.Context, name string, policy *ateapi.AccessPolicy) (*ateapi.AccessPolicy, error) {
	dbPolicy := proto.CloneOf(policy)
	if dbPolicy == nil {
		dbPolicy = &ateapi.AccessPolicy{}
	}
	if dbPolicy.Metadata == nil {
		dbPolicy.Metadata = &ateapi.ResourceMetadata{}
	}
	setCreateMetadata(dbPolicy.Metadata)
	protoBytes, err := proto.Marshal(dbPolicy)
	if err != nil {
		return nil, fmt.Errorf("marshaling external volume class access policy: %w", err)
	}

	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("beginning external volume class access policy create for %q: %w", name, err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck // no-op once committed

	if err := verifyMemberAtespacesExist(ctx, tx, name, dbPolicy.GetBindings()); err != nil {
		return nil, err
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO external_volume_class_access_policies (external_volume_class_name, uid, version, proto)
		VALUES ($1, $2, $3, $4)`, name, dbPolicy.GetMetadata().GetUid(), dbPolicy.GetMetadata().GetVersion(), protoBytes)
	if err != nil {
		if isUniqueViolation(err) {
			return nil, store.ErrAlreadyExists
		}
		if isForeignKeyViolation(err) {
			return nil, store.ErrFailedPrecondition
		}
		return nil, fmt.Errorf("inserting external volume class access policy for %q: %w", name, err)
	}
	if err := p.policyManager.ReconcileExternalVolumeClassAtespaces(ctx, tx, name, dbPolicy.GetBindings()); err != nil {
		return nil, fmt.Errorf("reconciling external volume class access policy bindings for %q: %w", name, err)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("committing external volume class access policy create for %q: %w", name, err)
	}
	return dbPolicy, nil
}

func (p *Persistence) GetExternalVolumeClassAccessPolicy(ctx context.Context, name string) (*ateapi.AccessPolicy, error) {
	return getAccessPolicyRow(ctx, p.pool, `
		SELECT uid, version, proto FROM external_volume_class_access_policies
		WHERE external_volume_class_name = $1`, name)
}

func (p *Persistence) UpdateExternalVolumeClassAccessPolicy(ctx context.Context, name string, precondition store.Precondition, mutate func(*ateapi.AccessPolicy) error) (*ateapi.AccessPolicy, error) {
	if err := precondition.Validate(); err != nil {
		return nil, err
	}

	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("beginning external volume class access policy update for %q: %w", name, err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck // no-op once committed

	// Read unlocked first so verifyMemberAtespacesExist acquires FOR KEY SHARE
	// on atespaces before FOR UPDATE on the policy row, matching DeleteAtespace's
	// lock order (atespaces -> policy row -> tuple).
	dbPolicy, err := getAccessPolicyRow(ctx, tx, `
		SELECT uid, version, proto FROM external_volume_class_access_policies
		WHERE external_volume_class_name = $1`, name)
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
	if err := verifyMemberAtespacesExist(ctx, tx, name, dbPolicy.GetBindings()); err != nil {
		return nil, err
	}

	var lockedUID string
	var lockedVersion int64
	if err := tx.QueryRow(ctx, `
		SELECT uid, version FROM external_volume_class_access_policies
		WHERE external_volume_class_name = $1 FOR UPDATE`, name).Scan(&lockedUID, &lockedVersion); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, store.ErrNotFound
		}
		return nil, fmt.Errorf("locking external volume class access policy for %q: %w", name, err)
	}
	if err := precondition.Check(&ateapi.ResourceMetadata{Uid: lockedUID, Version: lockedVersion}); err != nil {
		return nil, err
	}

	dbPolicy.Metadata = oldMeta
	setUpdateMetadata(dbPolicy.Metadata, oldMeta)
	protoBytes, err := proto.Marshal(dbPolicy)
	if err != nil {
		return nil, fmt.Errorf("marshaling updated external volume class access policy: %w", err)
	}
	_, err = tx.Exec(ctx, `
		UPDATE external_volume_class_access_policies SET version = $1, proto = $2
		WHERE external_volume_class_name = $3`, dbPolicy.GetMetadata().GetVersion(), protoBytes, name)
	if err != nil {
		return nil, fmt.Errorf("updating external volume class access policy for %q: %w", name, err)
	}
	if err := p.policyManager.ReconcileExternalVolumeClassAtespaces(ctx, tx, name, dbPolicy.GetBindings()); err != nil {
		return nil, fmt.Errorf("reconciling external volume class access policy bindings for %q: %w", name, err)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("committing external volume class access policy update for %q: %w", name, err)
	}
	return dbPolicy, nil
}

func (p *Persistence) DeleteExternalVolumeClassAccessPolicy(ctx context.Context, name string, precondition store.DeletePreconditions) (*ateapi.AccessPolicy, error) {
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("beginning external volume class access policy delete for %q: %w", name, err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck // no-op once committed

	var version int64
	var uid string
	var protoBytes []byte
	err = tx.QueryRow(ctx, `
		DELETE FROM external_volume_class_access_policies
		WHERE external_volume_class_name = $1
		  AND ($2::text = '' OR uid = $2::text)
		  AND ($3::bigint = 0 OR version = $3::bigint)
		RETURNING uid, version, proto`, name, precondition.UID, precondition.Version).Scan(&uid, &version, &protoBytes)
	if errors.Is(err, pgx.ErrNoRows) {
		err := tx.QueryRow(ctx, `SELECT uid, version FROM external_volume_class_access_policies WHERE external_volume_class_name = $1`, name).Scan(&uid, &version)
		return nil, mapDeleteError(err, uid, version, precondition)
	}
	if err != nil {
		return nil, fmt.Errorf("deleting external volume class access policy for %q: %w", name, err)
	}
	deleted, err := unmarshalAccessPolicy(uid, version, protoBytes)
	if err != nil {
		return nil, err
	}
	if err := p.policyManager.DeleteExternalVolumeClassPolicies(ctx, tx, name); err != nil {
		return nil, fmt.Errorf("deleting authorization tuples for external volume class %q: %w", name, err)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("committing external volume class access policy delete for %q: %w", name, err)
	}
	deleted.Metadata.DeleteTime = timestamppb.Now()
	return deleted, nil
}

// verifyMemberAtespacesExist locks every referenced "atespace:<name>" member
// in atespaces with FOR KEY SHARE in sorted order. FOR KEY SHARE never blocks
// other readers or policy writes, while serializing against concurrent
// DeleteAtespace transactions so a deleting atespace cannot gain new policy
// rows or tuples mid-delete.
func verifyMemberAtespacesExist(ctx context.Context, tx pgx.Tx, className string, bindings []*ateapi.Binding) error {
	var targets []string
	var seen map[string]struct{}
	for _, b := range bindings {
		for _, rawMember := range b.GetMembers() {
			if _, err := authz.FormatAtespaceMember(rawMember); err != nil {
				return err
			}
			atespaceName := strings.TrimPrefix(rawMember, "atespace:")
			if seen == nil {
				seen = make(map[string]struct{}, len(b.GetMembers()))
			}
			if _, dup := seen[atespaceName]; dup {
				continue
			}
			seen[atespaceName] = struct{}{}
			targets = append(targets, atespaceName)
		}
	}
	if len(targets) == 0 {
		return nil
	}

	slices.Sort(targets)
	rows, err := tx.Query(ctx, `SELECT 1 FROM atespaces WHERE name = ANY($1) ORDER BY name FOR KEY SHARE`, targets)
	if err != nil {
		return fmt.Errorf("checking member atespaces for external volume class %q: %w", className, err)
	}
	found := 0
	for rows.Next() {
		found++
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return fmt.Errorf("reading member atespaces for external volume class %q: %w", className, err)
	}
	if found != len(targets) {
		return store.ErrFailedPrecondition
	}
	return nil
}

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

	"github.com/jackc/pgx/v5"
	"go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/cmd/ateapi/internal/store"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func (p *Persistence) CreateAtespace(ctx context.Context, atespace *ateapi.Atespace) (*ateapi.Atespace, error) {
	name := atespace.GetMetadata().GetName()

	dbAtespace := proto.Clone(atespace).(*ateapi.Atespace)
	setCreateMetadata(dbAtespace.Metadata)

	protoBytes, err := proto.Marshal(dbAtespace)
	if err != nil {
		return nil, fmt.Errorf("marshaling atespace: %w", err)
	}

	_, err = p.pool.Exec(ctx, `
		INSERT INTO atespaces (name, uid, version, proto)
		VALUES ($1, $2, $3, $4)`,
		name, dbAtespace.GetMetadata().GetUid(), dbAtespace.GetMetadata().GetVersion(), protoBytes)
	if err != nil {
		if isUniqueViolation(err) {
			return nil, store.ErrAlreadyExists
		}
		return nil, fmt.Errorf("inserting atespace %q: %w", name, err)
	}
	return dbAtespace, nil
}

func (p *Persistence) GetAtespace(ctx context.Context, name string) (*ateapi.Atespace, error) {
	var protoBytes []byte
	err := p.pool.QueryRow(ctx, `SELECT proto FROM atespaces WHERE name = $1`, name).Scan(&protoBytes)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, store.ErrNotFound
		}
		return nil, fmt.Errorf("getting atespace %q: %w", name, err)
	}
	out := &ateapi.Atespace{}
	if err := unmarshalStored(protoBytes, out); err != nil {
		return nil, fmt.Errorf("unmarshaling atespace: %w", err)
	}
	return out, nil
}

func (p *Persistence) ListAtespaces(ctx context.Context, opts store.ListOptions) (store.ListResponse[*ateapi.Atespace], error) {
	opts, err := store.NormalizeListOptions(opts)
	if err != nil {
		return store.ListResponse[*ateapi.Atespace]{}, err
	}
	pageSize, pageTokenStr := opts.PageSize, opts.PageToken
	token, err := decodePageToken(pageTokenStr, kindAtespace, "", 1)
	if err != nil {
		return store.ListResponse[*ateapi.Atespace]{}, err
	}
	var last *string
	if len(token.Last) > 0 {
		last = &token.Last[0]
	}

	rows, err := p.pool.Query(ctx, `
		SELECT name, proto FROM atespaces
		WHERE $1::text IS NULL OR name > $1
		ORDER BY name
		LIMIT $2`, last, int64(pageSize)+1)
	if err != nil {
		return store.ListResponse[*ateapi.Atespace]{}, fmt.Errorf("listing atespaces: %w", err)
	}
	defer rows.Close()

	var names []string
	var result []*ateapi.Atespace
	for rows.Next() {
		var name string
		var protoBytes []byte
		if err := rows.Scan(&name, &protoBytes); err != nil {
			return store.ListResponse[*ateapi.Atespace]{}, fmt.Errorf("scanning atespace row: %w", err)
		}
		a := &ateapi.Atespace{}
		if err := unmarshalRow(protoBytes, a, "atespace", name); err != nil {
			return store.ListResponse[*ateapi.Atespace]{}, err
		}
		result = append(result, a)
		names = append(names, name)
	}
	if err := rows.Err(); err != nil {
		return store.ListResponse[*ateapi.Atespace]{}, fmt.Errorf("listing atespaces: %w", err)
	}

	var nextToken string
	if len(result) > int(pageSize) {
		result = result[:pageSize]
		nextToken = encodePageToken(kindAtespace, "", []string{names[pageSize-1]})
	}
	return store.ListResponse[*ateapi.Atespace]{Items: result, NextPageToken: nextToken}, nil
}

func (p *Persistence) DeleteAtespace(ctx context.Context, name string, precondition store.DeletePreconditions) (*ateapi.Atespace, error) {
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("beginning atespace delete: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck // no-op once committed

	var protoBytes []byte
	err = tx.QueryRow(ctx, `
		DELETE FROM atespaces
		WHERE name = $1
		  AND ($2::text = '' OR uid = $2::text)
		  AND ($3::bigint = 0 OR version = $3::bigint)
		RETURNING proto`, name, precondition.UID, precondition.Version).Scan(&protoBytes)
	if err != nil {
		if isForeignKeyViolation(err) {
			return nil, store.ErrFailedPrecondition
		}
		if errors.Is(err, pgx.ErrNoRows) {
			var uid string
			var version int64
			err := tx.QueryRow(ctx, `SELECT uid, version FROM atespaces WHERE name = $1`, name).Scan(&uid, &version)
			return nil, mapDeleteError(err, uid, version, precondition)
		}
		return nil, fmt.Errorf("deleting atespace %q: %w", name, err)
	}
	if err := p.policyManager.DeleteAtespacePolicies(ctx, tx, name); err != nil {
		return nil, fmt.Errorf("deleting authorization tuples for atespace %q: %w", name, err)
	}
	classes, err := p.policyManager.ExternalVolumeClassesForAtespace(ctx, tx, name)
	if err != nil {
		return nil, fmt.Errorf("listing external volume classes for atespace %q: %w", name, err)
	}
	if len(classes) > 0 {
		if err := scrubAtespaceFromExternalVolumeClassPolicies(ctx, tx, name, classes); err != nil {
			return nil, err
		}
		if err := p.policyManager.RemoveAtespaceFromExternalVolumeClasses(ctx, tx, name); err != nil {
			return nil, fmt.Errorf("removing atespace %q from external volume class policies: %w", name, err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("committing atespace delete %q: %w", name, err)
	}
	out := &ateapi.Atespace{}
	if err := unmarshalStored(protoBytes, out); err != nil {
		return nil, fmt.Errorf("unmarshaling deleted atespace: %w", err)
	}
	out.Metadata.DeleteTime = timestamppb.Now()
	return out, nil
}

// scrubAtespaceFromExternalVolumeClassPolicies locks the affected policy rows
// in sorted class-name order before RemoveAtespaceFromExternalVolumeClasses
// locks their OpenFGA tuple rows, matching the per-class policy write lock
// order (policy row FOR UPDATE -> tuple).
func scrubAtespaceFromExternalVolumeClassPolicies(ctx context.Context, tx pgx.Tx, atespaceName string, classes []string) error {
	if len(classes) == 0 {
		return nil
	}
	rows, err := tx.Query(ctx, `
		SELECT external_volume_class_name, uid, version, proto
		FROM external_volume_class_access_policies
		WHERE external_volume_class_name = ANY($1)
		ORDER BY external_volume_class_name
		FOR UPDATE`, classes)
	if err != nil {
		return fmt.Errorf("locking external volume class access policies for atespace %q: %w", atespaceName, err)
	}
	type lockedPolicy struct {
		className string
		policy    *ateapi.AccessPolicy
	}
	var locked []lockedPolicy
	for rows.Next() {
		var className, uid string
		var version int64
		var protoBytes []byte
		if err := rows.Scan(&className, &uid, &version, &protoBytes); err != nil {
			rows.Close()
			return fmt.Errorf("scanning external volume class access policy row: %w", err)
		}
		policy, err := unmarshalAccessPolicy(uid, version, protoBytes)
		if err != nil {
			rows.Close()
			return err
		}
		locked = append(locked, lockedPolicy{className: className, policy: policy})
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return fmt.Errorf("reading external volume class access policies for atespace %q: %w", atespaceName, err)
	}

	targetMember := "atespace:" + atespaceName
	for _, item := range locked {
		if !scrubPolicyMember(item.policy, targetMember) {
			continue
		}
		oldMeta := proto.CloneOf(item.policy.Metadata)
		setUpdateMetadata(item.policy.Metadata, oldMeta)
		protoBytes, err := proto.Marshal(item.policy)
		if err != nil {
			return fmt.Errorf("marshaling scrubbed access policy for external volume class %q: %w", item.className, err)
		}
		if _, err := tx.Exec(ctx, `
			UPDATE external_volume_class_access_policies
			SET version = $1, proto = $2
			WHERE external_volume_class_name = $3`,
			item.policy.GetMetadata().GetVersion(), protoBytes, item.className); err != nil {
			return fmt.Errorf("updating scrubbed access policy for external volume class %q: %w", item.className, err)
		}
	}
	return nil
}

// scrubPolicyMember removes targetMember from every binding in policy, dropping
// any binding whose member list becomes empty so Binding.members stays
// non-empty. It reports whether policy was modified.
func scrubPolicyMember(policy *ateapi.AccessPolicy, targetMember string) bool {
	changed := false
	var keptBindings []*ateapi.Binding
	for _, b := range policy.GetBindings() {
		if !slices.Contains(b.GetMembers(), targetMember) {
			keptBindings = append(keptBindings, b)
			continue
		}
		changed = true
		keptMembers := slices.DeleteFunc(slices.Clone(b.GetMembers()), func(m string) bool {
			return m == targetMember
		})
		if len(keptMembers) > 0 {
			b.Members = keptMembers
			keptBindings = append(keptBindings, b)
		}
	}
	if changed {
		policy.Bindings = keptBindings
	}
	return changed
}

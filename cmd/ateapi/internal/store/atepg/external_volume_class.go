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

	"github.com/jackc/pgx/v5"
	"go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/cmd/ateapi/internal/store"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func (p *Persistence) CreateExternalVolumeClass(ctx context.Context, class *ateapi.ExternalVolumeClass) (*ateapi.ExternalVolumeClass, error) {
	name := class.GetMetadata().GetName()

	dbClass := proto.Clone(class).(*ateapi.ExternalVolumeClass)
	setCreateMetadata(dbClass.Metadata)

	protoBytes, err := proto.Marshal(dbClass)
	if err != nil {
		return nil, fmt.Errorf("marshaling external volume class: %w", err)
	}

	_, err = p.pool.Exec(ctx, `
		INSERT INTO external_volume_classes (name, uid, version, proto)
		VALUES ($1, $2, $3, $4)`,
		name, dbClass.GetMetadata().GetUid(), dbClass.GetMetadata().GetVersion(), protoBytes)
	if err != nil {
		if isUniqueViolation(err) {
			return nil, store.ErrAlreadyExists
		}
		return nil, fmt.Errorf("inserting external volume class %q: %w", name, err)
	}
	return dbClass, nil
}

func (p *Persistence) GetExternalVolumeClass(ctx context.Context, name string) (*ateapi.ExternalVolumeClass, error) {
	var protoBytes []byte
	err := p.pool.QueryRow(ctx, `SELECT proto FROM external_volume_classes WHERE name = $1`, name).Scan(&protoBytes)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, store.ErrNotFound
		}
		return nil, fmt.Errorf("getting external volume class %q: %w", name, err)
	}
	out := &ateapi.ExternalVolumeClass{}
	if err := unmarshalStored(protoBytes, out); err != nil {
		return nil, fmt.Errorf("unmarshaling external volume class: %w", err)
	}
	return out, nil
}

func (p *Persistence) ListExternalVolumeClasses(ctx context.Context, opts store.ListOptions) (store.ListResponse[*ateapi.ExternalVolumeClass], error) {
	opts, err := store.NormalizeListOptions(opts)
	if err != nil {
		return store.ListResponse[*ateapi.ExternalVolumeClass]{}, err
	}
	pageSize, pageTokenStr := opts.PageSize, opts.PageToken
	token, err := decodePageToken(pageTokenStr, kindExternalVolumeClass, "", 1)
	if err != nil {
		return store.ListResponse[*ateapi.ExternalVolumeClass]{}, err
	}
	var last *string
	if len(token.Last) > 0 {
		last = &token.Last[0]
	}

	rows, err := p.pool.Query(ctx, `
		SELECT name, proto FROM external_volume_classes
		WHERE $1::text IS NULL OR name > $1
		ORDER BY name
		LIMIT $2`, last, int64(pageSize)+1)
	if err != nil {
		return store.ListResponse[*ateapi.ExternalVolumeClass]{}, fmt.Errorf("listing external volume classes: %w", err)
	}
	defer rows.Close()

	var names []string
	var result []*ateapi.ExternalVolumeClass
	for rows.Next() {
		var name string
		var protoBytes []byte
		if err := rows.Scan(&name, &protoBytes); err != nil {
			return store.ListResponse[*ateapi.ExternalVolumeClass]{}, fmt.Errorf("scanning external volume class row: %w", err)
		}
		c := &ateapi.ExternalVolumeClass{}
		if err := unmarshalRow(protoBytes, c, "external volume class", name); err != nil {
			return store.ListResponse[*ateapi.ExternalVolumeClass]{}, err
		}
		result = append(result, c)
		names = append(names, name)
	}
	if err := rows.Err(); err != nil {
		return store.ListResponse[*ateapi.ExternalVolumeClass]{}, fmt.Errorf("listing external volume classes: %w", err)
	}

	var nextToken string
	if len(result) > int(pageSize) {
		result = result[:pageSize]
		nextToken = encodePageToken(kindExternalVolumeClass, "", []string{names[pageSize-1]})
	}
	return store.ListResponse[*ateapi.ExternalVolumeClass]{Items: result, NextPageToken: nextToken}, nil
}

func (p *Persistence) DeleteExternalVolumeClass(ctx context.Context, name string, precondition store.DeletePreconditions) (*ateapi.ExternalVolumeClass, error) {
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("beginning external volume class delete: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck // no-op once committed

	var protoBytes []byte
	err = tx.QueryRow(ctx, `
		DELETE FROM external_volume_classes
		WHERE name = $1
		  AND ($2::text = '' OR uid = $2::text)
		  AND ($3::bigint = 0 OR version = $3::bigint)
		RETURNING proto`, name, precondition.UID, precondition.Version).Scan(&protoBytes)
	if errors.Is(err, pgx.ErrNoRows) {
		var uid string
		var version int64
		err := tx.QueryRow(ctx, `SELECT uid, version FROM external_volume_classes WHERE name = $1`, name).Scan(&uid, &version)
		return nil, mapDeleteError(err, uid, version, precondition)
	}
	if err != nil {
		return nil, fmt.Errorf("deleting external volume class %q: %w", name, err)
	}
	if err := p.policyManager.DeleteExternalVolumeClassPolicies(ctx, tx, name); err != nil {
		return nil, fmt.Errorf("deleting authorization tuples for external volume class %q: %w", name, err)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("committing external volume class delete %q: %w", name, err)
	}
	out := &ateapi.ExternalVolumeClass{}
	if err := unmarshalStored(protoBytes, out); err != nil {
		return nil, fmt.Errorf("unmarshaling deleted external volume class: %w", err)
	}
	out.Metadata.DeleteTime = timestamppb.Now()
	return out, nil
}

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
	"go.ate.dev/substrate/internal/resources"
	"google.golang.org/protobuf/proto"
)

func (p *Persistence) GetExternalVolume(ctx context.Context, volumeRef resources.ExternalVolumeRef) (*ateapi.ExternalVolume, error) {
	atespace, name := volumeRef.Atespace, volumeRef.Name
	var protoBytes []byte
	if err := p.pool.QueryRow(ctx, `
		SELECT proto FROM external_volumes
		WHERE atespace = $1 AND name = $2`, atespace, name).Scan(&protoBytes); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, store.ErrNotFound
		}
		return nil, fmt.Errorf("getting external volume %s/%s: %w", atespace, name, err)
	}
	volume := &ateapi.ExternalVolume{}
	if err := unmarshalStored(protoBytes, volume); err != nil {
		return nil, fmt.Errorf("unmarshaling external volume: %w", err)
	}
	return volume, nil
}

func (p *Persistence) ListExternalVolumes(ctx context.Context, atespace string, opts store.ListOptions) (store.ListResponse[*ateapi.ExternalVolume], error) {
	opts, err := store.NormalizeListOptions(opts)
	if err != nil {
		return store.ListResponse[*ateapi.ExternalVolume]{}, err
	}
	var items []*ateapi.ExternalVolume
	var nextToken string
	if atespace != "" {
		items, nextToken, err = p.listExternalVolumesScoped(ctx, atespace, opts.PageSize, opts.PageToken)
	} else {
		items, nextToken, err = p.listExternalVolumesGlobal(ctx, opts.PageSize, opts.PageToken)
	}
	if err != nil {
		return store.ListResponse[*ateapi.ExternalVolume]{}, err
	}
	return store.ListResponse[*ateapi.ExternalVolume]{Items: items, NextPageToken: nextToken}, nil
}

func (p *Persistence) listExternalVolumesScoped(ctx context.Context, atespace string, pageSize int32, pageTokenStr string) ([]*ateapi.ExternalVolume, string, error) {
	token, err := decodePageToken(pageTokenStr, kindExternalVol, atespace, 1)
	if err != nil {
		return nil, "", err
	}
	var last *string
	if len(token.Last) > 0 {
		last = &token.Last[0]
	}
	rows, err := p.pool.Query(ctx, `
		SELECT name, proto FROM external_volumes
		WHERE atespace = $1 AND ($2::text IS NULL OR name > $2)
		ORDER BY name
		LIMIT $3`, atespace, last, int64(pageSize)+1)
	if err != nil {
		return nil, "", fmt.Errorf("listing external volumes in %q: %w", atespace, err)
	}
	defer rows.Close()

	var names []string
	var result []*ateapi.ExternalVolume
	for rows.Next() {
		var name string
		var protoBytes []byte
		if err := rows.Scan(&name, &protoBytes); err != nil {
			return nil, "", fmt.Errorf("scanning external volume row: %w", err)
		}
		volume := &ateapi.ExternalVolume{}
		if err := unmarshalRow(protoBytes, volume, "external volume", atespace, name); err != nil {
			return nil, "", err
		}
		result = append(result, volume)
		names = append(names, name)
	}
	if err := rows.Err(); err != nil {
		return nil, "", fmt.Errorf("listing external volumes in %q: %w", atespace, err)
	}
	var nextToken string
	if len(result) > int(pageSize) {
		result = result[:pageSize]
		nextToken = encodePageToken(kindExternalVol, atespace, []string{names[pageSize-1]})
	}
	return result, nextToken, nil
}

func (p *Persistence) listExternalVolumesGlobal(ctx context.Context, pageSize int32, pageTokenStr string) ([]*ateapi.ExternalVolume, string, error) {
	token, err := decodePageToken(pageTokenStr, kindExternalVol, "", 2)
	if err != nil {
		return nil, "", err
	}
	var lastAtespace, lastName *string
	if len(token.Last) == 2 {
		lastAtespace, lastName = &token.Last[0], &token.Last[1]
	}
	rows, err := p.pool.Query(ctx, `
		SELECT atespace, name, proto FROM external_volumes
		WHERE $1::text IS NULL OR (atespace, name) > ($1, $2)
		ORDER BY atespace, name
		LIMIT $3`, lastAtespace, lastName, int64(pageSize)+1)
	if err != nil {
		return nil, "", fmt.Errorf("listing external volumes: %w", err)
	}
	defer rows.Close()

	type key struct{ atespace, name string }
	var keys []key
	var result []*ateapi.ExternalVolume
	for rows.Next() {
		var k key
		var protoBytes []byte
		if err := rows.Scan(&k.atespace, &k.name, &protoBytes); err != nil {
			return nil, "", fmt.Errorf("scanning external volume row: %w", err)
		}
		volume := &ateapi.ExternalVolume{}
		if err := unmarshalRow(protoBytes, volume, "external volume", k.atespace, k.name); err != nil {
			return nil, "", err
		}
		result = append(result, volume)
		keys = append(keys, k)
	}
	if err := rows.Err(); err != nil {
		return nil, "", fmt.Errorf("listing external volumes: %w", err)
	}
	var nextToken string
	if len(result) > int(pageSize) {
		result = result[:pageSize]
		last := keys[pageSize-1]
		nextToken = encodePageToken(kindExternalVol, "", []string{last.atespace, last.name})
	}
	return result, nextToken, nil
}

func (p *Persistence) CreateExternalVolume(ctx context.Context, volume *ateapi.ExternalVolume) (*ateapi.ExternalVolume, error) {
	atespace := volume.GetMetadata().GetAtespace()
	name := volume.GetMetadata().GetName()
	dbVolume := proto.CloneOf(volume)
	setCreateMetadata(dbVolume.Metadata)
	protoBytes, err := proto.Marshal(dbVolume)
	if err != nil {
		return nil, fmt.Errorf("marshaling external volume: %w", err)
	}

	_, err = p.pool.Exec(ctx, `
		INSERT INTO external_volumes (atespace, name, uid, version, proto)
		VALUES ($1, $2, $3, $4, $5)`, atespace, name,
		dbVolume.GetMetadata().GetUid(), dbVolume.GetMetadata().GetVersion(), protoBytes)
	if err != nil {
		if isUniqueViolation(err) {
			return nil, store.ErrAlreadyExists
		}
		if isForeignKeyViolation(err) {
			switch pgErrConstraint(err) {
			case "external_volumes_atespace_fk":
				return nil, store.ErrFailedPrecondition
			default:
				return nil, fmt.Errorf("inserting external volume %s/%s violated unknown foreign key %q: %w", atespace, name, pgErrConstraint(err), err)
			}
		}
		return nil, fmt.Errorf("inserting external volume %s/%s: %w", atespace, name, err)
	}
	return dbVolume, nil
}

func (p *Persistence) DeleteExternalVolume(ctx context.Context, volumeRef resources.ExternalVolumeRef, precondition store.DeletePreconditions) (*ateapi.ExternalVolume, error) {
	atespace, name := volumeRef.Atespace, volumeRef.Name
	var protoBytes []byte
	err := p.pool.QueryRow(ctx, `
		DELETE FROM external_volumes
		WHERE atespace = $1 AND name = $2
		  AND ($3::text = '' OR uid = $3::text)
		  AND ($4::bigint = 0 OR version = $4::bigint)
		RETURNING proto`, atespace, name, precondition.UID, precondition.Version).Scan(&protoBytes)
	if errors.Is(err, pgx.ErrNoRows) {
		var uid string
		var version int64
		err := p.pool.QueryRow(ctx, `SELECT uid, version FROM external_volumes WHERE atespace = $1 AND name = $2`, atespace, name).Scan(&uid, &version)
		return nil, mapDeleteError(err, uid, version, precondition)
	}
	if err != nil {
		return nil, fmt.Errorf("deleting external volume %s/%s: %w", atespace, name, err)
	}
	volume := &ateapi.ExternalVolume{}
	if err := unmarshalStored(protoBytes, volume); err != nil {
		return nil, fmt.Errorf("unmarshaling deleted external volume: %w", err)
	}
	return volume, nil
}

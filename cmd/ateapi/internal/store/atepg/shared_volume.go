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
	"maps"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/jackc/pgx/v5"
	"google.golang.org/protobuf/proto"
)

func (p *Persistence) GetSharedVolume(ctx context.Context, volumeRef resources.SharedVolumeRef) (*ateapipb.SharedVolume, error) {
	atespace, name := volumeRef.Atespace, volumeRef.Name
	var protoBytes []byte
	if err := p.pool.QueryRow(ctx, `
		SELECT proto FROM shared_volumes
		WHERE atespace = $1 AND name = $2`, atespace, name).Scan(&protoBytes); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, store.ErrNotFound
		}
		return nil, fmt.Errorf("getting shared volume %s/%s: %w", atespace, name, err)
	}
	volume := &ateapipb.SharedVolume{}
	if err := unmarshalStored(protoBytes, volume); err != nil {
		return nil, fmt.Errorf("unmarshaling shared volume: %w", err)
	}
	return volume, nil
}

func (p *Persistence) ListSharedVolumes(ctx context.Context, atespace string, opts store.ListOptions) (store.ListResponse[*ateapipb.SharedVolume], error) {
	opts, err := store.NormalizeListOptions(opts)
	if err != nil {
		return store.ListResponse[*ateapipb.SharedVolume]{}, err
	}
	var items []*ateapipb.SharedVolume
	var nextToken string
	if atespace != "" {
		items, nextToken, err = p.listSharedVolumesScoped(ctx, atespace, opts.PageSize, opts.PageToken)
	} else {
		items, nextToken, err = p.listSharedVolumesGlobal(ctx, opts.PageSize, opts.PageToken)
	}
	if err != nil {
		return store.ListResponse[*ateapipb.SharedVolume]{}, err
	}
	return store.ListResponse[*ateapipb.SharedVolume]{Items: items, NextPageToken: nextToken}, nil
}

func (p *Persistence) listSharedVolumesScoped(ctx context.Context, atespace string, pageSize int32, pageTokenStr string) ([]*ateapipb.SharedVolume, string, error) {
	token, err := decodePageToken(pageTokenStr, kindExternalVol, atespace, 1)
	if err != nil {
		return nil, "", err
	}
	var last *string
	if len(token.Last) > 0 {
		last = &token.Last[0]
	}
	rows, err := p.pool.Query(ctx, `
		SELECT name, proto FROM shared_volumes
		WHERE atespace = $1 AND ($2::text IS NULL OR name > $2)
		ORDER BY name
		LIMIT $3`, atespace, last, int64(pageSize)+1)
	if err != nil {
		return nil, "", fmt.Errorf("listing shared volumes in %q: %w", atespace, err)
	}
	defer rows.Close()

	var names []string
	var result []*ateapipb.SharedVolume
	for rows.Next() {
		var name string
		var protoBytes []byte
		if err := rows.Scan(&name, &protoBytes); err != nil {
			return nil, "", fmt.Errorf("scanning shared volume row: %w", err)
		}
		volume := &ateapipb.SharedVolume{}
		if err := unmarshalStored(protoBytes, volume); err != nil {
			return nil, "", fmt.Errorf("unmarshaling shared volume: %w", err)
		}
		result = append(result, volume)
		names = append(names, name)
	}
	if err := rows.Err(); err != nil {
		return nil, "", fmt.Errorf("listing shared volumes in %q: %w", atespace, err)
	}
	var nextToken string
	if len(result) > int(pageSize) {
		result = result[:pageSize]
		nextToken = encodePageToken(kindExternalVol, atespace, []string{names[pageSize-1]})
	}
	return result, nextToken, nil
}

func (p *Persistence) listSharedVolumesGlobal(ctx context.Context, pageSize int32, pageTokenStr string) ([]*ateapipb.SharedVolume, string, error) {
	token, err := decodePageToken(pageTokenStr, kindExternalVol, "", 2)
	if err != nil {
		return nil, "", err
	}
	var lastAtespace, lastName *string
	if len(token.Last) == 2 {
		lastAtespace, lastName = &token.Last[0], &token.Last[1]
	}
	rows, err := p.pool.Query(ctx, `
		SELECT atespace, name, proto FROM shared_volumes
		WHERE $1::text IS NULL OR (atespace, name) > ($1, $2)
		ORDER BY atespace, name
		LIMIT $3`, lastAtespace, lastName, int64(pageSize)+1)
	if err != nil {
		return nil, "", fmt.Errorf("listing shared volumes: %w", err)
	}
	defer rows.Close()

	type key struct{ atespace, name string }
	var keys []key
	var result []*ateapipb.SharedVolume
	for rows.Next() {
		var k key
		var protoBytes []byte
		if err := rows.Scan(&k.atespace, &k.name, &protoBytes); err != nil {
			return nil, "", fmt.Errorf("scanning shared volume row: %w", err)
		}
		volume := &ateapipb.SharedVolume{}
		if err := unmarshalStored(protoBytes, volume); err != nil {
			return nil, "", fmt.Errorf("unmarshaling shared volume: %w", err)
		}
		result = append(result, volume)
		keys = append(keys, k)
	}
	if err := rows.Err(); err != nil {
		return nil, "", fmt.Errorf("listing shared volumes: %w", err)
	}
	var nextToken string
	if len(result) > int(pageSize) {
		result = result[:pageSize]
		last := keys[pageSize-1]
		nextToken = encodePageToken(kindExternalVol, "", []string{last.atespace, last.name})
	}
	return result, nextToken, nil
}

func (p *Persistence) CreateSharedVolume(ctx context.Context, volume *ateapipb.SharedVolume) (*ateapipb.SharedVolume, error) {
	atespace := volume.GetMetadata().GetAtespace()
	name := volume.GetMetadata().GetName()
	dbVolume := proto.CloneOf(volume)
	setCreateMetadata(dbVolume.Metadata)
	protoBytes, err := proto.Marshal(dbVolume)
	if err != nil {
		return nil, fmt.Errorf("marshaling shared volume: %w", err)
	}

	_, err = p.pool.Exec(ctx, `
		INSERT INTO shared_volumes (atespace, name, uid, version, proto)
		VALUES ($1, $2, $3, $4, $5)`, atespace, name,
		dbVolume.GetMetadata().GetUid(), dbVolume.GetMetadata().GetVersion(), protoBytes)
	if err != nil {
		if isUniqueViolation(err) {
			return nil, store.ErrAlreadyExists
		}
		if isForeignKeyViolation(err) {
			switch pgErrConstraint(err) {
			case "shared_volumes_atespace_fk":
				return nil, store.ErrFailedPrecondition
			default:
				return nil, fmt.Errorf("inserting shared volume %s/%s violated unknown foreign key %q: %w", atespace, name, pgErrConstraint(err), err)
			}
		}
		return nil, fmt.Errorf("inserting shared volume %s/%s: %w", atespace, name, err)
	}
	return dbVolume, nil
}

func validateUpdateSharedVolumeMutation(storedVolume, mutatedVolume *ateapipb.SharedVolume) error {
	if stored, mutated := storedVolume.GetMetadata().GetAtespace(), mutatedVolume.GetMetadata().GetAtespace(); stored != mutated {
		return fmt.Errorf("metadata.atespace is immutable: mutation changed it from %q to %q", stored, mutated)
	}
	if stored, mutated := storedVolume.GetMetadata().GetName(), mutatedVolume.GetMetadata().GetName(); stored != mutated {
		return fmt.Errorf("metadata.name is immutable: mutation changed it from %q to %q", stored, mutated)
	}
	if stored, mutated := storedVolume.GetAccessMode(), mutatedVolume.GetAccessMode(); stored != ateapipb.AccessMode_ACCESS_MODE_UNSPECIFIED && stored != mutated {
		return fmt.Errorf("access_mode is immutable once set: mutation changed it from %v to %v", stored, mutated)
	}
	if stored, mutated := storedVolume.GetStorageClassName(), mutatedVolume.GetStorageClassName(); stored != "" && stored != mutated {
		return fmt.Errorf("storage_class_name is immutable once set: mutation changed it from %q to %q", stored, mutated)
	}
	if stored, mutated := storedVolume.GetCapacity(), mutatedVolume.GetCapacity(); stored != "" && stored != mutated {
		return fmt.Errorf("capacity is immutable once set: mutation changed it from %q to %q", stored, mutated)
	}
	if stored, mutated := storedVolume.GetVolumeId(), mutatedVolume.GetVolumeId(); stored != "" && stored != mutated {
		return fmt.Errorf("volume_id is immutable once set: mutation changed it from %q to %q", stored, mutated)
	}
	if stored, mutated := storedVolume.GetVolumeType(), mutatedVolume.GetVolumeType(); stored != "" && stored != mutated {
		return fmt.Errorf("volume_type is immutable once set: mutation changed it from %q to %q", stored, mutated)
	}
	if stored, mutated := storedVolume.GetVolumeContext(), mutatedVolume.GetVolumeContext(); len(stored) > 0 && !maps.Equal(stored, mutated) {
		return fmt.Errorf("volume_context is immutable once set: mutation changed it from %v to %v", stored, mutated)
	}
	return nil
}

func (p *Persistence) UpdateSharedVolume(ctx context.Context, volumeRef resources.SharedVolumeRef, precondition store.Precondition, mutate func(*ateapipb.SharedVolume) error) (*ateapipb.SharedVolume, error) {
	if err := precondition.Validate(); err != nil {
		return nil, err
	}
	atespace, name := volumeRef.Atespace, volumeRef.Name
	var currentUID string
	var currentVersion int64
	var currentBytes []byte
	if err := p.pool.QueryRow(ctx, `
			SELECT uid, version, proto FROM shared_volumes
			WHERE atespace = $1 AND name = $2`, atespace, name).Scan(&currentUID, &currentVersion, &currentBytes); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, store.ErrNotFound
		}
		return nil, fmt.Errorf("getting shared volume %s/%s for update: %w", atespace, name, err)
	}

	dbVolume := &ateapipb.SharedVolume{}
	if err := unmarshalStored(currentBytes, dbVolume); err != nil {
		return nil, fmt.Errorf("unmarshaling shared volume: %w", err)
	}
	if err := validateProtoMetadataMatchesColumns(fmt.Sprintf("shared volume %s/%s", atespace, name), dbVolume.GetMetadata(), currentUID, currentVersion); err != nil {
		return nil, err
	}
	if err := precondition.Check(dbVolume.GetMetadata()); err != nil {
		return nil, err
	}
	volumeBeforeMutation := proto.CloneOf(dbVolume)
	if err := mutate(dbVolume); err != nil {
		return nil, err
	}
	if err := validateUpdateSharedVolumeMutation(volumeBeforeMutation, dbVolume); err != nil {
		return nil, fmt.Errorf("%w: %w", store.ErrImmutableField, err)
	}
	setUpdateMetadata(dbVolume.Metadata, volumeBeforeMutation.GetMetadata())

	updatedBytes, err := proto.Marshal(dbVolume)
	if err != nil {
		return nil, fmt.Errorf("marshaling shared volume: %w", err)
	}
	commandTag, err := p.pool.Exec(ctx, `
			UPDATE shared_volumes
			SET version = $1, proto = $2
			WHERE atespace = $3 AND name = $4 AND uid = $5 AND version = $6`,
		dbVolume.GetMetadata().GetVersion(), updatedBytes, atespace, name, currentUID, currentVersion)
	if err != nil {
		return nil, fmt.Errorf("updating shared volume %s/%s: %w", atespace, name, err)
	}
	if commandTag.RowsAffected() == 0 {
		return nil, store.ErrVersionConflict
	}
	if commandTag.RowsAffected() != 1 {
		return nil, fmt.Errorf("updating shared volume %s/%s affected %d rows, want 1", atespace, name, commandTag.RowsAffected())
	}
	return dbVolume, nil
}

func (p *Persistence) DeleteSharedVolume(ctx context.Context, volumeRef resources.SharedVolumeRef, precondition store.DeletePreconditions) (*ateapipb.SharedVolume, error) {
	atespace, name := volumeRef.Atespace, volumeRef.Name
	var protoBytes []byte
	err := p.pool.QueryRow(ctx, `
		DELETE FROM shared_volumes
		WHERE atespace = $1 AND name = $2
		  AND ($3::text = '' OR uid = $3::text)
		  AND ($4::bigint = 0 OR version = $4::bigint)
		RETURNING proto`, atespace, name, precondition.UID, precondition.Version).Scan(&protoBytes)
	if errors.Is(err, pgx.ErrNoRows) {
		var uid string
		var version int64
		err := p.pool.QueryRow(ctx, `SELECT uid, version FROM shared_volumes WHERE atespace = $1 AND name = $2`, atespace, name).Scan(&uid, &version)
		return nil, mapDeleteError(err, uid, version, precondition)
	}
	if err != nil {
		return nil, fmt.Errorf("deleting shared volume %s/%s: %w", atespace, name, err)
	}
	volume := &ateapipb.SharedVolume{}
	if err := unmarshalStored(protoBytes, volume); err != nil {
		return nil, fmt.Errorf("unmarshaling deleted shared volume: %w", err)
	}
	return volume, nil
}

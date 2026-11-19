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

func validateCsiDriverConfigColumns(name string, cfg *ateapi.CsiDriverConfig, uid string, version int64, driverName string) error {
	resource := fmt.Sprintf("csi driver config %q", name)
	if err := validateProtoMetadataMatchesColumns(resource, cfg.GetMetadata(), uid, version); err != nil {
		return err
	}
	if cfg.GetMetadata().GetName() != name {
		return fmt.Errorf("%s name projection %q does not match proto metadata name %q", resource, name, cfg.GetMetadata().GetName())
	}
	if cfg.GetDriverName() != driverName {
		return fmt.Errorf("%s driver_name projection %q does not match proto driver_name %q", resource, driverName, cfg.GetDriverName())
	}
	return nil
}

func (p *Persistence) CreateCsiDriverConfig(ctx context.Context, config *ateapi.CsiDriverConfig) (*ateapi.CsiDriverConfig, error) {
	dbConfig := proto.CloneOf(config)
	if dbConfig.Metadata == nil {
		dbConfig.Metadata = &ateapi.ResourceMetadata{}
	}
	name := dbConfig.GetMetadata().GetName()
	setCreateMetadata(dbConfig.Metadata)
	protoBytes, err := proto.Marshal(dbConfig)
	if err != nil {
		return nil, fmt.Errorf("marshaling csi driver config: %w", err)
	}
	_, err = p.pool.Exec(ctx, `
		INSERT INTO csi_driver_configs (name, uid, version, driver_name, proto)
		VALUES ($1, $2, $3, $4, $5)`,
		name, dbConfig.GetMetadata().GetUid(), dbConfig.GetMetadata().GetVersion(), dbConfig.GetDriverName(), protoBytes)
	if err != nil {
		if isUniqueViolation(err) {
			if pgErrConstraint(err) == "csi_driver_configs_driver_name_key" {
				return nil, fmt.Errorf("%w: driver_name %q is already registered", store.ErrAlreadyExists, dbConfig.GetDriverName())
			}
			return nil, fmt.Errorf("%w: csi driver config %q already exists", store.ErrAlreadyExists, name)
		}
		return nil, fmt.Errorf("inserting csi driver config %q: %w", name, err)
	}
	return dbConfig, nil
}

func (p *Persistence) getCsiDriverConfig(ctx context.Context, query, arg string) (*ateapi.CsiDriverConfig, error) {
	var name, uid, driverName string
	var version int64
	var protoBytes []byte
	if err := p.pool.QueryRow(ctx, query, arg).Scan(&name, &uid, &version, &driverName, &protoBytes); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, store.ErrNotFound
		}
		return nil, fmt.Errorf("getting csi driver config %q: %w", arg, err)
	}
	out := &ateapi.CsiDriverConfig{}
	if err := unmarshalStored(protoBytes, out); err != nil {
		return nil, fmt.Errorf("unmarshaling csi driver config: %w", err)
	}
	if err := validateCsiDriverConfigColumns(name, out, uid, version, driverName); err != nil {
		return nil, err
	}
	return out, nil
}

func (p *Persistence) GetCsiDriverConfig(ctx context.Context, name string) (*ateapi.CsiDriverConfig, error) {
	return p.getCsiDriverConfig(ctx, `SELECT name, uid, version, driver_name, proto FROM csi_driver_configs WHERE name = $1`, name)
}

func (p *Persistence) GetCsiDriverConfigByDriverName(ctx context.Context, driverName string) (*ateapi.CsiDriverConfig, error) {
	return p.getCsiDriverConfig(ctx, `SELECT name, uid, version, driver_name, proto FROM csi_driver_configs WHERE driver_name = $1`, driverName)
}

func (p *Persistence) ListCsiDriverConfigs(ctx context.Context, opts store.ListOptions) (store.ListResponse[*ateapi.CsiDriverConfig], error) {
	opts, err := store.NormalizeListOptions(opts)
	if err != nil {
		return store.ListResponse[*ateapi.CsiDriverConfig]{}, err
	}
	pageSize := opts.PageSize
	token, err := decodePageToken(opts.PageToken, kindCsiDriverConfig, "", 1)
	if err != nil {
		return store.ListResponse[*ateapi.CsiDriverConfig]{}, err
	}
	var last *string
	if len(token.Last) > 0 {
		last = &token.Last[0]
	}
	rows, err := p.pool.Query(ctx, `
		SELECT name, uid, version, driver_name, proto FROM csi_driver_configs
		WHERE $1::text IS NULL OR name > $1
		ORDER BY name
		LIMIT $2`, last, int64(pageSize)+1)
	if err != nil {
		return store.ListResponse[*ateapi.CsiDriverConfig]{}, fmt.Errorf("listing csi driver configs: %w", err)
	}
	defer rows.Close()

	var names []string
	var result []*ateapi.CsiDriverConfig
	for rows.Next() {
		var name, uid, driverName string
		var version int64
		var protoBytes []byte
		if err := rows.Scan(&name, &uid, &version, &driverName, &protoBytes); err != nil {
			return store.ListResponse[*ateapi.CsiDriverConfig]{}, fmt.Errorf("scanning csi driver config row: %w", err)
		}
		cfg := &ateapi.CsiDriverConfig{}
		if err := unmarshalRow(protoBytes, cfg, "csi driver config", name); err != nil {
			return store.ListResponse[*ateapi.CsiDriverConfig]{}, err
		}
		if err := validateCsiDriverConfigColumns(name, cfg, uid, version, driverName); err != nil {
			return store.ListResponse[*ateapi.CsiDriverConfig]{}, err
		}
		result = append(result, cfg)
		names = append(names, name)
	}
	if err := rows.Err(); err != nil {
		return store.ListResponse[*ateapi.CsiDriverConfig]{}, fmt.Errorf("listing csi driver configs: %w", err)
	}
	var nextToken string
	if len(result) > int(pageSize) {
		result = result[:pageSize]
		nextToken = encodePageToken(kindCsiDriverConfig, "", []string{names[pageSize-1]})
	}
	return store.ListResponse[*ateapi.CsiDriverConfig]{Items: result, NextPageToken: nextToken}, nil
}

func (p *Persistence) UpdateCsiDriverConfig(ctx context.Context, name string, precondition store.Precondition, mutate func(toUpdate *ateapi.CsiDriverConfig) error) (*ateapi.CsiDriverConfig, error) {
	if err := precondition.Validate(); err != nil {
		return nil, err
	}
	var currentUID, currentDriverName string
	var currentVersion int64
	var currentBytes []byte
	err := p.pool.QueryRow(ctx, `SELECT uid, version, driver_name, proto FROM csi_driver_configs WHERE name = $1`, name).Scan(
		&currentUID, &currentVersion, &currentDriverName, &currentBytes)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, store.ErrNotFound
		}
		return nil, fmt.Errorf("getting csi driver config %q for update: %w", name, err)
	}
	dbConfig := &ateapi.CsiDriverConfig{}
	if err := unmarshalStored(currentBytes, dbConfig); err != nil {
		return nil, fmt.Errorf("unmarshaling csi driver config %q: %w", name, err)
	}
	if err := validateCsiDriverConfigColumns(name, dbConfig, currentUID, currentVersion, currentDriverName); err != nil {
		return nil, err
	}
	if err := precondition.Check(dbConfig.GetMetadata()); err != nil {
		return nil, err
	}

	oldMeta := proto.CloneOf(dbConfig.GetMetadata())
	oldDriverName := dbConfig.GetDriverName()
	if mutate != nil {
		if err := mutate(dbConfig); err != nil {
			return nil, err
		}
	}
	if got := dbConfig.GetMetadata().GetAtespace(); got != "" {
		return nil, fmt.Errorf("%w: metadata.atespace must be empty, got %q", store.ErrImmutableField, got)
	}
	if got := dbConfig.GetMetadata().GetName(); got != name {
		return nil, fmt.Errorf("%w: metadata.name changed from %q to %q", store.ErrImmutableField, name, got)
	}
	if got := dbConfig.GetDriverName(); got != oldDriverName {
		return nil, fmt.Errorf("%w: driver_name changed from %q to %q", store.ErrImmutableField, oldDriverName, got)
	}
	setUpdateMetadata(dbConfig.Metadata, oldMeta)

	updatedBytes, err := proto.Marshal(dbConfig)
	if err != nil {
		return nil, fmt.Errorf("marshaling csi driver config: %w", err)
	}
	commandTag, err := p.pool.Exec(ctx, `
		UPDATE csi_driver_configs SET version = $1, proto = $2
		WHERE name = $3 AND uid = $4 AND version = $5`,
		dbConfig.GetMetadata().GetVersion(), updatedBytes, name, currentUID, currentVersion)
	if err != nil {
		return nil, fmt.Errorf("updating csi driver config %q: %w", name, err)
	}
	if commandTag.RowsAffected() == 0 {
		return nil, store.ErrVersionConflict
	}
	if commandTag.RowsAffected() != 1 {
		return nil, fmt.Errorf("updating csi driver config %q affected %d rows, want 1", name, commandTag.RowsAffected())
	}
	return dbConfig, nil
}

func isDriverInUseByExternalVolumeClass(ctx context.Context, tx pgx.Tx, driverName string) (bool, error) {
	var inUse bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM external_volume_classes WHERE driver_name = $1)`, driverName).Scan(&inUse); err != nil {
		return false, fmt.Errorf("checking external volume classes for driver %q: %w", driverName, err)
	}
	if inUse {
		return true, nil
	}
	// Rows written before migration 000003 or by an older replica during a
	// rolling update carry an empty driver_name projection.
	rows, err := tx.Query(ctx, `SELECT name, proto FROM external_volume_classes WHERE driver_name = ''`)
	if err != nil {
		return false, fmt.Errorf("checking legacy external volume classes for driver %q: %w", driverName, err)
	}
	defer rows.Close()
	for rows.Next() {
		var clsName string
		var protoBytes []byte
		if err := rows.Scan(&clsName, &protoBytes); err != nil {
			return false, fmt.Errorf("scanning external volume class row: %w", err)
		}
		cls := &ateapi.ExternalVolumeClass{}
		if err := unmarshalRow(protoBytes, cls, "external volume class", clsName); err != nil {
			return false, err
		}
		if cls.GetDriverName() == driverName {
			return true, nil
		}
	}
	if err := rows.Err(); err != nil {
		return false, fmt.Errorf("checking legacy external volume classes for driver %q: %w", driverName, err)
	}
	return false, nil
}

func (p *Persistence) DeleteCsiDriverConfig(ctx context.Context, name string, precondition store.DeletePreconditions) (*ateapi.CsiDriverConfig, error) {
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("beginning csi driver config delete: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck // no-op once committed

	var uid, driverName string
	var version int64
	var protoBytes []byte
	err = tx.QueryRow(ctx, `SELECT uid, version, driver_name, proto FROM csi_driver_configs WHERE name = $1 FOR UPDATE`, name).Scan(
		&uid, &version, &driverName, &protoBytes)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, store.ErrNotFound
		}
		return nil, fmt.Errorf("locking csi driver config %q for delete: %w", name, err)
	}
	deleted := &ateapi.CsiDriverConfig{}
	if err := unmarshalStored(protoBytes, deleted); err != nil {
		return nil, fmt.Errorf("unmarshaling csi driver config %q: %w", name, err)
	}
	if err := validateCsiDriverConfigColumns(name, deleted, uid, version, driverName); err != nil {
		return nil, err
	}
	if err := precondition.Check(deleted.GetMetadata()); err != nil {
		return nil, err
	}

	inUse, err := isDriverInUseByExternalVolumeClass(ctx, tx, deleted.GetDriverName())
	if err != nil {
		return nil, err
	}
	if inUse {
		return nil, fmt.Errorf("%w: csi driver config %q (driver %q) is in use by an external volume class", store.ErrFailedPrecondition, name, deleted.GetDriverName())
	}
	commandTag, err := tx.Exec(ctx, `DELETE FROM csi_driver_configs WHERE name = $1`, name)
	if err != nil {
		return nil, fmt.Errorf("deleting csi driver config %q: %w", name, err)
	}
	if commandTag.RowsAffected() != 1 {
		return nil, fmt.Errorf("deleting csi driver config %q affected %d rows, want 1", name, commandTag.RowsAffected())
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("committing csi driver config delete %q: %w", name, err)
	}
	deleted.Metadata.DeleteTime = timestamppb.Now()
	return deleted, nil
}

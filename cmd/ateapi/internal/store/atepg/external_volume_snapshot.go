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
	"go.ate.dev/substrate/cmd/ateapi/internal/apivalidation"
	"go.ate.dev/substrate/cmd/ateapi/internal/store"
	"go.ate.dev/substrate/internal/resources"
	"google.golang.org/protobuf/proto"
)

func (p *Persistence) CreateExternalVolumeSnapshot(ctx context.Context, snapshot *ateapi.ExternalVolumeSnapshot) (*ateapi.ExternalVolumeSnapshot, error) {
	atespace := snapshot.GetMetadata().GetAtespace()
	name := snapshot.GetMetadata().GetName()
	dbSnapshot := proto.CloneOf(snapshot)
	if dbSnapshot.Metadata == nil {
		dbSnapshot.Metadata = &ateapi.ResourceMetadata{}
	}
	setCreateMetadata(dbSnapshot.Metadata)
	if errs := apivalidation.ValidateExternalVolumeSnapshot(ctx, dbSnapshot); len(errs) > 0 {
		return nil, fmt.Errorf("invalid external volume snapshot %s/%s: %w", atespace, name, errs.ToAggregate())
	}
	protoBytes, err := proto.Marshal(dbSnapshot)
	if err != nil {
		return nil, fmt.Errorf("marshaling external volume snapshot: %w", err)
	}

	origin := dbSnapshot.GetOrigin().GetTag()
	tagRef := fmt.Sprintf("%s/%s", atespace, origin.GetTag().GetName())
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("beginning external volume snapshot create: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck // no-op once committed

	// Locked so the tag checked is the one the snapshot is recorded for. The
	// uid keeps a snapshot from being attributed to a later tag that reuses
	// the name.
	var tagBytes []byte
	err = tx.QueryRow(ctx, `
		SELECT proto FROM tags
		WHERE atespace = $1 AND name = $2 AND uid = $3
		FOR SHARE`, atespace, origin.GetTag().GetName(), origin.GetTagUid()).Scan(&tagBytes)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, fmt.Errorf("%w: tag %s with uid %s not found", store.ErrFailedPrecondition, tagRef, origin.GetTagUid())
	}
	if err != nil {
		return nil, fmt.Errorf("locking tag %s: %w", tagRef, err)
	}
	tag := &ateapi.Tag{}
	if err := unmarshalStored(tagBytes, tag); err != nil {
		return nil, fmt.Errorf("unmarshaling tag %s: %w", tagRef, err)
	}
	if tag.GetMetadata().GetDeleteTime() != nil {
		return nil, fmt.Errorf("%w: tag %s is being deleted", store.ErrFailedPrecondition, tagRef)
	}
	if tag.GetStatus().GetSnapshot().GetDurableSnapshot().GetState() != ateapi.SnapshotStorageState_SNAPSHOT_STORAGE_STATE_PENDING {
		return nil, fmt.Errorf("%w: tag %s has no PENDING snapshot", store.ErrFailedPrecondition, tagRef)
	}

	_, err = tx.Exec(ctx, `
		INSERT INTO external_volume_snapshots (atespace, name, uid, version, tag_name, tag_uid, proto)
		VALUES ($1, $2, $3, $4, $5, $6, $7)`, atespace, name,
		dbSnapshot.GetMetadata().GetUid(), dbSnapshot.GetMetadata().GetVersion(),
		origin.GetTag().GetName(), origin.GetTagUid(), protoBytes)
	if err != nil {
		if isUniqueViolation(err) {
			return nil, store.ErrAlreadyExists
		}
		if isForeignKeyViolation(err) {
			return nil, store.ErrFailedPrecondition
		}
		return nil, fmt.Errorf("inserting external volume snapshot %s/%s: %w", atespace, name, err)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("committing external volume snapshot create %s/%s: %w", atespace, name, err)
	}
	return dbSnapshot, nil
}

func (p *Persistence) GetExternalVolumeSnapshot(ctx context.Context, snapshotRef resources.ExternalVolumeSnapshotRef) (*ateapi.ExternalVolumeSnapshot, error) {
	atespace, name := snapshotRef.Atespace, snapshotRef.Name
	var protoBytes []byte
	if err := p.pool.QueryRow(ctx, `
		SELECT proto FROM external_volume_snapshots
		WHERE atespace = $1 AND name = $2`, atespace, name).Scan(&protoBytes); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, store.ErrNotFound
		}
		return nil, fmt.Errorf("getting external volume snapshot %s/%s: %w", atespace, name, err)
	}
	snapshot := &ateapi.ExternalVolumeSnapshot{}
	if err := unmarshalStored(protoBytes, snapshot); err != nil {
		return nil, fmt.Errorf("unmarshaling external volume snapshot: %w", err)
	}
	return snapshot, nil
}

func (p *Persistence) ListExternalVolumeSnapshots(ctx context.Context, atespace string, opts store.ListOptions) (store.ListResponse[*ateapi.ExternalVolumeSnapshot], error) {
	opts, err := store.NormalizeListOptions(opts)
	if err != nil {
		return store.ListResponse[*ateapi.ExternalVolumeSnapshot]{}, err
	}
	var items []*ateapi.ExternalVolumeSnapshot
	var nextToken string
	if atespace != "" {
		items, nextToken, err = p.listExternalVolumeSnapshotsScoped(ctx, atespace, opts.PageSize, opts.PageToken)
	} else {
		items, nextToken, err = p.listExternalVolumeSnapshotsGlobal(ctx, opts.PageSize, opts.PageToken)
	}
	if err != nil {
		return store.ListResponse[*ateapi.ExternalVolumeSnapshot]{}, err
	}
	return store.ListResponse[*ateapi.ExternalVolumeSnapshot]{Items: items, NextPageToken: nextToken}, nil
}

func (p *Persistence) listExternalVolumeSnapshotsScoped(ctx context.Context, atespace string, pageSize int32, pageTokenStr string) ([]*ateapi.ExternalVolumeSnapshot, string, error) {
	token, err := decodePageToken(pageTokenStr, kindExternalVolumeSnapshot, atespace, 1)
	if err != nil {
		return nil, "", err
	}
	var last *string
	if len(token.Last) > 0 {
		last = &token.Last[0]
	}
	rows, err := p.pool.Query(ctx, `
		SELECT name, proto FROM external_volume_snapshots
		WHERE atespace = $1 AND ($2::text IS NULL OR name > $2)
		ORDER BY name
		LIMIT $3`, atespace, last, int64(pageSize)+1)
	if err != nil {
		return nil, "", fmt.Errorf("listing external volume snapshots in %q: %w", atespace, err)
	}
	defer rows.Close()

	var names []string
	var result []*ateapi.ExternalVolumeSnapshot
	for rows.Next() {
		var name string
		var protoBytes []byte
		if err := rows.Scan(&name, &protoBytes); err != nil {
			return nil, "", fmt.Errorf("scanning external volume snapshot row: %w", err)
		}
		snapshot := &ateapi.ExternalVolumeSnapshot{}
		if err := unmarshalRow(protoBytes, snapshot, "external volume snapshot", atespace, name); err != nil {
			return nil, "", err
		}
		result = append(result, snapshot)
		names = append(names, name)
	}
	if err := rows.Err(); err != nil {
		return nil, "", fmt.Errorf("listing external volume snapshots in %q: %w", atespace, err)
	}
	var nextToken string
	if len(result) > int(pageSize) {
		result = result[:pageSize]
		nextToken = encodePageToken(kindExternalVolumeSnapshot, atespace, []string{names[pageSize-1]})
	}
	return result, nextToken, nil
}

func (p *Persistence) listExternalVolumeSnapshotsGlobal(ctx context.Context, pageSize int32, pageTokenStr string) ([]*ateapi.ExternalVolumeSnapshot, string, error) {
	token, err := decodePageToken(pageTokenStr, kindExternalVolumeSnapshot, "", 2)
	if err != nil {
		return nil, "", err
	}
	var lastAtespace, lastName *string
	if len(token.Last) == 2 {
		lastAtespace, lastName = &token.Last[0], &token.Last[1]
	}
	rows, err := p.pool.Query(ctx, `
		SELECT atespace, name, proto FROM external_volume_snapshots
		WHERE $1::text IS NULL OR (atespace, name) > ($1, $2)
		ORDER BY atespace, name
		LIMIT $3`, lastAtespace, lastName, int64(pageSize)+1)
	if err != nil {
		return nil, "", fmt.Errorf("listing external volume snapshots: %w", err)
	}
	defer rows.Close()

	type key struct{ atespace, name string }
	var keys []key
	var result []*ateapi.ExternalVolumeSnapshot
	for rows.Next() {
		var k key
		var protoBytes []byte
		if err := rows.Scan(&k.atespace, &k.name, &protoBytes); err != nil {
			return nil, "", fmt.Errorf("scanning external volume snapshot row: %w", err)
		}
		snapshot := &ateapi.ExternalVolumeSnapshot{}
		if err := unmarshalRow(protoBytes, snapshot, "external volume snapshot", k.atespace, k.name); err != nil {
			return nil, "", err
		}
		result = append(result, snapshot)
		keys = append(keys, k)
	}
	if err := rows.Err(); err != nil {
		return nil, "", fmt.Errorf("listing external volume snapshots: %w", err)
	}
	var nextToken string
	if len(result) > int(pageSize) {
		result = result[:pageSize]
		last := keys[pageSize-1]
		nextToken = encodePageToken(kindExternalVolumeSnapshot, "", []string{last.atespace, last.name})
	}
	return result, nextToken, nil
}

// validateUpdateExternalVolumeSnapshotMutation allows changes to status
// only, and only those that keep its set-once fields. Whether
// storage_snapshot_id and error_message are both set is checked on the result.
func validateUpdateExternalVolumeSnapshotMutation(stored, mutated *ateapi.ExternalVolumeSnapshot) error {
	if s, m := stored.GetMetadata().GetAtespace(), mutated.GetMetadata().GetAtespace(); s != m {
		return fmt.Errorf("metadata.atespace is immutable: mutation changed it from %q to %q", s, m)
	}
	if s, m := stored.GetMetadata().GetName(), mutated.GetMetadata().GetName(); s != m {
		return fmt.Errorf("metadata.name is immutable: mutation changed it from %q to %q", s, m)
	}
	if s, m := stored.GetVolumeType(), mutated.GetVolumeType(); s != m {
		return fmt.Errorf("volume_type is immutable: mutation changed it from %q to %q", s, m)
	}
	if s, m := stored.GetOrigin(), mutated.GetOrigin(); !proto.Equal(s, m) {
		return fmt.Errorf("origin is immutable: mutation changed it from %v to %v", s, m)
	}
	s, m := stored.GetStatus(), mutated.GetStatus()
	if s.GetStorageSnapshotId() != "" && s.GetStorageSnapshotId() != m.GetStorageSnapshotId() {
		return fmt.Errorf("status.storage_snapshot_id is immutable once set: mutation changed it from %q to %q", s.GetStorageSnapshotId(), m.GetStorageSnapshotId())
	}
	if s.GetReadyToUse() && !m.GetReadyToUse() {
		return errors.New("status.ready_to_use cannot go from true to false")
	}
	if s.GetSizeBytes() != 0 && s.GetSizeBytes() != m.GetSizeBytes() {
		return fmt.Errorf("status.size_bytes is immutable once set: mutation changed it from %d to %d", s.GetSizeBytes(), m.GetSizeBytes())
	}
	// A later failure may replace the error, but only the write that records
	// a handle may clear it.
	recovered := s.GetStorageSnapshotId() == "" && m.GetStorageSnapshotId() != ""
	if s.GetErrorMessage() != "" && m.GetErrorMessage() == "" && !recovered {
		return fmt.Errorf("status.error_message can only be cleared by recording storage_snapshot_id: mutation cleared %q", s.GetErrorMessage())
	}
	return nil
}

func (p *Persistence) UpdateExternalVolumeSnapshot(ctx context.Context, snapshotRef resources.ExternalVolumeSnapshotRef, precondition store.Precondition, mutate func(*ateapi.ExternalVolumeSnapshot) error) (*ateapi.ExternalVolumeSnapshot, error) {
	if err := precondition.Validate(); err != nil {
		return nil, err
	}
	atespace, name := snapshotRef.Atespace, snapshotRef.Name
	var currentUID string
	var currentVersion int64
	var currentBytes []byte
	if err := p.pool.QueryRow(ctx, `
		SELECT uid, version, proto FROM external_volume_snapshots
		WHERE atespace = $1 AND name = $2`, atespace, name).Scan(&currentUID, &currentVersion, &currentBytes); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, store.ErrNotFound
		}
		return nil, fmt.Errorf("getting external volume snapshot %s/%s for update: %w", atespace, name, err)
	}

	dbSnapshot := &ateapi.ExternalVolumeSnapshot{}
	if err := unmarshalStored(currentBytes, dbSnapshot); err != nil {
		return nil, fmt.Errorf("unmarshaling external volume snapshot: %w", err)
	}
	if err := validateProtoMetadataMatchesColumns(fmt.Sprintf("external volume snapshot %s/%s", atespace, name), dbSnapshot.GetMetadata(), currentUID, currentVersion); err != nil {
		return nil, err
	}
	if err := precondition.Check(dbSnapshot.GetMetadata()); err != nil {
		return nil, err
	}
	stored := proto.CloneOf(dbSnapshot)
	if err := mutate(dbSnapshot); err != nil {
		return nil, err
	}
	if err := validateUpdateExternalVolumeSnapshotMutation(stored, dbSnapshot); err != nil {
		return nil, fmt.Errorf("%w: %w", store.ErrImmutableField, err)
	}
	// Stored metadata is authoritative; discard any metadata edits made by the
	// closure.
	setUpdateMetadata(dbSnapshot.Metadata, stored.GetMetadata())
	if errs := apivalidation.ValidateExternalVolumeSnapshot(ctx, dbSnapshot); len(errs) > 0 {
		return nil, fmt.Errorf("invalid external volume snapshot %s/%s: %w", atespace, name, errs.ToAggregate())
	}

	updatedBytes, err := proto.Marshal(dbSnapshot)
	if err != nil {
		return nil, fmt.Errorf("marshaling external volume snapshot: %w", err)
	}
	commandTag, err := p.pool.Exec(ctx, `
		UPDATE external_volume_snapshots
		SET version = $1, proto = $2
		WHERE atespace = $3 AND name = $4 AND uid = $5 AND version = $6`,
		dbSnapshot.GetMetadata().GetVersion(), updatedBytes, atespace, name, currentUID, currentVersion)
	if err != nil {
		return nil, fmt.Errorf("updating external volume snapshot %s/%s: %w", atespace, name, err)
	}
	if commandTag.RowsAffected() == 0 {
		return nil, store.ErrVersionConflict
	}
	if commandTag.RowsAffected() != 1 {
		return nil, fmt.Errorf("updating external volume snapshot %s/%s affected %d rows, want 1", atespace, name, commandTag.RowsAffected())
	}
	return dbSnapshot, nil
}

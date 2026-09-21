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
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/jackc/pgx/v5"
	"google.golang.org/protobuf/proto"
)

func (p *Persistence) GetTag(ctx context.Context, tagRef resources.TagRef) (*ateapipb.Tag, error) {
	atespace, name := tagRef.Atespace, tagRef.Name
	var protoBytes []byte
	if err := p.pool.QueryRow(ctx, `
		SELECT proto FROM tags
		WHERE atespace = $1 AND name = $2`, atespace, name).Scan(&protoBytes); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, store.ErrNotFound
		}
		return nil, fmt.Errorf("getting tag %s/%s: %w", atespace, name, err)
	}
	tag := &ateapipb.Tag{}
	if err := unmarshalStored(protoBytes, tag); err != nil {
		return nil, fmt.Errorf("unmarshaling tag: %w", err)
	}
	return tag, nil
}

func (p *Persistence) ListTags(ctx context.Context, atespace string, opts store.ListOptions) (store.ListResponse[*ateapipb.Tag], error) {
	opts, err := store.NormalizeListOptions(opts)
	if err != nil {
		return store.ListResponse[*ateapipb.Tag]{}, err
	}
	var items []*ateapipb.Tag
	var nextToken string
	if atespace != "" {
		items, nextToken, err = p.listTagsScoped(ctx, atespace, opts.PageSize, opts.PageToken)
	} else {
		items, nextToken, err = p.listTagsGlobal(ctx, opts.PageSize, opts.PageToken)
	}
	if err != nil {
		return store.ListResponse[*ateapipb.Tag]{}, err
	}
	return store.ListResponse[*ateapipb.Tag]{Items: items, NextPageToken: nextToken}, nil
}

func (p *Persistence) listTagsScoped(ctx context.Context, atespace string, pageSize int32, pageTokenStr string) ([]*ateapipb.Tag, string, error) {
	token, err := decodePageToken(pageTokenStr, kindTag, atespace, 1)
	if err != nil {
		return nil, "", err
	}
	var last *string
	if len(token.Last) > 0 {
		last = &token.Last[0]
	}
	rows, err := p.pool.Query(ctx, `
		SELECT name, proto FROM tags
		WHERE atespace = $1 AND ($2::text IS NULL OR name > $2)
		ORDER BY name
		LIMIT $3`, atespace, last, int64(pageSize)+1)
	if err != nil {
		return nil, "", fmt.Errorf("listing tags in %q: %w", atespace, err)
	}
	defer rows.Close()

	var names []string
	var result []*ateapipb.Tag
	for rows.Next() {
		var name string
		var protoBytes []byte
		if err := rows.Scan(&name, &protoBytes); err != nil {
			return nil, "", fmt.Errorf("scanning tag row: %w", err)
		}
		tag := &ateapipb.Tag{}
		if err := unmarshalStored(protoBytes, tag); err != nil {
			return nil, "", fmt.Errorf("unmarshaling tag: %w", err)
		}
		result = append(result, tag)
		names = append(names, name)
	}
	if err := rows.Err(); err != nil {
		return nil, "", fmt.Errorf("listing tags in %q: %w", atespace, err)
	}
	var nextToken string
	if len(result) > int(pageSize) {
		result = result[:pageSize]
		nextToken = encodePageToken(kindTag, atespace, []string{names[pageSize-1]})
	}
	return result, nextToken, nil
}

func (p *Persistence) listTagsGlobal(ctx context.Context, pageSize int32, pageTokenStr string) ([]*ateapipb.Tag, string, error) {
	token, err := decodePageToken(pageTokenStr, kindTag, "", 2)
	if err != nil {
		return nil, "", err
	}
	var lastAtespace, lastName *string
	if len(token.Last) == 2 {
		lastAtespace, lastName = &token.Last[0], &token.Last[1]
	}
	rows, err := p.pool.Query(ctx, `
		SELECT atespace, name, proto FROM tags
		WHERE $1::text IS NULL OR (atespace, name) > ($1, $2)
		ORDER BY atespace, name
		LIMIT $3`, lastAtespace, lastName, int64(pageSize)+1)
	if err != nil {
		return nil, "", fmt.Errorf("listing tags: %w", err)
	}
	defer rows.Close()

	type key struct{ atespace, name string }
	var keys []key
	var result []*ateapipb.Tag
	for rows.Next() {
		var k key
		var protoBytes []byte
		if err := rows.Scan(&k.atespace, &k.name, &protoBytes); err != nil {
			return nil, "", fmt.Errorf("scanning tag row: %w", err)
		}
		tag := &ateapipb.Tag{}
		if err := unmarshalStored(protoBytes, tag); err != nil {
			return nil, "", fmt.Errorf("unmarshaling tag: %w", err)
		}
		result = append(result, tag)
		keys = append(keys, k)
	}
	if err := rows.Err(); err != nil {
		return nil, "", fmt.Errorf("listing tags: %w", err)
	}
	var nextToken string
	if len(result) > int(pageSize) {
		result = result[:pageSize]
		last := keys[pageSize-1]
		nextToken = encodePageToken(kindTag, "", []string{last.atespace, last.name})
	}
	return result, nextToken, nil
}

func (p *Persistence) CreateTag(ctx context.Context, tag *ateapipb.Tag) (*ateapipb.Tag, error) {
	atespace := tag.GetMetadata().GetAtespace()
	name := tag.GetMetadata().GetName()
	dbTag := proto.CloneOf(tag)
	dbTag.Metadata = newCreateMetadata(atespace, name)
	protoBytes, err := proto.Marshal(dbTag)
	if err != nil {
		return nil, fmt.Errorf("marshaling tag: %w", err)
	}

	_, err = p.pool.Exec(ctx, `
		INSERT INTO tags (atespace, name, uid, version, proto)
		VALUES ($1, $2, $3, $4, $5)`, atespace, name,
		dbTag.GetMetadata().GetUid(), dbTag.GetMetadata().GetVersion(), protoBytes)
	if err != nil {
		if isUniqueViolation(err) {
			return nil, store.ErrAlreadyExists
		}
		if isForeignKeyViolation(err) {
			switch pgErrConstraint(err) {
			case "tags_atespace_fk":
				return nil, store.ErrFailedPrecondition
			default:
				return nil, fmt.Errorf("inserting tag %s/%s violated unknown foreign key %q: %w", atespace, name, pgErrConstraint(err), err)
			}
		}
		return nil, fmt.Errorf("inserting tag %s/%s: %w", atespace, name, err)
	}
	return dbTag, nil
}

func validateUpdateTagMutation(storedTag, mutatedTag *ateapipb.Tag) error {
	if stored, mutated := storedTag.GetMetadata().GetAtespace(), mutatedTag.GetMetadata().GetAtespace(); stored != mutated {
		return fmt.Errorf("metadata.atespace is immutable: mutation changed it from %q to %q", stored, mutated)
	}
	if stored, mutated := storedTag.GetMetadata().GetName(), mutatedTag.GetMetadata().GetName(); stored != mutated {
		return fmt.Errorf("metadata.name is immutable: mutation changed it from %q to %q", stored, mutated)
	}
	if stored, mutated := storedTag.GetStatus().GetSnapshot(), mutatedTag.GetStatus().GetSnapshot(); stored != nil && !proto.Equal(stored, mutated) {
		return fmt.Errorf("status.snapshot is immutable once set: mutation changed it from %s to %s", stored, mutated)
	}
	if stored, mutated := storedTag.GetStatus().GetStorageLocation(), mutatedTag.GetStatus().GetStorageLocation(); stored != mutated {
		return fmt.Errorf("status.storage_location is immutable: mutation changed it from %q to %q", stored, mutated)
	}
	if stored, mutated := storedTag.GetStatus().GetActorTemplateUid(), mutatedTag.GetStatus().GetActorTemplateUid(); stored != mutated {
		return fmt.Errorf("status.actor_template_uid is immutable: mutation changed it from %q to %q", stored, mutated)
	}
	if stored, mutated := storedTag.GetStatus().GetSourceActorUid(), mutatedTag.GetStatus().GetSourceActorUid(); stored != mutated {
		return fmt.Errorf("status.source_actor_uid is immutable: mutation changed it from %q to %q", stored, mutated)
	}
	return nil
}

func (p *Persistence) UpdateTag(ctx context.Context, tagRef resources.TagRef, precondition store.Precondition, mutate func(*ateapipb.Tag) error) (*ateapipb.Tag, error) {
	if err := precondition.Validate(); err != nil {
		return nil, err
	}
	atespace, name := tagRef.Atespace, tagRef.Name
	var currentUID string
	var currentVersion int64
	var currentBytes []byte
	if err := p.pool.QueryRow(ctx, `
			SELECT uid, version, proto FROM tags
			WHERE atespace = $1 AND name = $2`, atespace, name).Scan(&currentUID, &currentVersion, &currentBytes); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, store.ErrNotFound
		}
		return nil, fmt.Errorf("getting tag %s/%s for update: %w", atespace, name, err)
	}

	dbTag := &ateapipb.Tag{}
	if err := unmarshalStored(currentBytes, dbTag); err != nil {
		return nil, fmt.Errorf("unmarshaling tag: %w", err)
	}
	if err := validateProtoMetadataMatchesColumns(fmt.Sprintf("tag %s/%s", atespace, name), dbTag.GetMetadata(), currentUID, currentVersion); err != nil {
		return nil, err
	}
	if err := precondition.Check(dbTag.GetMetadata()); err != nil {
		return nil, err
	}
	tagBeforeMutation := proto.Clone(dbTag).(*ateapipb.Tag)
	if err := mutate(dbTag); err != nil {
		return nil, err
	}
	if err := validateUpdateTagMutation(tagBeforeMutation, dbTag); err != nil {
		return nil, fmt.Errorf("%w: %w", store.ErrImmutableField, err)
	}
	// Stored metadata is authoritative; discard any metadata edits made by the
	// closure and derive the next revision from the state this attempt read.
	dbTag.Metadata = newUpdateMetadata(tagBeforeMutation.GetMetadata())

	updatedBytes, err := proto.Marshal(dbTag)
	if err != nil {
		return nil, fmt.Errorf("marshaling tag: %w", err)
	}
	commandTag, err := p.pool.Exec(ctx, `
			UPDATE tags
			SET version = $1, proto = $2
			WHERE atespace = $3 AND name = $4 AND uid = $5 AND version = $6`,
		dbTag.GetMetadata().GetVersion(), updatedBytes, atespace, name, currentUID, currentVersion)
	if err != nil {
		return nil, fmt.Errorf("updating tag %s/%s: %w", atespace, name, err)
	}
	if commandTag.RowsAffected() == 0 {
		return nil, store.ErrVersionConflict
	}
	if commandTag.RowsAffected() != 1 {
		return nil, fmt.Errorf("updating tag %s/%s affected %d rows, want 1", atespace, name, commandTag.RowsAffected())
	}
	return dbTag, nil
}

func (p *Persistence) DeleteTag(ctx context.Context, tagRef resources.TagRef) (*ateapipb.Tag, error) {
	atespace, name := tagRef.Atespace, tagRef.Name
	var protoBytes []byte
	if err := p.pool.QueryRow(ctx, `
		DELETE FROM tags
		WHERE atespace = $1 AND name = $2
		RETURNING proto`, atespace, name).Scan(&protoBytes); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, store.ErrNotFound
		}
		return nil, fmt.Errorf("deleting tag %s/%s: %w", atespace, name, err)
	}
	tag := &ateapipb.Tag{}
	if err := unmarshalStored(protoBytes, tag); err != nil {
		return nil, fmt.Errorf("unmarshaling deleted tag: %w", err)
	}
	return tag, nil
}

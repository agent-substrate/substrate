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

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store/storesql"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/protobuf/proto"
)

func (p *Persistence) GetTag(ctx context.Context, tagRef resources.TagRef) (*ateapipb.Tag, error) {
	atespace, name := tagRef.Atespace, tagRef.Name
	var protoBytes []byte
	if err := p.db.QueryRowContext(ctx, `
		SELECT proto FROM tags
		WHERE atespace = ? AND name = ?`, atespace, name).Scan(&protoBytes); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, store.ErrNotFound
		}
		return nil, fmt.Errorf("getting tag %s/%s: %w", atespace, name, err)
	}
	tag := &ateapipb.Tag{}
	if err := storesql.UnmarshalStored(protoBytes, tag); err != nil {
		return nil, fmt.Errorf("unmarshaling tag: %w", err)
	}
	return tag, nil
}

func (p *Persistence) ListTags(ctx context.Context, atespace string, opts store.ListOptions) (store.ListResponse[*ateapipb.Tag], error) {
	opts, err := store.NormalizeListOptions(opts)
	if err != nil {
		return store.ListResponse[*ateapipb.Tag]{}, err
	}
	decode := func(b []byte, keys []string) (*ateapipb.Tag, error) {
		tag := &ateapipb.Tag{}
		return tag, storesql.UnmarshalRow(b, tag, "tag", rowID(atespace, keys)...)
	}
	items, next, err := listScoped(ctx, p.db, "tags", storesql.KindTag, atespace, opts, decode)
	if err != nil {
		return store.ListResponse[*ateapipb.Tag]{}, err
	}
	return store.ListResponse[*ateapipb.Tag]{Items: items, NextPageToken: next}, nil
}

func (p *Persistence) CreateTag(ctx context.Context, tag *ateapipb.Tag) (*ateapipb.Tag, error) {
	atespace := tag.GetMetadata().GetAtespace()
	name := tag.GetMetadata().GetName()
	dbTag := proto.CloneOf(tag)
	storesql.SetCreateMetadata(dbTag.Metadata)
	protoBytes, err := proto.Marshal(dbTag)
	if err != nil {
		return nil, fmt.Errorf("marshaling tag: %w", err)
	}
	err = p.insertChild(ctx, "tag "+atespace+"/"+name, func(tx *sql.Tx) error { return lockAtespace(ctx, tx, atespace) }, `
		INSERT INTO tags (atespace, name, uid, version, proto)
		VALUES (?, ?, ?, ?, ?)`, atespace, name,
		dbTag.GetMetadata().GetUid(), dbTag.GetMetadata().GetVersion(), protoBytes)
	if err != nil {
		return nil, err
	}
	return dbTag, nil
}

func (p *Persistence) UpdateTag(ctx context.Context, tagRef resources.TagRef, precondition store.Precondition, mutate func(*ateapipb.Tag) error) (*ateapipb.Tag, error) {
	if err := precondition.Validate(); err != nil {
		return nil, err
	}
	atespace, name := tagRef.Atespace, tagRef.Name
	var currentUID string
	var currentVersion int64
	var currentBytes []byte
	if err := p.db.QueryRowContext(ctx, `
			SELECT uid, version, proto FROM tags
			WHERE atespace = ? AND name = ?`, atespace, name).Scan(&currentUID, &currentVersion, &currentBytes); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, store.ErrNotFound
		}
		return nil, fmt.Errorf("getting tag %s/%s for update: %w", atespace, name, err)
	}

	dbTag := &ateapipb.Tag{}
	if err := storesql.UnmarshalStored(currentBytes, dbTag); err != nil {
		return nil, fmt.Errorf("unmarshaling tag: %w", err)
	}
	if err := storesql.ValidateProtoMetadataMatchesColumns(fmt.Sprintf("tag %s/%s", atespace, name), dbTag.GetMetadata(), currentUID, currentVersion); err != nil {
		return nil, err
	}
	if err := precondition.Check(dbTag.GetMetadata()); err != nil {
		return nil, err
	}
	tagBeforeMutation := proto.Clone(dbTag).(*ateapipb.Tag)
	if err := mutate(dbTag); err != nil {
		return nil, err
	}
	if err := storesql.ValidateUpdateTagMutation(tagBeforeMutation, dbTag); err != nil {
		return nil, fmt.Errorf("%w: %w", store.ErrImmutableField, err)
	}
	// Stored metadata is authoritative; discard any metadata edits made by the
	// closure and derive the next revision from the state this attempt read.
	storesql.SetUpdateMetadata(dbTag.Metadata, tagBeforeMutation.GetMetadata())

	updatedBytes, err := proto.Marshal(dbTag)
	if err != nil {
		return nil, fmt.Errorf("marshaling tag: %w", err)
	}
	if err := updateGuarded(ctx, p.db, fmt.Sprintf("tag %s/%s", atespace, name), `
			UPDATE tags
			SET version = ?, proto = ?
			WHERE atespace = ? AND name = ? AND uid = ? AND version = ?`,
		dbTag.GetMetadata().GetVersion(), updatedBytes, atespace, name, currentUID, currentVersion); err != nil {
		return nil, err
	}
	return dbTag, nil
}

func (p *Persistence) DeleteTag(ctx context.Context, tagRef resources.TagRef, precondition store.DeletePreconditions) (*ateapipb.Tag, error) {
	var protoBytes []byte
	err := inTx(ctx, p.db, func(tx *sql.Tx) error {
		var err error
		_, _, protoBytes, err = deleteLockedRow(ctx, tx, "tags", "atespace = ? AND name = ?", precondition, tagRef.Atespace, tagRef.Name)
		return err
	})
	if err != nil {
		return nil, err
	}
	tag := &ateapipb.Tag{}
	if err := storesql.UnmarshalStored(protoBytes, tag); err != nil {
		return nil, fmt.Errorf("unmarshaling deleted tag: %w", err)
	}
	return tag, nil
}

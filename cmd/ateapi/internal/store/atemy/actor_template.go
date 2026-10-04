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

func (p *Persistence) CreateActorTemplate(ctx context.Context, template *ateapipb.ActorTemplate) (*ateapipb.ActorTemplate, error) {
	atespace, name := template.GetMetadata().GetAtespace(), template.GetMetadata().GetName()
	dbTemplate := proto.Clone(template).(*ateapipb.ActorTemplate)
	if dbTemplate.Metadata == nil {
		dbTemplate.Metadata = &ateapipb.ResourceMetadata{}
	}
	storesql.SetCreateMetadata(dbTemplate.Metadata)
	protoBytes, err := proto.Marshal(dbTemplate)
	if err != nil {
		return nil, fmt.Errorf("marshaling actor template: %w", err)
	}
	err = p.insertChild(ctx, "actor template "+atespace+"/"+name, func(tx *sql.Tx) error { return lockAtespace(ctx, tx, atespace) }, `
		INSERT INTO actor_templates (atespace, name, uid, version, proto)
		VALUES (?, ?, ?, ?, ?)`,
		atespace, name, dbTemplate.GetMetadata().GetUid(), dbTemplate.GetMetadata().GetVersion(), protoBytes)
	if err != nil {
		return nil, err
	}
	return dbTemplate, nil
}

func (p *Persistence) GetActorTemplate(ctx context.Context, templateRef resources.ActorTemplateRef) (*ateapipb.ActorTemplate, error) {
	var protoBytes []byte
	err := p.db.QueryRowContext(ctx, `SELECT proto FROM actor_templates WHERE atespace = ? AND name = ?`, templateRef.Atespace, templateRef.Name).Scan(&protoBytes)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, store.ErrNotFound
		}
		return nil, fmt.Errorf("getting actor template %s: %w", templateRef, err)
	}
	out := &ateapipb.ActorTemplate{}
	if err := storesql.UnmarshalStored(protoBytes, out); err != nil {
		return nil, fmt.Errorf("unmarshaling actor template: %w", err)
	}
	return out, nil
}

func (p *Persistence) UpdateActorTemplate(ctx context.Context, templateRef resources.ActorTemplateRef, precondition store.Precondition, mutate func(*ateapipb.ActorTemplate) error) (*ateapipb.ActorTemplate, error) {
	if err := precondition.Validate(); err != nil {
		return nil, err
	}
	var currentUID string
	var currentVersion int64
	var currentBytes []byte
	if err := p.db.QueryRowContext(ctx, `
			SELECT uid, version, proto FROM actor_templates
			WHERE atespace = ? AND name = ?`, templateRef.Atespace, templateRef.Name).Scan(&currentUID, &currentVersion, &currentBytes); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, store.ErrNotFound
		}
		return nil, fmt.Errorf("getting actor template %s for update: %w", templateRef, err)
	}

	dbTemplate := &ateapipb.ActorTemplate{}
	if err := storesql.UnmarshalStored(currentBytes, dbTemplate); err != nil {
		return nil, fmt.Errorf("unmarshaling actor template for update: %w", err)
	}
	if err := storesql.ValidateProtoMetadataMatchesColumns("actor template "+templateRef.String(), dbTemplate.GetMetadata(), currentUID, currentVersion); err != nil {
		return nil, err
	}
	if err := precondition.Check(dbTemplate.GetMetadata()); err != nil {
		return nil, err
	}
	templateBeforeMutation := proto.Clone(dbTemplate).(*ateapipb.ActorTemplate)
	if err := mutate(dbTemplate); err != nil {
		return nil, err
	}
	if err := storesql.ValidateUpdateActorTemplateMutation(templateBeforeMutation, dbTemplate); err != nil {
		return nil, err
	}
	if dbTemplate.Metadata == nil {
		dbTemplate.Metadata = &ateapipb.ResourceMetadata{}
	}
	storesql.SetUpdateMetadata(dbTemplate.Metadata, templateBeforeMutation.GetMetadata())
	updatedBytes, err := proto.Marshal(dbTemplate)
	if err != nil {
		return nil, fmt.Errorf("marshaling actor template: %w", err)
	}
	if err := updateGuarded(ctx, p.db, "actor template "+templateRef.String(), `
			UPDATE actor_templates SET version = ?, proto = ?
			WHERE atespace = ? AND name = ? AND uid = ? AND version = ?`,
		dbTemplate.GetMetadata().GetVersion(), updatedBytes, templateRef.Atespace, templateRef.Name, currentUID, currentVersion); err != nil {
		return nil, err
	}
	return dbTemplate, nil
}

func (p *Persistence) ListActorTemplates(ctx context.Context, atespace string, opts store.ListOptions) (store.ListResponse[*ateapipb.ActorTemplate], error) {
	opts, err := store.NormalizeListOptions(opts)
	if err != nil {
		return store.ListResponse[*ateapipb.ActorTemplate]{}, err
	}
	decode := func(b []byte, keys []string) (*ateapipb.ActorTemplate, error) {
		template := &ateapipb.ActorTemplate{}
		return template, storesql.UnmarshalRow(b, template, "actor template", rowID(atespace, keys)...)
	}
	items, next, err := listScoped(ctx, p.db, "actor_templates", storesql.KindActorTemplate, atespace, opts, decode)
	if err != nil {
		return store.ListResponse[*ateapipb.ActorTemplate]{}, err
	}
	return store.ListResponse[*ateapipb.ActorTemplate]{Items: items, NextPageToken: next}, nil
}

func (p *Persistence) DeleteActorTemplate(ctx context.Context, templateRef resources.ActorTemplateRef, precondition store.DeletePreconditions) (*ateapipb.ActorTemplate, error) {
	var protoBytes []byte
	err := inTx(ctx, p.db, func(tx *sql.Tx) error {
		var err error
		_, _, protoBytes, err = deleteLockedRow(ctx, tx, "actor_templates", "atespace = ? AND name = ?", precondition, templateRef.Atespace, templateRef.Name)
		return err
	})
	if err != nil {
		return nil, err
	}
	out := &ateapipb.ActorTemplate{}
	if err := storesql.UnmarshalStored(protoBytes, out); err != nil {
		return nil, fmt.Errorf("unmarshaling deleted actor template: %w", err)
	}
	return out, nil
}

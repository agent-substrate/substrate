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

func (p *Persistence) CreateActorTemplate(ctx context.Context, template *ateapipb.ActorTemplate) (*ateapipb.ActorTemplate, error) {
	atespace, name := template.GetMetadata().GetAtespace(), template.GetMetadata().GetName()
	dbTemplate := proto.Clone(template).(*ateapipb.ActorTemplate)
	if dbTemplate.Metadata == nil {
		dbTemplate.Metadata = &ateapipb.ResourceMetadata{}
	}
	setCreateMetadata(dbTemplate.Metadata)
	protoBytes, err := proto.Marshal(dbTemplate)
	if err != nil {
		return nil, fmt.Errorf("marshaling actor template: %w", err)
	}
	_, err = p.pool.Exec(ctx, `
		INSERT INTO actor_templates (atespace, name, uid, version, proto)
		VALUES ($1, $2, $3, $4, $5)`,
		atespace, name, dbTemplate.GetMetadata().GetUid(), dbTemplate.GetMetadata().GetVersion(), protoBytes)
	if err != nil {
		if isUniqueViolation(err) {
			return nil, store.ErrAlreadyExists
		}
		if isForeignKeyViolation(err) {
			return nil, store.ErrFailedPrecondition
		}
		return nil, fmt.Errorf("inserting actor template %s/%s: %w", atespace, name, err)
	}
	return dbTemplate, nil
}

func (p *Persistence) GetActorTemplate(ctx context.Context, templateRef resources.ActorTemplateRef) (*ateapipb.ActorTemplate, error) {
	var protoBytes []byte
	err := p.pool.QueryRow(ctx, `SELECT proto FROM actor_templates WHERE atespace = $1 AND name = $2`, templateRef.Atespace, templateRef.Name).Scan(&protoBytes)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, store.ErrNotFound
		}
		return nil, fmt.Errorf("getting actor template %s: %w", templateRef, err)
	}
	out := &ateapipb.ActorTemplate{}
	if err := unmarshalStored(protoBytes, out); err != nil {
		return nil, fmt.Errorf("unmarshaling actor template: %w", err)
	}
	return out, nil
}

func validateUpdateActorTemplateMutation(storedTemplate, mutatedTemplate *ateapipb.ActorTemplate) error {
	if stored, mutated := storedTemplate.GetMetadata().GetAtespace(), mutatedTemplate.GetMetadata().GetAtespace(); stored != mutated {
		return fmt.Errorf("metadata.atespace is immutable: mutation changed it from %q to %q", stored, mutated)
	}
	if stored, mutated := storedTemplate.GetMetadata().GetName(), mutatedTemplate.GetMetadata().GetName(); stored != mutated {
		return fmt.Errorf("metadata.name is immutable: mutation changed it from %q to %q", stored, mutated)
	}
	return nil
}

func (p *Persistence) UpdateActorTemplate(ctx context.Context, templateRef resources.ActorTemplateRef, precondition store.Precondition, mutate func(*ateapipb.ActorTemplate) error) (*ateapipb.ActorTemplate, error) {
	if err := precondition.Validate(); err != nil {
		return nil, err
	}
	var currentUID string
	var currentVersion int64
	var currentBytes []byte
	if err := p.pool.QueryRow(ctx, `
			SELECT uid, version, proto FROM actor_templates
			WHERE atespace = $1 AND name = $2`, templateRef.Atespace, templateRef.Name).Scan(&currentUID, &currentVersion, &currentBytes); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, store.ErrNotFound
		}
		return nil, fmt.Errorf("getting actor template %s for update: %w", templateRef, err)
	}

	dbTemplate := &ateapipb.ActorTemplate{}
	if err := unmarshalStored(currentBytes, dbTemplate); err != nil {
		return nil, fmt.Errorf("unmarshaling actor template for update: %w", err)
	}
	if err := validateProtoMetadataMatchesColumns("actor template "+templateRef.String(), dbTemplate.GetMetadata(), currentUID, currentVersion); err != nil {
		return nil, err
	}
	if err := precondition.Check(dbTemplate.GetMetadata()); err != nil {
		return nil, err
	}
	templateBeforeMutation := proto.Clone(dbTemplate).(*ateapipb.ActorTemplate)
	if err := mutate(dbTemplate); err != nil {
		return nil, err
	}
	if err := validateUpdateActorTemplateMutation(templateBeforeMutation, dbTemplate); err != nil {
		return nil, err
	}
	if dbTemplate.Metadata == nil {
		dbTemplate.Metadata = &ateapipb.ResourceMetadata{}
	}
	setUpdateMetadata(dbTemplate.Metadata, templateBeforeMutation.GetMetadata())
	updatedBytes, err := proto.Marshal(dbTemplate)
	if err != nil {
		return nil, fmt.Errorf("marshaling actor template: %w", err)
	}
	commandTag, err := p.pool.Exec(ctx, `
			UPDATE actor_templates SET version = $1, proto = $2
			WHERE atespace = $3 AND name = $4 AND uid = $5 AND version = $6`,
		dbTemplate.GetMetadata().GetVersion(), updatedBytes, templateRef.Atespace, templateRef.Name, currentUID, currentVersion)
	if err != nil {
		return nil, fmt.Errorf("updating actor template %s: %w", templateRef, err)
	}
	if commandTag.RowsAffected() == 0 {
		return nil, store.ErrVersionConflict
	}
	if commandTag.RowsAffected() != 1 {
		return nil, fmt.Errorf("updating actor template %s affected %d rows, want 1", templateRef, commandTag.RowsAffected())
	}
	return dbTemplate, nil
}

func (p *Persistence) ListActorTemplates(ctx context.Context, atespace string, opts store.ListOptions) (store.ListResponse[*ateapipb.ActorTemplate], error) {
	opts, err := store.NormalizeListOptions(opts)
	if err != nil {
		return store.ListResponse[*ateapipb.ActorTemplate]{}, err
	}
	pageSize, pageTokenStr := opts.PageSize, opts.PageToken
	keyParts := 2
	if atespace != "" {
		keyParts = 1
	}
	token, err := decodePageToken(pageTokenStr, kindActorTemplate, atespace, keyParts)
	if err != nil {
		return store.ListResponse[*ateapipb.ActorTemplate]{}, err
	}

	var rows pgx.Rows
	if atespace != "" {
		var last *string
		if len(token.Last) == 1 {
			last = &token.Last[0]
		}
		rows, err = p.pool.Query(ctx, `
			SELECT atespace, name, proto FROM actor_templates
			WHERE atespace = $1 AND ($2::text IS NULL OR name > $2)
			ORDER BY name LIMIT $3`, atespace, last, int64(pageSize)+1)
	} else {
		var lastAtespace, lastName *string
		if len(token.Last) == 2 {
			lastAtespace, lastName = &token.Last[0], &token.Last[1]
		}
		rows, err = p.pool.Query(ctx, `
			SELECT atespace, name, proto FROM actor_templates
			WHERE $1::text IS NULL OR (atespace, name) > ($1, $2)
			ORDER BY atespace, name LIMIT $3`, lastAtespace, lastName, int64(pageSize)+1)
	}
	if err != nil {
		return store.ListResponse[*ateapipb.ActorTemplate]{}, fmt.Errorf("listing actor templates: %w", err)
	}
	defer rows.Close()

	type key struct{ atespace, name string }
	var keys []key
	var result []*ateapipb.ActorTemplate
	for rows.Next() {
		var k key
		var protoBytes []byte
		if err := rows.Scan(&k.atespace, &k.name, &protoBytes); err != nil {
			return store.ListResponse[*ateapipb.ActorTemplate]{}, fmt.Errorf("scanning actor template row: %w", err)
		}
		template := &ateapipb.ActorTemplate{}
		if err := unmarshalStored(protoBytes, template); err != nil {
			return store.ListResponse[*ateapipb.ActorTemplate]{}, fmt.Errorf("unmarshaling actor template: %w", err)
		}
		keys = append(keys, k)
		result = append(result, template)
	}
	if err := rows.Err(); err != nil {
		return store.ListResponse[*ateapipb.ActorTemplate]{}, fmt.Errorf("listing actor templates: %w", err)
	}
	var nextToken string
	if len(result) > int(pageSize) {
		result = result[:pageSize]
		last := keys[pageSize-1]
		lastParts := []string{last.atespace, last.name}
		if atespace != "" {
			lastParts = []string{last.name}
		}
		nextToken = encodePageToken(kindActorTemplate, atespace, lastParts)
	}
	return store.ListResponse[*ateapipb.ActorTemplate]{Items: result, NextPageToken: nextToken}, nil
}

func (p *Persistence) DeleteActorTemplate(ctx context.Context, templateRef resources.ActorTemplateRef) (*ateapipb.ActorTemplate, error) {
	var protoBytes []byte
	err := p.pool.QueryRow(ctx, `
		DELETE FROM actor_templates AS t
		WHERE t.atespace = $1 AND t.name = $2
		RETURNING t.proto`, templateRef.Atespace, templateRef.Name).Scan(&protoBytes)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, store.ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("deleting actor template %s: %w", templateRef, err)
	}
	out := &ateapipb.ActorTemplate{}
	if err := unmarshalStored(protoBytes, out); err != nil {
		return nil, fmt.Errorf("unmarshaling deleted actor template: %w", err)
	}
	return out, nil
}

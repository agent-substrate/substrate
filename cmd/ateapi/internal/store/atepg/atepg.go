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

// Package atepg is an ate storage backend built on PostgreSQL.
//
// Each table holds native SQL columns for fields SQL must operate on
// (primary keys, versions, pagination, update/delete preconditions) plus
// the complete protobuf message, binary-encoded, in a BYTEA column.
// TLS is configured entirely through the connection string passed
// to Connect (standard libpq sslmode/sslrootcert/sslcert/sslkey parameters)
package atepg

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Persistence is a service that stores ate state in PostgreSQL.
type Persistence struct {
	pool    *pgxpool.Pool
	lockTTL time.Duration
}

var _ store.Interface = (*Persistence)(nil)

// Connect opens a pgxpool against dsn, verifies connectivity, and applies the
// embedded schema. Startup fails if the database cannot be reached.
func Connect(ctx context.Context, dsn string) (*Persistence, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("opening PostgreSQL pool: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("pinging PostgreSQL: %w", err)
	}
	p, err := NewPersistence(ctx, pool)
	if err != nil {
		pool.Close()
		return nil, err
	}
	return p, nil
}

// NewPersistence wraps an already-open pool, applying the idempotent schema.
// Callers that already hold a pool (e.g. tests using
// testcontainers) use this directly instead of Connect.
func NewPersistence(ctx context.Context, pool *pgxpool.Pool) (*Persistence, error) {
	if err := applySchema(ctx, pool); err != nil {
		return nil, err
	}
	return &Persistence{pool: pool, lockTTL: defaultLockTTL}, nil
}

// querier is satisfied by both *pgxpool.Pool and pgx.Tx, letting read helpers
// run either directly against the pool or inside an in-flight transaction.
type querier interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
}

func newCreateMetadata(atespace, name string) *ateapipb.ResourceMetadata {
	now := timestamppb.Now()
	return &ateapipb.ResourceMetadata{
		Atespace:   atespace,
		Name:       name,
		Uid:        uuid.NewString(),
		Version:    1,
		CreateTime: now,
		UpdateTime: now,
	}
}

func newUpdateMetadata(current *ateapipb.ResourceMetadata) *ateapipb.ResourceMetadata {
	metadata := proto.Clone(current).(*ateapipb.ResourceMetadata)
	metadata.Version++
	metadata.UpdateTime = timestamppb.Now()
	return metadata
}

// validateProtoMetadataMatchesColumns verifies that the metadata in the database
// matches the metadata in the proto.
func validateProtoMetadataMatchesColumns(resource string, metadata *ateapipb.ResourceMetadata, uid string, version int64) error {
	if metadata.GetUid() != uid {
		return fmt.Errorf("%s uid projection %q does not match proto metadata uid %q", resource, uid, metadata.GetUid())
	}
	if metadata.GetVersion() != version {
		return fmt.Errorf("%s version projection %d does not match proto metadata version %d", resource, version, metadata.GetVersion())
	}
	return nil
}

func isUniqueViolation(err error) bool { return pgErrCode(err) == "23505" }

// isForeignKeyViolation matches both the insert/update-side violation
// (23503, foreign_key_violation) and the delete-side violation PostgreSQL 18
// split out into its own code (23001, restrict_violation, for ON DELETE
// RESTRICT); older PostgreSQL versions report 23503 for both cases.
func isForeignKeyViolation(err error) bool {
	switch pgErrCode(err) {
	case "23503", "23001":
		return true
	default:
		return false
	}
}

func pgErrCode(err error) string {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code
	}
	return ""
}

func pgErrConstraint(err error) string {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.ConstraintName
	}
	return ""
}

// --- Atespaces ---

func (p *Persistence) CreateAtespace(ctx context.Context, atespace *ateapipb.Atespace) (*ateapipb.Atespace, error) {
	name := atespace.GetMetadata().GetName()

	dbAtespace := proto.Clone(atespace).(*ateapipb.Atespace)
	dbAtespace.Metadata = newCreateMetadata("", name)

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

func (p *Persistence) GetAtespace(ctx context.Context, name string) (*ateapipb.Atespace, error) {
	var protoBytes []byte
	err := p.pool.QueryRow(ctx, `SELECT proto FROM atespaces WHERE name = $1`, name).Scan(&protoBytes)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, store.ErrNotFound
		}
		return nil, fmt.Errorf("getting atespace %q: %w", name, err)
	}
	out := &ateapipb.Atespace{}
	if err := proto.Unmarshal(protoBytes, out); err != nil {
		return nil, fmt.Errorf("unmarshaling atespace: %w", err)
	}
	return out, nil
}

func (p *Persistence) AtespaceExists(ctx context.Context, name string) (bool, error) {
	var exists bool
	if err := p.pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM atespaces WHERE name = $1)`, name).Scan(&exists); err != nil {
		return false, fmt.Errorf("checking atespace existence: %w", err)
	}
	return exists, nil
}

func (p *Persistence) ListAtespaces(ctx context.Context, opts store.ListOptions) (store.ListResponse[*ateapipb.Atespace], error) {
	opts, err := store.NormalizeListOptions(opts)
	if err != nil {
		return store.ListResponse[*ateapipb.Atespace]{}, err
	}
	pageSize, pageTokenStr := opts.PageSize, opts.PageToken
	token, err := decodePageToken(pageTokenStr, kindAtespace, "", 1)
	if err != nil {
		return store.ListResponse[*ateapipb.Atespace]{}, err
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
		return store.ListResponse[*ateapipb.Atespace]{}, fmt.Errorf("listing atespaces: %w", err)
	}
	defer rows.Close()

	var names []string
	var result []*ateapipb.Atespace
	for rows.Next() {
		var name string
		var protoBytes []byte
		if err := rows.Scan(&name, &protoBytes); err != nil {
			return store.ListResponse[*ateapipb.Atespace]{}, fmt.Errorf("scanning atespace row: %w", err)
		}
		a := &ateapipb.Atespace{}
		if err := proto.Unmarshal(protoBytes, a); err != nil {
			return store.ListResponse[*ateapipb.Atespace]{}, fmt.Errorf("unmarshaling atespace: %w", err)
		}
		result = append(result, a)
		names = append(names, name)
	}
	if err := rows.Err(); err != nil {
		return store.ListResponse[*ateapipb.Atespace]{}, fmt.Errorf("listing atespaces: %w", err)
	}

	var nextToken string
	if len(result) > int(pageSize) {
		result = result[:pageSize]
		nextToken = encodePageToken(kindAtespace, "", []string{names[pageSize-1]})
	}
	return store.ListResponse[*ateapipb.Atespace]{Items: result, NextPageToken: nextToken}, nil
}

func (p *Persistence) DeleteAtespace(ctx context.Context, name string) (*ateapipb.Atespace, error) {
	var protoBytes []byte
	err := p.pool.QueryRow(ctx, `DELETE FROM atespaces WHERE name = $1 RETURNING proto`, name).Scan(&protoBytes)
	if err != nil {
		if isForeignKeyViolation(err) {
			return nil, store.ErrFailedPrecondition
		}
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, store.ErrNotFound
		}
		return nil, fmt.Errorf("deleting atespace %q: %w", name, err)
	}
	out := &ateapipb.Atespace{}
	if err := proto.Unmarshal(protoBytes, out); err != nil {
		return nil, fmt.Errorf("unmarshaling deleted atespace: %w", err)
	}
	return out, nil
}

// --- Actor templates ---

func (p *Persistence) CreateActorTemplate(ctx context.Context, template *ateapipb.ActorTemplate) (*ateapipb.ActorTemplate, error) {
	atespace, name := template.GetMetadata().GetAtespace(), template.GetMetadata().GetName()
	dbTemplate := proto.Clone(template).(*ateapipb.ActorTemplate)
	dbTemplate.Metadata = newCreateMetadata(atespace, name)
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
	if err := proto.Unmarshal(protoBytes, out); err != nil {
		return nil, fmt.Errorf("unmarshaling actor template: %w", err)
	}
	return out, nil
}

func (p *Persistence) ActorTemplateExists(ctx context.Context, templateRef resources.ActorTemplateRef) (bool, error) {
	var exists bool
	if err := p.pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM actor_templates WHERE atespace = $1 AND name = $2)`, templateRef.Atespace, templateRef.Name).Scan(&exists); err != nil {
		return false, fmt.Errorf("checking actor template existence: %w", err)
	}
	return exists, nil
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
	if err := proto.Unmarshal(currentBytes, dbTemplate); err != nil {
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
	dbTemplate.Metadata = newUpdateMetadata(templateBeforeMutation.GetMetadata())
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
		if err := proto.Unmarshal(protoBytes, template); err != nil {
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
	if err := proto.Unmarshal(protoBytes, out); err != nil {
		return nil, fmt.Errorf("unmarshaling deleted actor template: %w", err)
	}
	return out, nil
}

// --- Actors ---

func (p *Persistence) CreateActor(ctx context.Context, actor *ateapipb.Actor) (*ateapipb.Actor, error) {
	atespace := actor.GetMetadata().GetAtespace()
	name := actor.GetMetadata().GetName()

	dbActor := proto.Clone(actor).(*ateapipb.Actor)
	dbActor.Metadata = newCreateMetadata(atespace, name)

	protoBytes, err := proto.Marshal(dbActor)
	if err != nil {
		return nil, fmt.Errorf("marshaling actor: %w", err)
	}

	_, err = p.pool.Exec(ctx, `
		INSERT INTO actors (atespace, name, uid, version, proto)
		VALUES ($1, $2, $3, $4, $5)`,
		atespace, name, dbActor.GetMetadata().GetUid(), dbActor.GetMetadata().GetVersion(), protoBytes)
	if err != nil {
		if isUniqueViolation(err) {
			return nil, store.ErrAlreadyExists
		}
		if isForeignKeyViolation(err) {
			// The atespace referenced by this actor doesn't exist (or was
			// deleted concurrently with the control API's own pre-check).
			return nil, store.ErrFailedPrecondition
		}
		return nil, fmt.Errorf("inserting actor %s/%s: %w", atespace, name, err)
	}
	return dbActor, nil
}

func (p *Persistence) GetActor(ctx context.Context, actorRef resources.ActorRef) (*ateapipb.Actor, error) {
	var protoBytes []byte
	err := p.pool.QueryRow(ctx, `SELECT proto FROM actors WHERE atespace = $1 AND name = $2`, actorRef.Atespace, actorRef.Name).Scan(&protoBytes)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, store.ErrNotFound
		}
		return nil, fmt.Errorf("getting actor %s/%s: %w", actorRef.Atespace, actorRef.Name, err)
	}
	out := &ateapipb.Actor{}
	if err := proto.Unmarshal(protoBytes, out); err != nil {
		return nil, fmt.Errorf("unmarshaling actor: %w", err)
	}
	return out, nil
}

// validateUpdateActorMutation reports whether an actor mutation changed fields
// that are immutable for the lifetime of the stored actor.
func validateUpdateActorMutation(storedActor, mutatedActor *ateapipb.Actor) error {
	if stored, mutated := storedActor.GetMetadata().GetAtespace(), mutatedActor.GetMetadata().GetAtespace(); stored != mutated {
		return fmt.Errorf("metadata.atespace is immutable: mutation changed it from %q to %q", stored, mutated)
	}
	if stored, mutated := storedActor.GetMetadata().GetName(), mutatedActor.GetMetadata().GetName(); stored != mutated {
		return fmt.Errorf("metadata.name is immutable: mutation changed it from %q to %q", stored, mutated)
	}
	if stored, mutated := storedActor.GetActorTemplateNamespace(), mutatedActor.GetActorTemplateNamespace(); stored != mutated {
		return fmt.Errorf("actor_template_namespace is immutable: mutation changed it from %q to %q", stored, mutated)
	}
	if stored, mutated := storedActor.GetActorTemplateName(), mutatedActor.GetActorTemplateName(); stored != mutated {
		return fmt.Errorf("actor_template_name is immutable: mutation changed it from %q to %q", stored, mutated)
	}
	if stored, mutated := storedActor.GetActorTemplate(), mutatedActor.GetActorTemplate(); !proto.Equal(stored, mutated) {
		return fmt.Errorf("actor_template is immutable: mutation changed it from %v to %v", stored, mutated)
	}
	if stored, mutated := storedActor.GetSourceSnapshotTag(), mutatedActor.GetSourceSnapshotTag(); !proto.Equal(stored, mutated) {
		return fmt.Errorf("source_snapshot_tag is immutable: mutation changed it from %v to %v", stored, mutated)
	}
	return nil
}

func (p *Persistence) UpdateActor(ctx context.Context, actorRef resources.ActorRef, precondition store.Precondition, mutate func(*ateapipb.Actor) error) (*ateapipb.Actor, error) {
	if err := precondition.Validate(); err != nil {
		return nil, err
	}
	atespace, name := actorRef.Atespace, actorRef.Name
	var currentUID string
	var currentVersion int64
	var currentBytes []byte
	if err := p.pool.QueryRow(ctx, `
			SELECT uid, version, proto FROM actors
			WHERE atespace = $1 AND name = $2`, atespace, name).Scan(&currentUID, &currentVersion, &currentBytes); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, store.ErrNotFound
		}
		return nil, fmt.Errorf("getting actor %s/%s for update: %w", atespace, name, err)
	}

	dbActor := &ateapipb.Actor{}
	if err := proto.Unmarshal(currentBytes, dbActor); err != nil {
		return nil, fmt.Errorf("unmarshaling actor for update: %w", err)
	}
	if err := validateProtoMetadataMatchesColumns("actor "+actorRef.String(), dbActor.GetMetadata(), currentUID, currentVersion); err != nil {
		return nil, err
	}
	if err := precondition.Check(dbActor.GetMetadata()); err != nil {
		return nil, err
	}
	actorBeforeMutation := proto.Clone(dbActor).(*ateapipb.Actor)
	if err := mutate(dbActor); err != nil {
		return nil, err
	}
	if err := validateUpdateActorMutation(actorBeforeMutation, dbActor); err != nil {
		return nil, fmt.Errorf("%w: %w", store.ErrImmutableField, err)
	}
	// Stored metadata is authoritative; discard any metadata edits made by the
	// closure and derive the next revision from the state this attempt read.
	dbActor.Metadata = newUpdateMetadata(actorBeforeMutation.GetMetadata())

	updatedBytes, err := proto.Marshal(dbActor)
	if err != nil {
		return nil, fmt.Errorf("marshaling actor: %w", err)
	}
	commandTag, err := p.pool.Exec(ctx, `
			UPDATE actors
			SET version = $1, proto = $2
			WHERE atespace = $3 AND name = $4 AND uid = $5 AND version = $6`,
		dbActor.GetMetadata().GetVersion(), updatedBytes, atespace, name, currentUID, currentVersion)
	if err != nil {
		return nil, fmt.Errorf("updating actor %s/%s: %w", atespace, name, err)
	}
	if commandTag.RowsAffected() == 0 {
		return nil, store.ErrVersionConflict
	}
	if commandTag.RowsAffected() != 1 {
		return nil, fmt.Errorf("updating actor %s/%s affected %d rows, want 1", atespace, name, commandTag.RowsAffected())
	}
	return dbActor, nil
}

func (p *Persistence) DeleteActor(ctx context.Context, actorRef resources.ActorRef) (*ateapipb.Actor, error) {
	atespace, name := actorRef.Atespace, actorRef.Name
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("beginning actor delete: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck // no-op once committed

	var protoBytes []byte
	err = tx.QueryRow(ctx, `
		SELECT proto FROM actors
		WHERE atespace = $1 AND name = $2
		FOR UPDATE`,
		atespace, name,
	).Scan(&protoBytes)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, store.ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("locking actor %s/%s for deletion: %w", atespace, name, err)
	}

	out := &ateapipb.Actor{}
	if err := proto.Unmarshal(protoBytes, out); err != nil {
		return nil, fmt.Errorf("unmarshaling actor for deletion: %w", err)
	}
	if out.GetStatus().GetState() != ateapipb.ActorState_ACTOR_STATE_DELETING {
		return nil, store.ErrFailedPrecondition
	}
	if _, err := tx.Exec(ctx, `DELETE FROM actors WHERE atespace = $1 AND name = $2`, atespace, name); err != nil {
		return nil, fmt.Errorf("deleting actor %s/%s: %w", atespace, name, err)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("committing actor delete: %w", err)
	}
	return out, nil
}

func (p *Persistence) ListActors(ctx context.Context, atespace string, opts store.ListOptions) (store.ListResponse[*ateapipb.Actor], error) {
	opts, err := store.NormalizeListOptions(opts)
	if err != nil {
		return store.ListResponse[*ateapipb.Actor]{}, err
	}
	var items []*ateapipb.Actor
	var nextToken string
	if atespace != "" {
		items, nextToken, err = p.listActorsScoped(ctx, atespace, opts.PageSize, opts.PageToken)
	} else {
		items, nextToken, err = p.listActorsGlobal(ctx, opts.PageSize, opts.PageToken)
	}
	if err != nil {
		return store.ListResponse[*ateapipb.Actor]{}, err
	}
	return store.ListResponse[*ateapipb.Actor]{Items: items, NextPageToken: nextToken}, nil
}

func (p *Persistence) listActorsScoped(ctx context.Context, atespace string, pageSize int32, pageTokenStr string) ([]*ateapipb.Actor, string, error) {
	token, err := decodePageToken(pageTokenStr, kindActor, atespace, 1)
	if err != nil {
		return nil, "", err
	}
	var last *string
	if len(token.Last) > 0 {
		last = &token.Last[0]
	}

	rows, err := p.pool.Query(ctx, `
		SELECT name, proto FROM actors
		WHERE atespace = $1 AND ($2::text IS NULL OR name > $2)
		ORDER BY name
		LIMIT $3`, atespace, last, int64(pageSize)+1)
	if err != nil {
		return nil, "", fmt.Errorf("listing actors in %q: %w", atespace, err)
	}
	defer rows.Close()

	var names []string
	var result []*ateapipb.Actor
	for rows.Next() {
		var name string
		var protoBytes []byte
		if err := rows.Scan(&name, &protoBytes); err != nil {
			return nil, "", fmt.Errorf("scanning actor row: %w", err)
		}
		a := &ateapipb.Actor{}
		if err := proto.Unmarshal(protoBytes, a); err != nil {
			return nil, "", fmt.Errorf("unmarshaling actor: %w", err)
		}
		result = append(result, a)
		names = append(names, name)
	}
	if err := rows.Err(); err != nil {
		return nil, "", fmt.Errorf("listing actors in %q: %w", atespace, err)
	}

	var nextToken string
	if len(result) > int(pageSize) {
		result = result[:pageSize]
		nextToken = encodePageToken(kindActor, atespace, []string{names[pageSize-1]})
	}
	return result, nextToken, nil
}

func (p *Persistence) listActorsGlobal(ctx context.Context, pageSize int32, pageTokenStr string) ([]*ateapipb.Actor, string, error) {
	token, err := decodePageToken(pageTokenStr, kindActor, "", 2)
	if err != nil {
		return nil, "", err
	}
	var lastAtespace, lastName *string
	if len(token.Last) == 2 {
		lastAtespace, lastName = &token.Last[0], &token.Last[1]
	}

	rows, err := p.pool.Query(ctx, `
		SELECT atespace, name, proto FROM actors
		WHERE $1::text IS NULL OR (atespace, name) > ($1, $2)
		ORDER BY atespace, name
		LIMIT $3`, lastAtespace, lastName, int64(pageSize)+1)
	if err != nil {
		return nil, "", fmt.Errorf("listing actors: %w", err)
	}
	defer rows.Close()

	type key struct{ atespace, name string }
	var keys []key
	var result []*ateapipb.Actor
	for rows.Next() {
		var k key
		var protoBytes []byte
		if err := rows.Scan(&k.atespace, &k.name, &protoBytes); err != nil {
			return nil, "", fmt.Errorf("scanning actor row: %w", err)
		}
		a := &ateapipb.Actor{}
		if err := proto.Unmarshal(protoBytes, a); err != nil {
			return nil, "", fmt.Errorf("unmarshaling actor: %w", err)
		}
		result = append(result, a)
		keys = append(keys, k)
	}
	if err := rows.Err(); err != nil {
		return nil, "", fmt.Errorf("listing actors: %w", err)
	}

	var nextToken string
	if len(result) > int(pageSize) {
		result = result[:pageSize]
		last := keys[pageSize-1]
		nextToken = encodePageToken(kindActor, "", []string{last.atespace, last.name})
	}
	return result, nextToken, nil
}

// --- Actor snapshots ---

func (p *Persistence) CreateActorSnapshot(ctx context.Context, snapshot *ateapipb.ActorSnapshot) (*ateapipb.ActorSnapshot, error) {
	atespace := snapshot.GetMetadata().GetAtespace()
	name := snapshot.GetMetadata().GetName()
	dbSnapshot := proto.Clone(snapshot).(*ateapipb.ActorSnapshot)
	dbSnapshot.Metadata = newCreateMetadata(atespace, name)

	protoBytes, err := proto.Marshal(dbSnapshot)
	if err != nil {
		return nil, fmt.Errorf("marshaling actor snapshot: %w", err)
	}
	if _, err := p.pool.Exec(ctx, `
		INSERT INTO actor_snapshots (atespace, name, uid, version, proto)
		VALUES ($1, $2, $3, $4, $5)`,
		atespace, name, dbSnapshot.GetMetadata().GetUid(), dbSnapshot.GetMetadata().GetVersion(), protoBytes); err != nil {
		if isUniqueViolation(err) {
			return nil, store.ErrAlreadyExists
		}
		return nil, fmt.Errorf("inserting actor snapshot %s/%s: %w", atespace, name, err)
	}
	return dbSnapshot, nil
}

func (p *Persistence) GetActorSnapshot(ctx context.Context, atespace, name string) (*ateapipb.ActorSnapshot, error) {
	var protoBytes []byte
	if err := p.pool.QueryRow(ctx, `
		SELECT proto FROM actor_snapshots
		WHERE atespace = $1 AND name = $2`, atespace, name).Scan(&protoBytes); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, store.ErrNotFound
		}
		return nil, fmt.Errorf("getting actor snapshot %s/%s: %w", atespace, name, err)
	}
	out := &ateapipb.ActorSnapshot{}
	if err := proto.Unmarshal(protoBytes, out); err != nil {
		return nil, fmt.Errorf("unmarshaling actor snapshot: %w", err)
	}
	return out, nil
}

func (p *Persistence) GetActorSnapshotTag(ctx context.Context, atespace, name string) (*ateapipb.ActorSnapshotTag, error) {
	var protoBytes []byte
	if err := p.pool.QueryRow(ctx, `
		SELECT proto FROM actor_snapshot_tags
		WHERE atespace = $1 AND name = $2`, atespace, name).Scan(&protoBytes); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, store.ErrNotFound
		}
		return nil, fmt.Errorf("getting actor snapshot tag %s/%s: %w", atespace, name, err)
	}
	tag := &ateapipb.ActorSnapshotTag{}
	if err := proto.Unmarshal(protoBytes, tag); err != nil {
		return nil, fmt.Errorf("unmarshaling actor snapshot tag: %w", err)
	}
	return tag, nil
}

func (p *Persistence) ListActorSnapshots(ctx context.Context, atespace string, opts store.ListOptions) (store.ListResponse[*ateapipb.ActorSnapshot], error) {
	opts, err := store.NormalizeListOptions(opts)
	if err != nil {
		return store.ListResponse[*ateapipb.ActorSnapshot]{}, err
	}
	var items []*ateapipb.ActorSnapshot
	var nextToken string
	if atespace != "" {
		items, nextToken, err = p.listActorSnapshotsScoped(ctx, atespace, opts.PageSize, opts.PageToken)
	} else {
		items, nextToken, err = p.listActorSnapshotsGlobal(ctx, opts.PageSize, opts.PageToken)
	}
	if err != nil {
		return store.ListResponse[*ateapipb.ActorSnapshot]{}, err
	}
	return store.ListResponse[*ateapipb.ActorSnapshot]{Items: items, NextPageToken: nextToken}, nil
}

func (p *Persistence) listActorSnapshotsScoped(ctx context.Context, atespace string, pageSize int32, pageTokenStr string) ([]*ateapipb.ActorSnapshot, string, error) {
	token, err := decodePageToken(pageTokenStr, kindSnapshot, atespace, 1)
	if err != nil {
		return nil, "", err
	}
	var last *string
	if len(token.Last) > 0 {
		last = &token.Last[0]
	}
	rows, err := p.pool.Query(ctx, `
		SELECT name, proto FROM actor_snapshots
		WHERE atespace = $1 AND ($2::text IS NULL OR name > $2)
		ORDER BY name
		LIMIT $3`, atespace, last, int64(pageSize)+1)
	if err != nil {
		return nil, "", fmt.Errorf("listing actor snapshots in %q: %w", atespace, err)
	}
	defer rows.Close()

	var names []string
	var result []*ateapipb.ActorSnapshot
	for rows.Next() {
		var name string
		var protoBytes []byte
		if err := rows.Scan(&name, &protoBytes); err != nil {
			return nil, "", fmt.Errorf("scanning actor snapshot row: %w", err)
		}
		snapshot := &ateapipb.ActorSnapshot{}
		if err := proto.Unmarshal(protoBytes, snapshot); err != nil {
			return nil, "", fmt.Errorf("unmarshaling actor snapshot: %w", err)
		}
		result = append(result, snapshot)
		names = append(names, name)
	}
	if err := rows.Err(); err != nil {
		return nil, "", fmt.Errorf("listing actor snapshots in %q: %w", atespace, err)
	}
	var nextToken string
	if len(result) > int(pageSize) {
		result = result[:pageSize]
		nextToken = encodePageToken(kindSnapshot, atespace, []string{names[pageSize-1]})
	}
	return result, nextToken, nil
}

func (p *Persistence) listActorSnapshotsGlobal(ctx context.Context, pageSize int32, pageTokenStr string) ([]*ateapipb.ActorSnapshot, string, error) {
	token, err := decodePageToken(pageTokenStr, kindSnapshot, "", 2)
	if err != nil {
		return nil, "", err
	}
	var lastAtespace, lastName *string
	if len(token.Last) == 2 {
		lastAtespace, lastName = &token.Last[0], &token.Last[1]
	}
	rows, err := p.pool.Query(ctx, `
		SELECT atespace, name, proto FROM actor_snapshots
		WHERE $1::text IS NULL OR (atespace, name) > ($1, $2)
		ORDER BY atespace, name
		LIMIT $3`, lastAtespace, lastName, int64(pageSize)+1)
	if err != nil {
		return nil, "", fmt.Errorf("listing actor snapshots: %w", err)
	}
	defer rows.Close()

	type key struct{ atespace, name string }
	var keys []key
	var result []*ateapipb.ActorSnapshot
	for rows.Next() {
		var k key
		var protoBytes []byte
		if err := rows.Scan(&k.atespace, &k.name, &protoBytes); err != nil {
			return nil, "", fmt.Errorf("scanning actor snapshot row: %w", err)
		}
		snapshot := &ateapipb.ActorSnapshot{}
		if err := proto.Unmarshal(protoBytes, snapshot); err != nil {
			return nil, "", fmt.Errorf("unmarshaling actor snapshot: %w", err)
		}
		result = append(result, snapshot)
		keys = append(keys, k)
	}
	if err := rows.Err(); err != nil {
		return nil, "", fmt.Errorf("listing actor snapshots: %w", err)
	}
	var nextToken string
	if len(result) > int(pageSize) {
		result = result[:pageSize]
		last := keys[pageSize-1]
		nextToken = encodePageToken(kindSnapshot, "", []string{last.atespace, last.name})
	}
	return result, nextToken, nil
}

func (p *Persistence) CreateActorSnapshotTag(ctx context.Context, snapshotAtespace, snapshotName string, tag *ateapipb.ActorSnapshotTag) (*ateapipb.ActorSnapshotTag, error) {
	tagAtespace := tag.GetMetadata().GetAtespace()
	tagName := tag.GetMetadata().GetName()
	dbTag := proto.Clone(tag).(*ateapipb.ActorSnapshotTag)
	dbTag.Metadata = newCreateMetadata(tagAtespace, tagName)
	dbTag.Snapshot = &ateapipb.ObjectRef{Atespace: snapshotAtespace, Name: snapshotName}
	protoBytes, err := proto.Marshal(dbTag)
	if err != nil {
		return nil, fmt.Errorf("marshaling actor snapshot tag: %w", err)
	}

	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("beginning actor snapshot tag create: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck // no-op once committed
	var inserted []byte
	err = tx.QueryRow(ctx, `
		INSERT INTO actor_snapshot_tags
		    (atespace, name, snapshot_atespace, snapshot_name, uid, version, proto)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		ON CONFLICT (atespace, name) DO NOTHING
		RETURNING proto`, tagAtespace, tagName, snapshotAtespace, snapshotName,
		dbTag.GetMetadata().GetUid(), dbTag.GetMetadata().GetVersion(), protoBytes).Scan(&inserted)
	if err == nil {
		if err := tx.Commit(ctx); err != nil {
			return nil, fmt.Errorf("committing actor snapshot tag create: %w", err)
		}
		return dbTag, nil
	}
	if isForeignKeyViolation(err) {
		switch pgErrConstraint(err) {
		case "actor_snapshot_tags_snapshot_fk":
			return nil, store.ErrNotFound
		case "actor_snapshot_tags_atespace_fk":
			return nil, store.ErrFailedPrecondition
		default:
			return nil, fmt.Errorf("inserting actor snapshot tag %s/%s violated unknown foreign key %q: %w", tagAtespace, tagName, pgErrConstraint(err), err)
		}
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return nil, fmt.Errorf("inserting actor snapshot tag %s/%s: %w", tagAtespace, tagName, err)
	}

	var existingBytes []byte
	if err := tx.QueryRow(ctx, `
		SELECT proto FROM actor_snapshot_tags
		WHERE atespace = $1 AND name = $2`, tagAtespace, tagName).Scan(&existingBytes); err != nil {
		return nil, fmt.Errorf("getting existing actor snapshot tag %s/%s: %w", tagAtespace, tagName, err)
	}
	existing := &ateapipb.ActorSnapshotTag{}
	if err := proto.Unmarshal(existingBytes, existing); err != nil {
		return nil, fmt.Errorf("unmarshaling actor snapshot tag: %w", err)
	}
	if existing.GetSnapshot().GetAtespace() != snapshotAtespace || existing.GetSnapshot().GetName() != snapshotName || existing.GetScope() != tag.GetScope() {
		return nil, store.ErrAlreadyExists
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("committing idempotent actor snapshot tag create: %w", err)
	}
	return existing, nil
}

func validateUpdateActorSnapshotTagMutation(storedTag, mutatedTag *ateapipb.ActorSnapshotTag) error {
	if stored, mutated := storedTag.GetMetadata().GetAtespace(), mutatedTag.GetMetadata().GetAtespace(); stored != mutated {
		return fmt.Errorf("metadata.atespace is immutable: mutation changed it from %q to %q", stored, mutated)
	}
	if stored, mutated := storedTag.GetMetadata().GetName(), mutatedTag.GetMetadata().GetName(); stored != mutated {
		return fmt.Errorf("metadata.name is immutable: mutation changed it from %q to %q", stored, mutated)
	}
	if stored, mutated := storedTag.GetSnapshot().GetAtespace(), mutatedTag.GetSnapshot().GetAtespace(); stored != mutated {
		return fmt.Errorf("snapshot.atespace is immutable: mutation changed it from %q to %q", stored, mutated)
	}
	if stored, mutated := storedTag.GetSnapshot().GetName(), mutatedTag.GetSnapshot().GetName(); stored != mutated {
		return fmt.Errorf("snapshot.name is immutable: mutation changed it from %q to %q", stored, mutated)
	}
	return nil
}

func (p *Persistence) UpdateActorSnapshotTag(ctx context.Context, atespace, name string, precondition store.Precondition, mutate func(*ateapipb.ActorSnapshotTag) error) (*ateapipb.ActorSnapshotTag, error) {
	if err := precondition.Validate(); err != nil {
		return nil, err
	}
	var currentUID string
	var currentVersion int64
	var currentBytes []byte
	if err := p.pool.QueryRow(ctx, `
			SELECT uid, version, proto FROM actor_snapshot_tags
			WHERE atespace = $1 AND name = $2`, atespace, name).Scan(&currentUID, &currentVersion, &currentBytes); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, store.ErrNotFound
		}
		return nil, fmt.Errorf("getting actor snapshot tag %s/%s for update: %w", atespace, name, err)
	}

	dbTag := &ateapipb.ActorSnapshotTag{}
	if err := proto.Unmarshal(currentBytes, dbTag); err != nil {
		return nil, fmt.Errorf("unmarshaling actor snapshot tag: %w", err)
	}
	if err := validateProtoMetadataMatchesColumns(fmt.Sprintf("actor snapshot tag %s/%s", atespace, name), dbTag.GetMetadata(), currentUID, currentVersion); err != nil {
		return nil, err
	}
	if err := precondition.Check(dbTag.GetMetadata()); err != nil {
		return nil, err
	}
	tagBeforeMutation := proto.Clone(dbTag).(*ateapipb.ActorSnapshotTag)
	if err := mutate(dbTag); err != nil {
		return nil, err
	}
	if err := validateUpdateActorSnapshotTagMutation(tagBeforeMutation, dbTag); err != nil {
		return nil, fmt.Errorf("%w: %w", store.ErrImmutableField, err)
	}
	// Stored metadata is authoritative; discard any metadata edits made by the
	// closure and derive the next revision from the state this attempt read.
	dbTag.Metadata = newUpdateMetadata(tagBeforeMutation.GetMetadata())

	updatedBytes, err := proto.Marshal(dbTag)
	if err != nil {
		return nil, fmt.Errorf("marshaling actor snapshot tag: %w", err)
	}
	commandTag, err := p.pool.Exec(ctx, `
			UPDATE actor_snapshot_tags
			SET version = $1, proto = $2
			WHERE atespace = $3 AND name = $4 AND uid = $5 AND version = $6`,
		dbTag.GetMetadata().GetVersion(), updatedBytes, atespace, name, currentUID, currentVersion)
	if err != nil {
		return nil, fmt.Errorf("updating actor snapshot tag %s/%s: %w", atespace, name, err)
	}
	if commandTag.RowsAffected() == 0 {
		return nil, store.ErrVersionConflict
	}
	if commandTag.RowsAffected() != 1 {
		return nil, fmt.Errorf("updating actor snapshot tag %s/%s affected %d rows, want 1", atespace, name, commandTag.RowsAffected())
	}
	return dbTag, nil
}

func (p *Persistence) DeleteActorSnapshotTag(ctx context.Context, atespace, name string) (*ateapipb.ActorSnapshotTag, error) {
	var protoBytes []byte
	if err := p.pool.QueryRow(ctx, `
		DELETE FROM actor_snapshot_tags
		WHERE atespace = $1 AND name = $2
		RETURNING proto`, atespace, name).Scan(&protoBytes); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, store.ErrNotFound
		}
		return nil, fmt.Errorf("deleting actor snapshot tag %s/%s: %w", atespace, name, err)
	}
	tag := &ateapipb.ActorSnapshotTag{}
	if err := proto.Unmarshal(protoBytes, tag); err != nil {
		return nil, fmt.Errorf("unmarshaling deleted actor snapshot tag: %w", err)
	}
	return tag, nil
}

// --- Workers ---

const (
	// workerChangeChannel is the fixed LISTEN/NOTIFY channel for worker changes.
	workerChangeChannel = "worker_changes"
	// maxNotifyPayloadBytes reflects PostgreSQL's NOTIFY payload size limit.
	// Writes fail rather than silently omit a notification if exceeded.
	maxNotifyPayloadBytes = 8000
)

type workerEventEnvelope struct {
	Type   int    `json:"t"`
	Worker string `json:"w"` // protojson-encoded Worker
}

func marshalWorkerEvent(eventType store.WorkerEventType, worker *ateapipb.Worker) ([]byte, error) {
	workerJSON, err := protojson.Marshal(worker)
	if err != nil {
		return nil, fmt.Errorf("in protojson.Marshal: %w", err)
	}
	msg, err := json.Marshal(workerEventEnvelope{Type: int(eventType), Worker: string(workerJSON)})
	if err != nil {
		return nil, fmt.Errorf("in json.Marshal: %w", err)
	}
	return msg, nil
}

func unmarshalWorkerEvent(payload string) (store.WorkerEvent, error) {
	var env workerEventEnvelope
	if err := json.Unmarshal([]byte(payload), &env); err != nil {
		return store.WorkerEvent{}, fmt.Errorf("in json.Unmarshal: %w", err)
	}
	worker := &ateapipb.Worker{}
	if err := protojson.Unmarshal([]byte(env.Worker), worker); err != nil {
		return store.WorkerEvent{}, fmt.Errorf("in protojson.Unmarshal: %w", err)
	}
	return store.WorkerEvent{Type: store.WorkerEventType(env.Type), Worker: worker}, nil
}

// writeAndNotify runs fn inside a transaction, then--only if fn reports a
// change worth notifying--calls pg_notify in the same transaction so
// delivery happens if and only if the transaction commits.
func (p *Persistence) writeAndNotify(ctx context.Context, eventType store.WorkerEventType, worker *ateapipb.Worker, fn func(ctx context.Context, tx pgx.Tx) (notify bool, err error)) error {
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("beginning transaction: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck // no-op once committed

	notify, err := fn(ctx, tx)
	if err != nil {
		return err
	}

	if notify {
		payload, err := marshalWorkerEvent(eventType, worker)
		if err != nil {
			return fmt.Errorf("marshaling worker event: %w", err)
		}
		if len(payload) > maxNotifyPayloadBytes {
			return fmt.Errorf("worker event payload of %d bytes exceeds PostgreSQL NOTIFY limit of %d bytes", len(payload), maxNotifyPayloadBytes)
		}
		if _, err := tx.Exec(ctx, `SELECT pg_notify($1, $2)`, workerChangeChannel, string(payload)); err != nil {
			return fmt.Errorf("notifying worker change: %w", err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("committing transaction: %w", err)
	}
	return nil
}

func (p *Persistence) CreateWorker(ctx context.Context, worker *ateapipb.Worker) error {
	dbWorker := proto.Clone(worker).(*ateapipb.Worker)
	// Workers are global-scoped, so the atespace is always empty.
	dbWorker.Metadata = newCreateMetadata("", worker.GetMetadata().GetName())

	protoBytes, err := proto.Marshal(dbWorker)
	if err != nil {
		return fmt.Errorf("marshaling worker: %w", err)
	}

	err = p.writeAndNotify(ctx, store.WorkerEventCreated, dbWorker, func(ctx context.Context, tx pgx.Tx) (bool, error) {
		_, err := tx.Exec(ctx, `
			INSERT INTO workers (name, uid, version, proto)
			VALUES ($1, $2, $3, $4)`,
			dbWorker.GetMetadata().GetName(), dbWorker.GetMetadata().GetUid(), dbWorker.GetMetadata().GetVersion(), protoBytes)
		if err != nil {
			return false, err
		}
		return true, nil
	})
	if err != nil {
		if isUniqueViolation(err) {
			return store.ErrAlreadyExists
		}
		return fmt.Errorf("creating worker: %w", err)
	}
	return nil
}

func getWorkerRow(ctx context.Context, q querier, name string) (*ateapipb.Worker, error) {
	var protoBytes []byte
	err := q.QueryRow(ctx, `SELECT proto FROM workers WHERE name = $1`, name).Scan(&protoBytes)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, store.ErrNotFound
		}
		return nil, fmt.Errorf("getting worker %s: %w", name, err)
	}
	out := &ateapipb.Worker{}
	if err := proto.Unmarshal(protoBytes, out); err != nil {
		return nil, fmt.Errorf("unmarshaling worker: %w", err)
	}
	return out, nil
}

func (p *Persistence) GetWorker(ctx context.Context, name string) (*ateapipb.Worker, error) {
	return getWorkerRow(ctx, p.pool, name)
}

func (p *Persistence) UpdateWorker(ctx context.Context, worker *ateapipb.Worker, expectedVersion int64) error {
	name := worker.GetMetadata().GetName()

	dbWorker := proto.Clone(worker).(*ateapipb.Worker)
	dbWorker.Metadata = newUpdateMetadata(worker.GetMetadata())
	dbWorker.Metadata.Version = expectedVersion + 1

	protoBytes, err := proto.Marshal(dbWorker)
	if err != nil {
		return fmt.Errorf("marshaling worker: %w", err)
	}

	return p.writeAndNotify(ctx, store.WorkerEventUpdated, dbWorker, func(ctx context.Context, tx pgx.Tx) (bool, error) {
		var returned []byte
		err := tx.QueryRow(ctx, `
			UPDATE workers
			SET version = $1, proto = $2
			WHERE name = $3 AND version = $4
			RETURNING proto`,
			dbWorker.GetMetadata().GetVersion(), protoBytes, name, expectedVersion,
		).Scan(&returned)
		if err == nil {
			return true, nil
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return false, fmt.Errorf("updating worker %s: %w", name, err)
		}

		current, getErr := getWorkerRow(ctx, tx, name)
		if getErr != nil {
			return false, getErr
		}
		if current.GetMetadata().GetVersion() != expectedVersion {
			return false, store.ErrVersionConflict
		}
		return false, fmt.Errorf("update worker %s: no row matched but current state is otherwise consistent", name)
	})
}

func (p *Persistence) DeleteWorker(ctx context.Context, name string) error {
	deletedEvent := &ateapipb.Worker{Metadata: &ateapipb.ResourceMetadata{Name: name}}
	return p.writeAndNotify(ctx, store.WorkerEventDeleted, deletedEvent, func(ctx context.Context, tx pgx.Tx) (bool, error) {
		var protoBytes []byte
		err := tx.QueryRow(ctx, `
			DELETE FROM workers
			WHERE name = $1
			RETURNING proto`, name).Scan(&protoBytes)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				// Idempotent: nothing existed, so nothing to notify either.
				return false, nil
			}
			return false, fmt.Errorf("deleting worker %s: %w", name, err)
		}
		return true, nil
	})
}

func (p *Persistence) ListWorkers(ctx context.Context, opts store.ListOptions) (store.ListResponse[*ateapipb.Worker], error) {
	opts, err := store.NormalizeListOptions(opts)
	if err != nil {
		return store.ListResponse[*ateapipb.Worker]{}, err
	}
	pageSize, pageTokenStr := opts.PageSize, opts.PageToken
	token, err := decodePageToken(pageTokenStr, kindWorker, "", 1)
	if err != nil {
		return store.ListResponse[*ateapipb.Worker]{}, err
	}
	var last *string
	if len(token.Last) > 0 {
		last = &token.Last[0]
	}

	rows, err := p.pool.Query(ctx, `
		SELECT name, proto FROM workers
		WHERE $1::text IS NULL OR name > $1
		ORDER BY name
		LIMIT $2`, last, int64(pageSize)+1)
	if err != nil {
		return store.ListResponse[*ateapipb.Worker]{}, fmt.Errorf("listing workers: %w", err)
	}
	defer rows.Close()

	var names []string
	var result []*ateapipb.Worker
	for rows.Next() {
		var name string
		var protoBytes []byte
		if err := rows.Scan(&name, &protoBytes); err != nil {
			return store.ListResponse[*ateapipb.Worker]{}, fmt.Errorf("scanning worker row: %w", err)
		}
		w := &ateapipb.Worker{}
		if err := proto.Unmarshal(protoBytes, w); err != nil {
			return store.ListResponse[*ateapipb.Worker]{}, fmt.Errorf("unmarshaling worker: %w", err)
		}
		result = append(result, w)
		names = append(names, name)
	}
	if err := rows.Err(); err != nil {
		return store.ListResponse[*ateapipb.Worker]{}, fmt.Errorf("listing workers: %w", err)
	}

	var nextToken string
	if len(result) > int(pageSize) {
		result = result[:pageSize]
		nextToken = encodePageToken(kindWorker, "", []string{names[pageSize-1]})
	}
	return store.ListResponse[*ateapipb.Worker]{Items: result, NextPageToken: nextToken}, nil
}

// WatchWorkers acquires a dedicated connection (hijacked out of the pool, so
// it's never handed back for unrelated queries), LISTENs on the fixed
// worker-change channel, and forwards decoded notifications until the
// context is cancelled or the caller closes the watch.
func (p *Persistence) WatchWorkers(ctx context.Context) (*store.WorkerWatch, error) {
	watchCtx, cancel := context.WithCancel(ctx)

	poolConn, err := p.pool.Acquire(watchCtx)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("acquiring watch connection: %w", err)
	}
	conn := poolConn.Hijack()

	if _, err := conn.Exec(watchCtx, "LISTEN "+workerChangeChannel); err != nil {
		conn.Close(watchCtx) //nolint:errcheck
		cancel()
		return nil, fmt.Errorf("listening for worker changes: %w", err)
	}

	ch := make(chan store.WorkerEvent, 128)
	go func() {
		defer close(ch)
		defer conn.Close(context.Background()) //nolint:errcheck
		for {
			notification, err := conn.WaitForNotification(watchCtx)
			if err != nil {
				// Context cancelled (caller closed the watch) or the
				// connection was lost. Either way, the caller must
				// re-subscribe; matches ateredis's WatchWorkers contract.
				return
			}
			event, err := unmarshalWorkerEvent(notification.Payload)
			if err != nil {
				slog.ErrorContext(ctx, "worker event unmarshal failed; closing watch", slog.Any("err", err))
				return
			}
			select {
			case ch <- event:
			case <-watchCtx.Done():
				return
			}
		}
	}()
	return store.NewWorkerWatch(ch, cancel), nil
}

// --- Workflow locks ---

// defaultLockTTL is how long a lock may go unrenewed before another client
// can reclaim it.
const defaultLockTTL = 30 * time.Second

func (p *Persistence) AcquireLock(ctx context.Context, key string) (*store.Lock, error) {
	ttl := p.lockTTL
	token := uuid.NewString()
	if err := p.cleanupExpiredLeases(ctx); err != nil {
		slog.WarnContext(ctx, "failed to clean up expired PostgreSQL leases", "error", err)
	}

	acquired, err := p.acquireLease(ctx, key, token, ttl)
	if err != nil {
		return nil, err
	}
	if !acquired {
		return nil, store.ErrLockConflict
	}

	leaseCtx, cancel := context.WithCancel(ctx)
	renewalDone := make(chan struct{})
	go func() {
		defer close(renewalDone)
		defer cancel()
		p.renewLockLoop(leaseCtx, key, token, ttl)
	}()

	closeFn := func() {
		cancel()
		<-renewalDone

		releaseCtx, releaseCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer releaseCancel()
		if err := p.releaseLease(releaseCtx, key, token); err != nil {
			slog.WarnContext(releaseCtx, "failed to release PostgreSQL lock, relying on TTL to reclaim it", "key", key, "error", err)
		}
	}
	return store.NewLock(leaseCtx, closeFn), nil
}

func (p *Persistence) cleanupExpiredLeases(ctx context.Context) error {
	if _, err := p.pool.Exec(ctx, `DELETE FROM leases WHERE expires_at <= clock_timestamp()`); err != nil {
		return fmt.Errorf("deleting expired leases: %w", err)
	}
	return nil
}

func (p *Persistence) acquireLease(ctx context.Context, key, token string, ttl time.Duration) (bool, error) {
	var returnedKey string
	err := p.pool.QueryRow(ctx, `
		INSERT INTO leases (key, token, expires_at)
		VALUES ($1, $2, clock_timestamp() + make_interval(secs => $3))
		ON CONFLICT (key) DO UPDATE
		SET token = EXCLUDED.token,
		    expires_at = EXCLUDED.expires_at
		WHERE leases.expires_at <= clock_timestamp()
		RETURNING key`, key, token, ttl.Seconds()).Scan(&returnedKey)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return false, nil
		}
		return false, fmt.Errorf("acquiring lock for %q: %w", key, err)
	}
	return true, nil
}

const (
	renewIntervalDivisor    = 3
	renewRetryPeriodDivisor = 10
	renewDeadlineFraction   = 2.0 / 3.0
)

func (p *Persistence) renewLockLoop(ctx context.Context, key, token string, ttl time.Duration) {
	interval := ttl / renewIntervalDivisor
	renewDeadline := time.Duration(float64(ttl) * renewDeadlineFraction)

	lastRenewed := time.Now()
	timer := time.NewTimer(interval)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			renewCtx, cancel := context.WithDeadline(ctx, lastRenewed.Add(renewDeadline))
			renewed := p.tryRenewLease(renewCtx, key, token, ttl)
			cancel()
			if !renewed {
				return
			}
			lastRenewed = time.Now()
			timer.Reset(interval)
		}
	}
}

func (p *Persistence) tryRenewLease(ctx context.Context, key, token string, ttl time.Duration) bool {
	retryPeriod := ttl / renewRetryPeriodDivisor
	retry := time.NewTimer(0)
	defer retry.Stop()

	for {
		select {
		case <-ctx.Done():
			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				slog.WarnContext(ctx, "failed to renew PostgreSQL lock before its deadline", "key", key)
			}
			return false
		case <-retry.C:
			renewed, err := p.renewLease(ctx, key, token, ttl)
			if ctx.Err() != nil {
				return false
			}
			switch {
			case err == nil && renewed:
				return true
			case err == nil:
				slog.WarnContext(ctx, "PostgreSQL lock renewal found lease no longer owned", "key", key)
				return false
			default:
				slog.WarnContext(ctx, "failed to renew PostgreSQL lock, retrying", "key", key, "error", err)
				retry.Reset(retryPeriod)
			}
		}
	}
}

func (p *Persistence) renewLease(ctx context.Context, key, token string, ttl time.Duration) (bool, error) {
	var returnedKey string
	err := p.pool.QueryRow(ctx, `
		UPDATE leases
		SET expires_at = clock_timestamp() + make_interval(secs => $3)
		WHERE key = $1 AND token = $2 AND expires_at > clock_timestamp()
		RETURNING key`, key, token, ttl.Seconds()).Scan(&returnedKey)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return false, nil
		}
		return false, fmt.Errorf("renewing lock for %q: %w", key, err)
	}
	return true, nil
}

func (p *Persistence) releaseLease(ctx context.Context, key, token string) error {
	if _, err := p.pool.Exec(ctx, `DELETE FROM leases WHERE key = $1 AND token = $2`, key, token); err != nil {
		return fmt.Errorf("releasing lock for %q: %w", key, err)
	}
	return nil
}

// --- Debug ---

func (p *Persistence) DebugClearAll(ctx context.Context) error {
	if _, err := p.pool.Exec(ctx, `TRUNCATE atespaces, actors, actor_templates, actor_snapshots, actor_snapshot_tags, workers, leases`); err != nil {
		return fmt.Errorf("truncating tables: %w", err)
	}
	return nil
}

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
	"github.com/jackc/pgx/v5/pgconn"
	"go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/cmd/ateapi/internal/store"
	"go.ate.dev/substrate/internal/resources"
	"google.golang.org/protobuf/proto"
)

func (p *Persistence) CreateActor(ctx context.Context, actor *ateapi.Actor) (*ateapi.Actor, error) {
	atespace := actor.GetMetadata().GetAtespace()
	name := actor.GetMetadata().GetName()

	// TODO: doing a full clone here is wasteful - the caller already has to
	// make modifications to the actor before passing it in, so we can safely
	// mutate it in place.  This breaks some of the contract tests, so we can
	// fix it later.
	dbActor := proto.Clone(actor).(*ateapi.Actor)
	setCreateMetadata(dbActor.Metadata)
	if err := validateSnapshotURIs("actor "+resources.ActorRefFromActor(dbActor).String(), dbActor.GetStatus().GetSnapshots()...); err != nil {
		return nil, err
	}

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

func (p *Persistence) GetActor(ctx context.Context, actorRef resources.ActorRef) (*ateapi.Actor, error) {
	var protoBytes []byte
	err := p.pool.QueryRow(ctx, `SELECT proto FROM actors WHERE atespace = $1 AND name = $2`, actorRef.Atespace, actorRef.Name).Scan(&protoBytes)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, store.ErrNotFound
		}
		return nil, fmt.Errorf("getting actor %s/%s: %w", actorRef.Atespace, actorRef.Name, err)
	}
	out := &ateapi.Actor{}
	if err := unmarshalStored(protoBytes, out); err != nil {
		return nil, fmt.Errorf("unmarshaling actor: %w", err)
	}
	return out, nil
}

func (p *Persistence) UpdateActor(ctx context.Context, actorRef resources.ActorRef, precondition store.Precondition, mutate func(*ateapi.Actor) error) (*ateapi.Actor, error) {
	if err := precondition.ValidateRequired(); err != nil {
		return nil, err
	}
	return updateActor(ctx, p.pool, actorRef, precondition, mutate, false)
}

func (p *Persistence) MarkActorForDeletion(ctx context.Context, actorRef resources.ActorRef, precondition store.Precondition, mutate func(*ateapi.Actor) error) (*ateapi.Actor, error) {
	return updateActor(ctx, p.pool, actorRef, precondition, mutate, true)
}

// updateActor applies mutate to the stored actor through q, which is the pool
// or a transaction the write must commit with.
func updateActor(ctx context.Context, q querier, actorRef resources.ActorRef, precondition store.Precondition, mutate func(*ateapi.Actor) error, markForDeletion bool) (*ateapi.Actor, error) {
	current, err := readActorForUpdate(q.QueryRow(ctx, actorForUpdateSQL, actorRef.Atespace, actorRef.Name), actorRef)
	if err != nil {
		return nil, err
	}
	next, err := current.apply(actorRef, precondition, mutate, markForDeletion)
	if err != nil {
		return nil, err
	}
	commandTag, err := q.Exec(ctx, updateActorSQL, next.args(actorRef)...)
	if err != nil {
		return nil, fmt.Errorf("updating actor %s/%s: %w", actorRef.Atespace, actorRef.Name, err)
	}
	if err := next.check(actorRef, commandTag); err != nil {
		return nil, err
	}
	return next.actor, nil
}

const (
	actorForUpdateSQL = `
			SELECT uid, version, proto FROM actors
			WHERE atespace = $1 AND name = $2`
	updateActorSQL = `
			UPDATE actors
			SET version = $1, proto = $2
			WHERE atespace = $3 AND name = $4 AND uid = $5 AND version = $6`
)

// readActor is the row an update starts from: the columns the UPDATE matches
// on and the decoded proto mutate runs against.
type readActor struct {
	uid     string
	version int64
	actor   *ateapi.Actor
}

// readActorForUpdate scans row, a result of actorForUpdateSQL.
func readActorForUpdate(row pgx.Row, actorRef resources.ActorRef) (readActor, error) {
	var current readActor
	var currentBytes []byte
	if err := row.Scan(&current.uid, &current.version, &currentBytes); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return readActor{}, store.ErrNotFound
		}
		return readActor{}, fmt.Errorf("getting actor %s/%s for update: %w", actorRef.Atespace, actorRef.Name, err)
	}
	current.actor = &ateapi.Actor{}
	if err := unmarshalStored(currentBytes, current.actor); err != nil {
		return readActor{}, fmt.Errorf("unmarshaling actor for update: %w", err)
	}
	if err := validateProtoMetadataMatchesColumns("actor "+actorRef.String(), current.actor.GetMetadata(), current.uid, current.version); err != nil {
		return readActor{}, err
	}
	return current, nil
}

// actorUpdate is the write an update ends with: the next revision and the
// row it replaces.
type actorUpdate struct {
	read  readActor
	actor *ateapi.Actor
	bytes []byte
}

// apply checks precondition against the row read and runs mutate on it.
func (current readActor) apply(actorRef resources.ActorRef, precondition store.Precondition, mutate func(*ateapi.Actor) error, markForDeletion bool) (actorUpdate, error) {
	dbActor := current.actor
	if err := precondition.Check(dbActor.GetMetadata()); err != nil {
		return actorUpdate{}, err
	}
	oldMeta := proto.CloneOf(dbActor.Metadata)
	if mutate != nil {
		if err := mutate(dbActor); err != nil {
			return actorUpdate{}, err
		}
	}
	if err := validateSnapshotURIs("actor "+actorRef.String(), dbActor.GetStatus().GetSnapshots()...); err != nil {
		return actorUpdate{}, err
	}
	// Stored metadata is authoritative; discard any metadata edits made by the
	// closure and derive the next revision from the state this attempt read.
	dbActor.Metadata = setUpdateMetadata(oldMeta)
	if markForDeletion && dbActor.Metadata.DeleteTime == nil {
		dbActor.Metadata.DeleteTime = proto.CloneOf(dbActor.Metadata.UpdateTime)
	}
	updatedBytes, err := proto.Marshal(dbActor)
	if err != nil {
		return actorUpdate{}, fmt.Errorf("marshaling actor: %w", err)
	}
	return actorUpdate{read: current, actor: dbActor, bytes: updatedBytes}, nil
}

// args are the arguments of updateActorSQL.
func (u actorUpdate) args(actorRef resources.ActorRef) []any {
	return []any{u.actor.GetMetadata().GetVersion(), u.bytes, actorRef.Atespace, actorRef.Name, u.read.uid, u.read.version}
}

// check reads the UPDATE's command tag: no row means the row moved on since
// it was read.
func (u actorUpdate) check(actorRef resources.ActorRef, commandTag pgconn.CommandTag) error {
	if commandTag.RowsAffected() == 0 {
		return store.ErrVersionConflict
	}
	if commandTag.RowsAffected() != 1 {
		return fmt.Errorf("updating actor %s/%s affected %d rows, want 1", actorRef.Atespace, actorRef.Name, commandTag.RowsAffected())
	}
	return nil
}

func (p *Persistence) DeleteActor(ctx context.Context, actorRef resources.ActorRef, precondition store.Precondition) (*ateapi.Actor, error) {
	atespace, name := actorRef.Atespace, actorRef.Name
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("beginning actor delete: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck // no-op once committed

	// Locked so the Actor checked is the one the DELETE removes.
	var protoBytes []byte
	err = tx.QueryRow(ctx, `SELECT proto FROM actors WHERE atespace = $1 AND name = $2 FOR UPDATE`, atespace, name).Scan(&protoBytes)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, store.ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("locking actor %s/%s: %w", atespace, name, err)
	}
	deleted := &ateapi.Actor{}
	if err := unmarshalStored(protoBytes, deleted); err != nil {
		return nil, fmt.Errorf("unmarshaling actor to delete: %w", err)
	}
	if err := precondition.Check(deleted.GetMetadata()); err != nil {
		return nil, err
	}
	if deleted.GetStatus().GetWorkerAssignment() != nil {
		return nil, fmt.Errorf("%w: actor %s/%s is still assigned to a worker", store.ErrFailedPrecondition, atespace, name)
	}
	if _, err := tx.Exec(ctx, `DELETE FROM actors WHERE atespace = $1 AND name = $2`, atespace, name); err != nil {
		return nil, fmt.Errorf("deleting actor %s/%s: %w", atespace, name, err)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("committing actor delete %s/%s: %w", atespace, name, err)
	}
	return deleted, nil
}

func (p *Persistence) ListActors(ctx context.Context, atespace string, opts store.ListOptions) (store.ListResponse[*ateapi.Actor], error) {
	opts, err := store.NormalizeListOptions(opts)
	if err != nil {
		return store.ListResponse[*ateapi.Actor]{}, err
	}
	var items []*ateapi.Actor
	var nextToken string
	if atespace != "" {
		items, nextToken, err = p.listActorsScoped(ctx, atespace, opts.PageSize, opts.PageToken)
	} else {
		items, nextToken, err = p.listActorsGlobal(ctx, opts.PageSize, opts.PageToken)
	}
	if err != nil {
		return store.ListResponse[*ateapi.Actor]{}, err
	}
	return store.ListResponse[*ateapi.Actor]{Items: items, NextPageToken: nextToken}, nil
}

func (p *Persistence) listActorsScoped(ctx context.Context, atespace string, pageSize int32, pageTokenStr string) ([]*ateapi.Actor, string, error) {
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
	var result []*ateapi.Actor
	for rows.Next() {
		var name string
		var protoBytes []byte
		if err := rows.Scan(&name, &protoBytes); err != nil {
			return nil, "", fmt.Errorf("scanning actor row: %w", err)
		}
		a := &ateapi.Actor{}
		if err := unmarshalRow(protoBytes, a, "actor", atespace, name); err != nil {
			return nil, "", err
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

func (p *Persistence) listActorsGlobal(ctx context.Context, pageSize int32, pageTokenStr string) ([]*ateapi.Actor, string, error) {
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
	var result []*ateapi.Actor
	for rows.Next() {
		var k key
		var protoBytes []byte
		if err := rows.Scan(&k.atespace, &k.name, &protoBytes); err != nil {
			return nil, "", fmt.Errorf("scanning actor row: %w", err)
		}
		a := &ateapi.Actor{}
		if err := unmarshalRow(protoBytes, a, "actor", k.atespace, k.name); err != nil {
			return nil, "", err
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

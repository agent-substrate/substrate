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

func (p *Persistence) CreateActor(ctx context.Context, actor *ateapipb.Actor) (*ateapipb.Actor, error) {
	atespace := actor.GetMetadata().GetAtespace()
	name := actor.GetMetadata().GetName()

	// TODO: doing a full clone here is wasteful - the caller already has to
	// make modifications to the actor before passing it in, so we can safely
	// mutate it in place.  This breaks some of the contract tests, so we can
	// fix it later.
	dbActor := proto.Clone(actor).(*ateapipb.Actor)
	setCreateMetadata(dbActor.Metadata)

	protoBytes, err := proto.Marshal(dbActor)
	if err != nil {
		return nil, fmt.Errorf("marshaling actor: %w", err)
	}

	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("beginning actor create: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck // no-op once committed

	_, err = tx.Exec(ctx, `
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
	if err := updateTagBorrow(ctx, tx, dbActor.GetMetadata().GetUid(), "", dbActor.GetStatus().GetExternalSnapshot().GetSnapshotUri()); err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("committing actor create: %w", err)
	}
	return dbActor, nil
}

// updateTagBorrow records the change of the actor's external snapshot from
// prevURI to newURI in tag_borrows. It is a no-op unless the Tag being
// borrowed changes.
//
// A new borrow is refused with ErrTagNotReady unless the Tag exists and is
// READY. The Tag row is share-locked, so a concurrent move to DELETING either
// waits for this borrow to commit and then sees it, or commits first and is
// seen here.
func updateTagBorrow(ctx context.Context, tx pgx.Tx, actorUID, prevURI, newURI string) error {
	newTagAtespace, newTagUID, err := borrowedTag(newURI)
	if err != nil {
		return fmt.Errorf("reading the external snapshot of actor %s: %w", actorUID, err)
	}
	if _, prevTagUID, err := borrowedTag(prevURI); err == nil && prevTagUID == newTagUID {
		// Tag didn't change, there's nothing to update.
		return nil
	}

	// New snapshot belongs to an actor, not to a tag.
	// We delete all previous borrowed tags from the actor in that case.
	if newTagUID == "" {
		if _, err := tx.Exec(ctx, `DELETE FROM tag_borrows WHERE actor_uid = $1`, actorUID); err != nil {
			return fmt.Errorf("clearing the tag borrow of actor %s: %w", actorUID, err)
		}
		return nil
	}

	// 1. Share-lock the Tag row. A concurrent UpdateTag moving the Tag to
	// DELETING needs the row lock, so it waits for this transaction to commit
	// and then sees the borrow in tag_borrows. If it committed first, this read
	// sees DELETING. Either way a Tag can't be deleted while it is borrowed.
	var tagBytes []byte
	err = tx.QueryRow(ctx, `
		SELECT proto FROM tags
		WHERE atespace = $1 AND uid = $2
		FOR SHARE`, newTagAtespace, newTagUID).Scan(&tagBytes)
	if errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("actor %s borrows missing tag %s: %w", actorUID, newTagUID, store.ErrTagNotReady)
	}
	if err != nil {
		return fmt.Errorf("locking tag %s borrowed by actor %s: %w", newTagUID, actorUID, err)
	}
	// 2. Refuse to borrow a Tag that isn't READY.
	tag := &ateapipb.Tag{}
	if err := unmarshalStored(tagBytes, tag); err != nil {
		return fmt.Errorf("unmarshaling tag %s: %w", newTagUID, err)
	}
	if state := tag.GetStatus().GetState(); state != ateapipb.TagState_TAG_STATE_READY {
		return fmt.Errorf("actor %s borrows tag %s in state %v: %w", actorUID, newTagUID, state, store.ErrTagNotReady)
	}

	// 3. Record the borrow, replacing any Tag the actor borrowed before.
	if _, err := tx.Exec(ctx, `
		INSERT INTO tag_borrows (actor_uid, tag_uid)
		VALUES ($1, $2)
		ON CONFLICT (actor_uid) DO UPDATE SET tag_uid = $2`,
		actorUID, newTagUID); err != nil {
		return fmt.Errorf("recording the borrow of tag %s by actor %s: %w", newTagUID, actorUID, err)
	}
	return nil
}

// borrowedTag returns the atespace and UID of the Tag that owns the snapshot
// at snapshotURI, or a zero UID if there is no snapshot or a Tag does not own
// it.
func borrowedTag(snapshotURI string) (atespace, uid string, err error) {
	if snapshotURI == "" {
		return "", "", nil
	}
	uri, err := resources.ParseSnapshotURI(snapshotURI)
	if err != nil {
		return "", "", err
	}
	owner := uri.Owner()
	if uid, ok := owner.TagUID(); ok {
		return owner.Atespace(), uid, nil
	}

	// Snapshot is owned by an actor
	return "", "", nil
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
	if err := unmarshalStored(protoBytes, out); err != nil {
		return nil, fmt.Errorf("unmarshaling actor: %w", err)
	}
	return out, nil
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
	if err := unmarshalStored(currentBytes, dbActor); err != nil {
		return nil, fmt.Errorf("unmarshaling actor for update: %w", err)
	}
	if err := validateProtoMetadataMatchesColumns("actor "+actorRef.String(), dbActor.GetMetadata(), currentUID, currentVersion); err != nil {
		return nil, err
	}
	if err := precondition.Check(dbActor.GetMetadata()); err != nil {
		return nil, err
	}
	oldMeta := proto.CloneOf(dbActor.Metadata)
	prevSnapshotURI := dbActor.GetStatus().GetExternalSnapshot().GetSnapshotUri()
	if err := mutate(dbActor); err != nil {
		return nil, err
	}
	// Stored metadata is authoritative; discard any metadata edits made by the
	// closure and derive the next revision from the state this attempt read.
	setUpdateMetadata(dbActor.Metadata, oldMeta)

	updatedBytes, err := proto.Marshal(dbActor)
	if err != nil {
		return nil, fmt.Errorf("marshaling actor: %w", err)
	}
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("beginning actor update: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck // no-op once committed

	commandTag, err := tx.Exec(ctx, `
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
	if err := updateTagBorrow(ctx, tx, currentUID, prevSnapshotURI, dbActor.GetStatus().GetExternalSnapshot().GetSnapshotUri()); err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("committing actor update: %w", err)
	}
	return dbActor, nil
}

func (p *Persistence) DeleteActor(ctx context.Context, actorRef resources.ActorRef, precondition store.DeletePreconditions) (*ateapipb.Actor, error) {
	atespace, name := actorRef.Atespace, actorRef.Name
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("beginning actor delete: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck // no-op once committed

	var protoBytes []byte
	err = tx.QueryRow(ctx, `
		DELETE FROM actors
		WHERE atespace = $1 AND name = $2
		  AND ($3::text = '' OR uid = $3::text)
		  AND ($4::bigint = 0 OR version = $4::bigint)
		RETURNING proto`, atespace, name, precondition.UID, precondition.Version).Scan(&protoBytes)
	if errors.Is(err, pgx.ErrNoRows) {
		var uid string
		var version int64
		err := tx.QueryRow(ctx, `SELECT uid, version FROM actors WHERE atespace = $1 AND name = $2`, atespace, name).Scan(&uid, &version)
		return nil, mapDeleteError(err, uid, version, precondition)
	}
	if err != nil {
		return nil, fmt.Errorf("deleting actor %s/%s: %w", atespace, name, err)
	}
	out := &ateapipb.Actor{}
	if err := unmarshalStored(protoBytes, out); err != nil {
		return nil, fmt.Errorf("unmarshaling deleted actor: %w", err)
	}
	if _, err := tx.Exec(ctx, `DELETE FROM tag_borrows WHERE actor_uid = $1`, out.GetMetadata().GetUid()); err != nil {
		return nil, fmt.Errorf("clearing the tag borrow of actor %s/%s: %w", atespace, name, err)
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
		if err := unmarshalStored(protoBytes, a); err != nil {
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
		if err := unmarshalStored(protoBytes, a); err != nil {
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

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

func (p *Persistence) CreateActor(ctx context.Context, actor *ateapipb.Actor) (*ateapipb.Actor, error) {
	atespace := actor.GetMetadata().GetAtespace()
	name := actor.GetMetadata().GetName()

	dbActor := proto.Clone(actor).(*ateapipb.Actor)
	storesql.SetCreateMetadata(dbActor.Metadata)

	protoBytes, err := proto.Marshal(dbActor)
	if err != nil {
		return nil, fmt.Errorf("marshaling actor: %w", err)
	}

	err = p.insertChild(ctx, "actor "+atespace+"/"+name, func(tx *sql.Tx) error { return lockAtespace(ctx, tx, atespace) }, `
		INSERT INTO actors (atespace, name, uid, version, proto)
		VALUES (?, ?, ?, ?, ?)`,
		atespace, name, dbActor.GetMetadata().GetUid(), dbActor.GetMetadata().GetVersion(), protoBytes)
	if err != nil {
		return nil, err
	}
	return dbActor, nil
}

func (p *Persistence) GetActor(ctx context.Context, actorRef resources.ActorRef) (*ateapipb.Actor, error) {
	var protoBytes []byte
	err := p.db.QueryRowContext(ctx, `SELECT proto FROM actors WHERE atespace = ? AND name = ?`, actorRef.Atespace, actorRef.Name).Scan(&protoBytes)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, store.ErrNotFound
		}
		return nil, fmt.Errorf("getting actor %s/%s: %w", actorRef.Atespace, actorRef.Name, err)
	}
	out := &ateapipb.Actor{}
	if err := storesql.UnmarshalStored(protoBytes, out); err != nil {
		return nil, fmt.Errorf("unmarshaling actor: %w", err)
	}
	return out, nil
}

// UpdateActor is a compare-and-set on uid and version rather than a locked
// read, so mutate never runs while a row lock is held.
func (p *Persistence) UpdateActor(ctx context.Context, actorRef resources.ActorRef, precondition store.Precondition, mutate func(*ateapipb.Actor) error) (*ateapipb.Actor, error) {
	if err := precondition.Validate(); err != nil {
		return nil, err
	}
	atespace, name := actorRef.Atespace, actorRef.Name
	var currentUID string
	var currentVersion int64
	var currentBytes []byte
	if err := p.db.QueryRowContext(ctx, `
			SELECT uid, version, proto FROM actors
			WHERE atespace = ? AND name = ?`, atespace, name).Scan(&currentUID, &currentVersion, &currentBytes); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, store.ErrNotFound
		}
		return nil, fmt.Errorf("getting actor %s/%s for update: %w", atespace, name, err)
	}

	dbActor := &ateapipb.Actor{}
	if err := storesql.UnmarshalStored(currentBytes, dbActor); err != nil {
		return nil, fmt.Errorf("unmarshaling actor for update: %w", err)
	}
	if err := storesql.ValidateProtoMetadataMatchesColumns("actor "+actorRef.String(), dbActor.GetMetadata(), currentUID, currentVersion); err != nil {
		return nil, err
	}
	if err := precondition.Check(dbActor.GetMetadata()); err != nil {
		return nil, err
	}
	oldMeta := proto.CloneOf(dbActor.Metadata)
	if err := mutate(dbActor); err != nil {
		return nil, err
	}
	// Stored metadata is authoritative; discard any metadata edits made by the
	// closure and derive the next revision from the state this attempt read.
	storesql.SetUpdateMetadata(dbActor.Metadata, oldMeta)

	updatedBytes, err := proto.Marshal(dbActor)
	if err != nil {
		return nil, fmt.Errorf("marshaling actor: %w", err)
	}
	if err := updateGuarded(ctx, p.db, "actor "+actorRef.String(), `
			UPDATE actors
			SET version = ?, proto = ?
			WHERE atespace = ? AND name = ? AND uid = ? AND version = ?`,
		dbActor.GetMetadata().GetVersion(), updatedBytes, atespace, name, currentUID, currentVersion); err != nil {
		return nil, err
	}
	return dbActor, nil
}

// DeleteActor removes the actor's egress policy with it.
func (p *Persistence) DeleteActor(ctx context.Context, actorRef resources.ActorRef, precondition store.DeletePreconditions) (*ateapipb.Actor, error) {
	atespace, name := actorRef.Atespace, actorRef.Name
	var protoBytes []byte
	err := inTx(ctx, p.db, func(tx *sql.Tx) error {
		var err error
		_, _, protoBytes, err = deleteLockedRow(ctx, tx, "actors", "atespace = ? AND name = ?", precondition, atespace, name)
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM actor_egress_policies WHERE atespace = ? AND actor_name = ?`, atespace, name); err != nil {
			return fmt.Errorf("deleting egress policy of actor %s/%s: %w", atespace, name, err)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	out := &ateapipb.Actor{}
	if err := storesql.UnmarshalStored(protoBytes, out); err != nil {
		return nil, fmt.Errorf("unmarshaling deleted actor: %w", err)
	}
	return out, nil
}

func (p *Persistence) ListActors(ctx context.Context, atespace string, opts store.ListOptions) (store.ListResponse[*ateapipb.Actor], error) {
	opts, err := store.NormalizeListOptions(opts)
	if err != nil {
		return store.ListResponse[*ateapipb.Actor]{}, err
	}
	decode := func(b []byte, keys []string) (*ateapipb.Actor, error) {
		a := &ateapipb.Actor{}
		return a, storesql.UnmarshalRow(b, a, "actor", rowID(atespace, keys)...)
	}
	items, next, err := listScoped(ctx, p.db, "actors", storesql.KindActor, atespace, opts, decode)
	if err != nil {
		if errors.Is(err, store.ErrInvalidPageToken) {
			return store.ListResponse[*ateapipb.Actor]{}, err
		}
		return store.ListResponse[*ateapipb.Actor]{}, fmt.Errorf("listing actors: %w", err)
	}
	return store.ListResponse[*ateapipb.Actor]{Items: items, NextPageToken: next}, nil
}

// rowID names a row of an atespace-scoped listing in errors. A scoped listing
// selects only the name; a global one selects atespace and name.
func rowID(atespace string, keys []string) []string {
	if atespace != "" {
		return append([]string{atespace}, keys...)
	}
	return keys
}

// listScoped pages through an atespace-owned table keyed by (atespace, name):
// one atespace ordered by name, or every atespace ordered by (atespace, name)
// when atespace is empty. table is a trusted SQL identifier.
func listScoped[T any](ctx context.Context, q querier, table string, kind storesql.Kind, atespace string, opts store.ListOptions, decode func(b []byte, keys []string) (T, error)) ([]T, string, error) {
	limit := int64(opts.PageSize) + 1
	if atespace != "" {
		token, err := storesql.DecodePageToken(opts.PageToken, kind, atespace, 1)
		if err != nil {
			return nil, "", err
		}
		query, args := `SELECT name, proto FROM `+table+` WHERE atespace = ? ORDER BY name LIMIT ?`, []any{atespace, limit}
		if len(token.Last) == 1 {
			query, args = `SELECT name, proto FROM `+table+` WHERE atespace = ? AND name > ? ORDER BY name LIMIT ?`, []any{atespace, token.Last[0], limit}
		}
		return listPage(ctx, q, kind, atespace, opts.PageSize, decode, query, args...)
	}
	token, err := storesql.DecodePageToken(opts.PageToken, kind, "", 2)
	if err != nil {
		return nil, "", err
	}
	query, args := `SELECT atespace, name, proto FROM `+table+` ORDER BY atespace, name LIMIT ?`, []any{limit}
	if len(token.Last) == 2 {
		// Expanded rather than a row comparison so MySQL plans a primary
		// key range scan.
		query = `SELECT atespace, name, proto FROM ` + table + `
			WHERE atespace > ? OR (atespace = ? AND name > ?)
			ORDER BY atespace, name LIMIT ?`
		args = []any{token.Last[0], token.Last[0], token.Last[1], limit}
	}
	return listPage(ctx, q, kind, "", opts.PageSize, decode, query, args...)
}

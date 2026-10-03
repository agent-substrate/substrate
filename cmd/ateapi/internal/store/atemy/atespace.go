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

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/authz"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store/storesql"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/protobuf/proto"
)

func (p *Persistence) CreateAtespace(ctx context.Context, atespace *ateapipb.Atespace) (*ateapipb.Atespace, error) {
	name := atespace.GetMetadata().GetName()

	dbAtespace := proto.Clone(atespace).(*ateapipb.Atespace)
	storesql.SetCreateMetadata(dbAtespace.Metadata)

	protoBytes, err := proto.Marshal(dbAtespace)
	if err != nil {
		return nil, fmt.Errorf("marshaling atespace: %w", err)
	}

	err = inTx(ctx, p.db, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `
			INSERT INTO atespaces (name, uid, version, proto)
			VALUES (?, ?, ?, ?)`,
			name, dbAtespace.GetMetadata().GetUid(), dbAtespace.GetMetadata().GetVersion(), protoBytes)
		if isUniqueViolation(err) {
			return store.ErrAlreadyExists
		}
		if err != nil {
			return fmt.Errorf("inserting atespace %q: %w", name, err)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return dbAtespace, nil
}

func (p *Persistence) GetAtespace(ctx context.Context, name string) (*ateapipb.Atespace, error) {
	var protoBytes []byte
	err := p.db.QueryRowContext(ctx, `SELECT proto FROM atespaces WHERE name = ?`, name).Scan(&protoBytes)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, store.ErrNotFound
		}
		return nil, fmt.Errorf("getting atespace %q: %w", name, err)
	}
	out := &ateapipb.Atespace{}
	if err := storesql.UnmarshalStored(protoBytes, out); err != nil {
		return nil, fmt.Errorf("unmarshaling atespace: %w", err)
	}
	return out, nil
}

func (p *Persistence) ListAtespaces(ctx context.Context, opts store.ListOptions) (store.ListResponse[*ateapipb.Atespace], error) {
	opts, err := store.NormalizeListOptions(opts)
	if err != nil {
		return store.ListResponse[*ateapipb.Atespace]{}, err
	}
	token, err := storesql.DecodePageToken(opts.PageToken, storesql.KindAtespace, "", 1)
	if err != nil {
		return store.ListResponse[*ateapipb.Atespace]{}, err
	}
	query, args := `SELECT name, proto FROM atespaces ORDER BY name LIMIT ?`, []any{int64(opts.PageSize) + 1}
	if len(token.Last) > 0 {
		query, args = `SELECT name, proto FROM atespaces WHERE name > ? ORDER BY name LIMIT ?`, []any{token.Last[0], int64(opts.PageSize) + 1}
	}
	items, next, err := listPage(ctx, p.db, storesql.KindAtespace, "", opts.PageSize, func(b []byte, keys []string) (*ateapipb.Atespace, error) {
		a := &ateapipb.Atespace{}
		return a, storesql.UnmarshalRow(b, a, "atespace", keys...)
	}, query, args...)
	if err != nil {
		return store.ListResponse[*ateapipb.Atespace]{}, err
	}
	return store.ListResponse[*ateapipb.Atespace]{Items: items, NextPageToken: next}, nil
}

// DeleteAtespace refuses an atespace that still holds actors, actor templates
// or tags, and removes its access policy and authorization tuples with it.
func (p *Persistence) DeleteAtespace(ctx context.Context, name string, precondition store.DeletePreconditions) (*ateapipb.Atespace, error) {
	var protoBytes []byte
	err := inTx(ctx, p.db, func(tx *sql.Tx) error {
		var err error
		_, _, protoBytes, err = deleteLockedRow(ctx, tx, "atespaces", "name = ?", precondition, name)
		if err != nil {
			return err
		}
		// The exclusive lock deleteLockedRow took waits out every child insert
		// that locked this atespace, so these reads see all of its children.
		var nonEmpty bool
		if err := tx.QueryRowContext(ctx, `
			SELECT EXISTS (SELECT 1 FROM actors WHERE atespace = ?)
			    OR EXISTS (SELECT 1 FROM actor_templates WHERE atespace = ?)
			    OR EXISTS (SELECT 1 FROM tags WHERE atespace = ?)`, name, name, name).Scan(&nonEmpty); err != nil {
			return fmt.Errorf("checking atespace %q for children: %w", name, err)
		}
		if nonEmpty {
			return store.ErrFailedPrecondition
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM atespace_access_policies WHERE atespace_name = ?`, name); err != nil {
			return fmt.Errorf("deleting access policy of atespace %q: %w", name, err)
		}
		if err := p.policyManager.DeleteAtespacePolicies(ctx, authz.SQLTx(tx), name); err != nil {
			return fmt.Errorf("deleting authorization tuples for atespace %q: %w", name, err)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	out := &ateapipb.Atespace{}
	if err := storesql.UnmarshalStored(protoBytes, out); err != nil {
		return nil, fmt.Errorf("unmarshaling deleted atespace: %w", err)
	}
	return out, nil
}

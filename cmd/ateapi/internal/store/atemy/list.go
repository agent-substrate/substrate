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
	"fmt"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store/storesql"
)

// listPage runs a keyset page query whose rows hold len(keys) ordering
// columns followed by the proto, decodes each row, and trims the extra row
// the query fetched to learn whether another page follows. The query must
// select pageSize+1 rows.
func listPage[T any](ctx context.Context, q querier, kind storesql.Kind, scope string, pageSize int32, decode func(b []byte, keys []string) (T, error), query string, args ...any) ([]T, string, error) {
	rows, err := q.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, "", fmt.Errorf("listing %s rows: %w", kind, err)
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return nil, "", fmt.Errorf("listing %s rows: %w", kind, err)
	}
	var items []T
	var lastKeys [][]string
	for rows.Next() {
		keys := make([]string, len(cols)-1)
		var protoBytes []byte
		dest := make([]any, 0, len(cols))
		for i := range keys {
			dest = append(dest, &keys[i])
		}
		dest = append(dest, &protoBytes)
		if err := rows.Scan(dest...); err != nil {
			return nil, "", fmt.Errorf("scanning %s row: %w", kind, err)
		}
		item, err := decode(protoBytes, keys)
		if err != nil {
			return nil, "", err
		}
		items = append(items, item)
		lastKeys = append(lastKeys, keys)
	}
	if err := rows.Err(); err != nil {
		return nil, "", fmt.Errorf("listing %s rows: %w", kind, err)
	}
	if len(items) <= int(pageSize) {
		return items, "", nil
	}
	return items[:pageSize], storesql.EncodePageToken(kind, scope, lastKeys[pageSize-1]), nil
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

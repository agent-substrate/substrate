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

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store/storesql"
)

// listPage runs a keyset page query whose rows hold len(keys) ordering
// columns followed by the proto, decodes each row, and trims the extra row
// the query fetched to learn whether another page follows. The query must
// select pageSize+1 rows.
func listPage[T any](ctx context.Context, q querier, kind storesql.Kind, scope string, pageSize int32, decode func(b []byte, keys []string) (T, error), query string, args ...any) ([]T, string, error) {
	rows, err := q.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, "", err
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return nil, "", err
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
		return nil, "", err
	}
	if len(items) <= int(pageSize) {
		return items, "", nil
	}
	return items[:pageSize], storesql.EncodePageToken(kind, scope, lastKeys[pageSize-1]), nil
}

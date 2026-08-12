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
	"encoding/base64"
	"encoding/json"
	"fmt"
)

// pageTokenVersion guards against decoding a token produced by an incompatible
// future token format.
const pageTokenVersion = 1

// resourceKind identifies which List method a page token belongs to, so a
// token can't be replayed against the wrong method.
type resourceKind string

const (
	kindAtespace             resourceKind = "atespace"
	kindActor                resourceKind = "actor"
	kindActorTemplate        resourceKind = "actor-template"
	kindActorTemplateVersion resourceKind = "actor-template-version"
	kindSnapshot             resourceKind = "snapshot"
	kindWorker               resourceKind = "worker"
)

// pageToken is PostgreSQL's opaque keyset page token. Unlike ateredis's
// shard/cursor token, it carries no database topology: just enough to resume
// an ORDER BY ... WHERE (cols) > (last) scan.
type pageToken struct {
	Version int          `json:"v"`
	Kind    resourceKind `json:"kind"`
	Scope   string       `json:"scope"` // atespace name for scoped actor listing; empty otherwise.
	Last    []string     `json:"last"`  // last row's ordering-column values, in order.
}

func encodePageToken(kind resourceKind, scope string, last []string) string {
	b, _ := json.Marshal(pageToken{Version: pageTokenVersion, Kind: kind, Scope: scope, Last: last})
	return base64.StdEncoding.EncodeToString(b)
}

// decodePageToken decodes tokenStr and validates it was issued for the same
// method/scope it's now being presented to. An empty tokenStr decodes to a
// zero-value (start of list) token.
func decodePageToken(tokenStr string, wantKind resourceKind, wantScope string, wantKeyParts int) (pageToken, error) {
	if tokenStr == "" {
		return pageToken{Version: pageTokenVersion, Kind: wantKind, Scope: wantScope}, nil
	}
	b, err := base64.StdEncoding.DecodeString(tokenStr)
	if err != nil {
		return pageToken{}, fmt.Errorf("invalid page token: %w", err)
	}
	var token pageToken
	if err := json.Unmarshal(b, &token); err != nil {
		return pageToken{}, fmt.Errorf("invalid page token: %w", err)
	}
	if token.Version != pageTokenVersion {
		return pageToken{}, fmt.Errorf("invalid page token: unsupported version %d", token.Version)
	}
	if token.Kind != wantKind {
		return pageToken{}, fmt.Errorf("invalid page token: for %q, used with %q", token.Kind, wantKind)
	}
	if token.Scope != wantScope {
		return pageToken{}, fmt.Errorf("invalid page token: for scope %q, used with scope %q", token.Scope, wantScope)
	}
	if len(token.Last) != wantKeyParts {
		return pageToken{}, fmt.Errorf("invalid page token: got %d key parts, want %d", len(token.Last), wantKeyParts)
	}
	return token, nil
}

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

package main

import (
	"testing"

	"github.com/agent-substrate/substrate/internal/proto/ateletpb"
)

// TestStoreForURI verifies the per-URI switch: a restore that reads several
// snapshots resolves each object source independently, so the actor's snapshot
// can use a signed store while an unsigned golden falls back to the node's
// built-in client.
func TestStoreForURI(t *testing.T) {
	s := &AteomHerder{gcsClient: fakeObjectStorage{}}
	signedAccess := map[string]*ateletpb.SignedObjectAccess{
		"s3://bucket/actor": {PrefixUrl: "https://account/container", ReadToken: "tok"},
		"s3://bucket/empty": {PrefixUrl: ""}, // no prefix -> not a usable capability
	}

	isSigned := func(uri string, saMap map[string]*ateletpb.SignedObjectAccess) bool {
		_, ok := s.storeForURI(saMap, uri).(*signedObjectStore)
		return ok
	}

	if !isSigned("s3://bucket/actor", signedAccess) {
		t.Errorf("URI with a capability: want signed store")
	}
	if isSigned("s3://bucket/other", signedAccess) {
		t.Errorf("URI absent from the map: want built-in fallback")
	}
	if isSigned("s3://bucket/empty", signedAccess) {
		t.Errorf("empty-prefix capability: want built-in fallback")
	}
	if isSigned("s3://bucket/actor", nil) {
		t.Errorf("nil capability map: want built-in fallback")
	}
}

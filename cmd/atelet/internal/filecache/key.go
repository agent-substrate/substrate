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

package filecache

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
)

// Key identifies a cache entry. It is an identity, not an address: it says
// nothing about where the bytes come from (the fetcher does), only what they
// are, so two sources serving the same identity share one entry.
//
// Keys are built via constructors only. The zero Key is invalid.
type Key struct {
	// canonical is the unambiguous string form, recorded in the entry's
	// meta.json for debugging. A constructor-specific prefix keeps the key
	// spaces disjoint (a sha256 digest can never equal a URI key).
	canonical string
	// dir is hex(sha256(canonical)): the entry's directory name under
	// entries/. Hashing (rather than escaping the canonical form) yields a
	// fixed-length, filesystem-safe name for arbitrarily long keys, and lets
	// a GC root set be matched against entry dirs by hashing its keys.
	dir string
}

// keyPartSeparator joins URIKey parts unambiguously: NUL cannot appear in a
// URI or file name, so ("a/b","c") and ("a","b/c") canonicalize differently.
const keyPartSeparator = "\x00"

// SHA256Key returns the key for a content-addressed artifact, identified by
// the lowercase hex sha256 of its bytes. Uppercase digits are normalized so
// equal digests always yield equal keys.
func SHA256Key(hexDigest string) (Key, error) {
	d := strings.ToLower(hexDigest)
	if len(d) != sha256.Size*2 {
		return Key{}, fmt.Errorf("sha256 key: digest %q has length %d, want %d", hexDigest, len(d), sha256.Size*2)
	}
	if _, err := hex.DecodeString(d); err != nil {
		return Key{}, fmt.Errorf("sha256 key: digest %q is not hex: %w", hexDigest, err)
	}
	return newKey("sha256:" + d), nil
}

// URIKey returns the key for an artifact identified by an immutable source,
// e.g. URIKey(goldenSnapshotURI, fileName). The parts must identify content
// that never changes underneath them; the cache has no invalidation, so a
// republished URI would serve stale bytes forever. Callers pass at least one
// non-empty part.
func URIKey(parts ...string) Key {
	return newKey("uri:" + strings.Join(parts, keyPartSeparator))
}

func newKey(canonical string) Key {
	sum := sha256.Sum256([]byte(canonical))
	return Key{canonical: canonical, dir: hex.EncodeToString(sum[:])}
}

// String returns the canonical form, for logs and meta.json.
func (k Key) String() string { return k.canonical }

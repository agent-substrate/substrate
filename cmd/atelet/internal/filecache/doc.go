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

// Package filecache is a node-local disk cache of downloaded artifacts,
// keyed by opaque identities (see Key). An artifact wanted by N concurrent
// callers is fetched once, publication into the cache is atomic and
// crash-safe, and entries are evicted under byte-budget pressure without
// breaking consumers.
//
// On-disk layout, under a store's root (which the store owns exclusively):
//
//	entries/<sha256(key)>/
//	  data       # the cached file (or directory tree)
//	  meta.json  # canonical key + creation time; debugging only
//	tmp/         # in-flight fetches; same filesystem as entries/, so
//	             # publication is one atomic rename
//	.rm-*        # retired entries awaiting removal
//
// An entry directory's mtime is its last-use clock. Nothing on a correctness
// path reads meta.json: entries are matched to keys by hashing the keys.
// Everything under entries/ is a complete, published entry; SweepDebris
// clears crash leftovers from the other two locations at startup.
//
// # Contracts
//
// Eviction never invalidates what a caller already received:
//
//   - GetFileTo serves a hit as a hard link, created in the same locked
//     step that resolves the hit. Evicting the entry later removes only the
//     cache's own link; the consumer's file stays valid.
//   - GetFileCopyTo serves a private copy. The consumer owns the inode and
//     may mutate it, and a copy in progress reads a held-open handle, so a
//     concurrent eviction cannot corrupt it.
//   - The store's min age (WithMinAge) vetoes eviction of entries younger
//     than the window between publication and a consumer's use becoming
//     visible. Size it for the slowest consumer.
//
// Entries are published read-only (0444): a consumer writing through its
// hard link fails with EACCES instead of corrupting the shared bytes.
// Consumers that must mutate a file use GetFileCopyTo.
//
// Keys are identities, not addresses. The store never invalidates: a key
// must name content that is immutable at its source (see URIKey); changed
// content must arrive under a new key.
package filecache

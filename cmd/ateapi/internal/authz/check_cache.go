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

package authz

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/storage/cache/keys"
)

// checkCache backs OpenFGA's tuple iterator cache and coordinates local
// generation-based invalidation with Authorizer's scope-level Check cache.
// Invalidate bumps generation atomically so both OpenFGA tuple iterators and
// scope-level positive Check entries written before a local policy mutation
// read as absent immediately, while peer replicas age entries out within ttl.
type checkCache struct {
	lru        *storage.InMemoryLRUCache[any]
	ttl        time.Duration
	generation atomic.Uint64
	stopOnce   sync.Once
	onStop     func()
}

type cachedEntry struct {
	generation uint64
	value      any
}

func newCheckCache(limit int64, ttl time.Duration) (*checkCache, error) {
	lru, err := storage.NewInMemoryLRUCache(storage.WithMaxCacheSize[any](limit))
	if err != nil {
		return nil, err
	}
	return &checkCache{lru: lru, ttl: ttl}, nil
}

func (c *checkCache) Get(key keys.Key) any {
	entry, ok := c.lru.Get(key).(cachedEntry)
	if !ok {
		return nil
	}
	if entry.generation != c.generation.Load() {
		c.lru.Delete(key)
		return nil
	}
	return entry.value
}

func (c *checkCache) Set(key keys.Key, value any, ttl time.Duration) {
	c.lru.Set(key, cachedEntry{generation: c.generation.Load(), value: value}, ttl)
}

func (c *checkCache) Delete(key keys.Key) { c.lru.Delete(key) }

func (c *checkCache) Stop() {
	c.stopOnce.Do(func() {
		c.lru.Stop()
		if c.onStop != nil {
			c.onStop()
		}
	})
}

func (c *checkCache) Invalidate() {
	if c != nil {
		c.generation.Add(1)
	}
}

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

package ingress

import (
	"container/list"
	"sync"
	"time"

	"go.ate.dev/substrate/api/ateapi/v1"
)

const defaultRouteCacheCapacity = 65536

type cachedActorRoute struct {
	workerIP         string
	workerSPIFFEID   string
	actorUID         string
	templateAtespace string
	templateName     string
	expiresAt        time.Time
}

func (c cachedActorRoute) toActor() *ateapi.Actor {
	return &ateapi.Actor{
		Metadata: &ateapi.ResourceMetadata{
			Uid: c.actorUID,
		},
		ActorTemplate: &ateapi.ObjectRef{
			Atespace: c.templateAtespace,
			Name:     c.templateName,
		},
		Status: &ateapi.ActorStatus{
			State: ateapi.ActorState_ACTOR_STATE_RUNNING,
			WorkerAssignment: &ateapi.WorkerAssignment{
				WorkerPodIps:   []string{c.workerIP},
				WorkerSpiffeId: c.workerSPIFFEID,
			},
		},
	}
}

func actorUIDOf(actor *ateapi.Actor) string {
	if actor == nil {
		return ""
	}
	if meta := actor.GetMetadata(); meta != nil && meta.GetUid() != "" {
		return meta.GetUid()
	}
	return ""
}

type routeCacheEntry struct {
	key   string
	route cachedActorRoute
	elem  *list.Element
}

// boundedRouteCache maintains a thread-safe, bounded LRU cache of warm actor routes.
type boundedRouteCache struct {
	mu       sync.Mutex
	capacity int
	entries  map[string]*routeCacheEntry
	order    *list.List
}

func newBoundedRouteCache(capacity int) *boundedRouteCache {
	if capacity <= 0 {
		capacity = defaultRouteCacheCapacity
	}
	return &boundedRouteCache{
		capacity: capacity,
		entries:  make(map[string]*routeCacheEntry),
		order:    list.New(),
	}
}

func (c *boundedRouteCache) Get(key string) (cachedActorRoute, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	entry, ok := c.entries[key]
	if !ok {
		return cachedActorRoute{}, false
	}
	if time.Now().After(entry.route.expiresAt) {
		c.removeEntryLocked(entry)
		return cachedActorRoute{}, false
	}
	c.order.MoveToFront(entry.elem)
	return entry.route, true
}

func (c *boundedRouteCache) Set(key string, route cachedActorRoute) {
	c.mu.Lock()
	defer c.mu.Unlock()

	now := time.Now()
	if entry, ok := c.entries[key]; ok {
		entry.route = route
		c.order.MoveToFront(entry.elem)
		return
	}

	c.pruneLocked(now)

	entry := &routeCacheEntry{
		key:   key,
		route: route,
	}
	entry.elem = c.order.PushFront(entry)
	c.entries[key] = entry
}

func (c *boundedRouteCache) Delete(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if entry, ok := c.entries[key]; ok {
		c.removeEntryLocked(entry)
	}
}

func (c *boundedRouteCache) pruneLocked(now time.Time) {
	for elem := c.order.Back(); elem != nil; {
		prev := elem.Prev()
		entry := elem.Value.(*routeCacheEntry)
		if now.After(entry.route.expiresAt) {
			c.removeEntryLocked(entry)
		} else {
			break
		}
		elem = prev
	}

	for len(c.entries) >= c.capacity {
		back := c.order.Back()
		if back == nil {
			break
		}
		c.removeEntryLocked(back.Value.(*routeCacheEntry))
	}
}

func (c *boundedRouteCache) removeEntryLocked(entry *routeCacheEntry) {
	if entry.elem != nil {
		c.order.Remove(entry.elem)
		entry.elem = nil
	}
	delete(c.entries, entry.key)
}

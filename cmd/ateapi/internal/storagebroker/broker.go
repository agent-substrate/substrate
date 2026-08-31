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

// Package storagebroker is the vendor-neutral seam by which ate-api-server
// mints short-lived, snapshot-scoped capabilities (signed URLs) for atelet to
// read and write snapshot objects over plain HTTP. Core imports only this
// interface and a registry; a cloud-specific implementation registers itself
// from an out-of-tree build (see the azure sub-implementation, which links the
// Azure SDK). This keeps the control-plane binary free of any cloud SDK unless
// a provider is compiled in, and keeps the node (atelet) free of one entirely.
package storagebroker

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"
)

// Capability is a control-plane-minted, snapshot-scoped grant. atelet forms a
// per-object request from these mechanics; it needs no cloud credential of its
// own. A read capability (from MintRead) populates the read fields; a write
// capability (from MintWrite) populates the write fields.
type Capability struct {
	// PrefixURL is the account/bucket base URL, e.g.
	// "https://acct.blob.core.windows.net/snapshots" or "http://s3-host/bucket".
	PrefixURL string

	// Read mechanics. Either a token appended to <PrefixURL>/<key> (Azure), or a
	// per-object presigned URL map (S3).
	ReadToken      string
	ReadObjectURLs map[string]string

	// Write mechanics. WriteMethod is "PUT" (Azure: append WriteToken, send
	// WriteHeaders) or "POST" (S3: multipart form to PostURL with PostFields).
	WriteMethod  string
	WriteToken   string
	WriteHeaders map[string]string
	PostURL      string
	PostFields   map[string]string
}

// Write methods a Capability may carry.
const (
	WriteMethodPUT  = "PUT"
	WriteMethodPOST = "POST"
)

// Broker mints read/write capabilities for one snapshot, identified by its URI
// prefix. Implementations authorize and sign; they never touch snapshot bytes.
type Broker interface {
	MintRead(ctx context.Context, snapshotURI string, ttl time.Duration) (Capability, error)
	MintWrite(ctx context.Context, snapshotURI string, ttl time.Duration) (Capability, error)
}

// Factory builds a Broker. Cloud implementations register one under a backend
// name; New selects it. Registration keeps core free of cloud SDK imports.
type Factory func(ctx context.Context) (Broker, error)

var (
	mu        sync.RWMutex
	factories = map[string]Factory{}
)

// Register adds a broker backend. Implementations call this from an init in
// their own (out-of-tree) package.
func Register(name string, f Factory) {
	mu.Lock()
	defer mu.Unlock()
	factories[name] = f
}

// New constructs the broker registered under backend, or an error naming the
// backends that are compiled in. An empty backend disables signing (nil, nil),
// so callers fall back to the legacy in-atelet storage client.
func New(ctx context.Context, backend string) (Broker, error) {
	if backend == "" {
		return nil, nil
	}
	mu.RLock()
	f, ok := factories[backend]
	mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("storage broker backend %q not registered (have: %v)", backend, registered())
	}
	return f(ctx)
}

func registered() []string {
	mu.RLock()
	defer mu.RUnlock()
	names := make([]string, 0, len(factories))
	for n := range factories {
		names = append(names, n)
	}
	sort.Strings(names)
	return names
}

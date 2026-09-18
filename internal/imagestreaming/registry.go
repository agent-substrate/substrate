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

package imagestreaming

import (
	"context"
	"fmt"
	"slices"
	"sync"
)

// SocketPathKey is the standard configuration key for passing a daemon socket path.
const SocketPathKey = "socket"

// Config carries provider-specific options such as socket paths or timeouts.
type Config map[string]string

// Factory creates an instance of an ImageStreamer given a configuration.
type Factory func(ctx context.Context, cfg Config) (ImageStreamer, error)

var (
	mu        sync.RWMutex
	providers = make(map[string]Factory)
)

// Register registers an ImageStreamer factory under the given provider name.
// Panics if the factory is nil or if a provider with the same name is already registered.
func Register(name string, f Factory) {
	mu.Lock()
	defer mu.Unlock()

	if f == nil {
		panic(fmt.Sprintf("imagestreaming: Register factory for %q is nil", name))
	}
	if _, dup := providers[name]; dup {
		panic(fmt.Sprintf("imagestreaming: Register called twice for provider %q", name))
	}
	providers[name] = f
}

// Get instantiates the named ImageStreamer provider with the supplied configuration.
// Returns an error if no provider by that name is registered.
func Get(ctx context.Context, name string, cfg Config) (ImageStreamer, error) {
	mu.RLock()
	factory, ok := providers[name]
	mu.RUnlock()

	if !ok {
		return nil, fmt.Errorf("imagestreaming: unknown provider %q (registered: %v)", name, Providers())
	}
	return factory(ctx, cfg)
}

// Providers returns a sorted list of all currently registered provider names.
func Providers() []string {
	mu.RLock()
	defer mu.RUnlock()

	names := make([]string, 0, len(providers))
	for name := range providers {
		names = append(names, name)
	}
	slices.Sort(names)
	return names
}

// unregisterAll is a helper for unit testing to reset the registry between tests.
func unregisterAll() {
	mu.Lock()
	defer mu.Unlock()
	providers = make(map[string]Factory)
}

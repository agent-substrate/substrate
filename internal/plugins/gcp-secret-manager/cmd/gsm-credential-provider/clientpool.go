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
	"errors"
	"sync"

	"go.ate.dev/substrate/internal/plugins/gcp-secret-manager/internal/provider"
)

// secretManagerClient is a closable SecretAccessor.
type secretManagerClient interface {
	provider.SecretAccessor
	Close() error
}

// clientPool holds the global Secret Manager client plus one per region,
// created on first use: a regional secret is served only by its region's
// endpoint.
type clientPool struct {
	global secretManagerClient
	// newRegional must not use a request's context: a client keeps the context
	// it was created with, e.g. to refresh credentials.
	newRegional func(location string) (secretManagerClient, error)

	mu       sync.Mutex
	regional map[string]secretManagerClient
}

// For returns the client for location ("" for global). A failed creation is
// not cached, so the next call retries.
func (p *clientPool) For(location string) (provider.SecretAccessor, error) {
	if location == "" {
		return p.global, nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if c, ok := p.regional[location]; ok {
		return c, nil
	}
	c, err := p.newRegional(location)
	if err != nil {
		return nil, err
	}
	if p.regional == nil {
		p.regional = make(map[string]secretManagerClient)
	}
	p.regional[location] = c
	return c, nil
}

// Close closes every client the pool holds.
func (p *clientPool) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	errs := []error{p.global.Close()}
	for _, c := range p.regional {
		errs = append(errs, c.Close())
	}
	return errors.Join(errs...)
}

// regionalEndpoint returns the endpoint serving a region's secrets.
func regionalEndpoint(location string) string {
	return "secretmanager." + location + ".rep.googleapis.com:443"
}

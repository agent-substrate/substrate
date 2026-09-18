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

// Package mock provides a mock implementation of imagestreaming.ImageStreamer for unit testing.
package mock

import (
	"context"
	"sync"

	"github.com/agent-substrate/substrate/internal/imagestreaming"
)

// Streamer is a thread-safe mock implementation of imagestreaming.ImageStreamer.
type Streamer struct {
	mu sync.Mutex

	NameVal string

	CanStreamFunc       func(ctx context.Context, req *imagestreaming.StreamRequest) (bool, error)
	PrepareLayersFunc   func(ctx context.Context, req *imagestreaming.StreamRequest) (*imagestreaming.StreamResult, error)
	ReleaseLayersFunc   func(ctx context.Context, req *imagestreaming.StreamRequest) error
	ReconcileLeasesFunc func(ctx context.Context, active []*imagestreaming.ActiveLease) error

	CanStreamCalls       []*imagestreaming.StreamRequest
	PrepareLayersCalls   []*imagestreaming.StreamRequest
	ReleaseLayersCalls   []*imagestreaming.StreamRequest
	ReconcileLeasesCalls [][]*imagestreaming.ActiveLease
}

var _ imagestreaming.ImageStreamer = (*Streamer)(nil)

// New creates a new MockStreamer with default name "mock".
func New() *Streamer {
	return &Streamer{NameVal: "mock"}
}

func (m *Streamer) Name() string {
	if m.NameVal == "" {
		return "mock"
	}
	return m.NameVal
}

func (m *Streamer) CanStream(ctx context.Context, req *imagestreaming.StreamRequest) (bool, error) {
	m.mu.Lock()
	m.CanStreamCalls = append(m.CanStreamCalls, req)
	fn := m.CanStreamFunc
	m.mu.Unlock()

	if fn != nil {
		return fn(ctx, req)
	}
	return true, nil
}

func (m *Streamer) PrepareLayers(ctx context.Context, req *imagestreaming.StreamRequest) (*imagestreaming.StreamResult, error) {
	m.mu.Lock()
	m.PrepareLayersCalls = append(m.PrepareLayersCalls, req)
	fn := m.PrepareLayersFunc
	m.mu.Unlock()

	if fn != nil {
		return fn(ctx, req)
	}
	return &imagestreaming.StreamResult{
		LayerDirs: []string{"/mock/layer/fs"},
	}, nil
}

func (m *Streamer) ReleaseLayers(ctx context.Context, req *imagestreaming.StreamRequest) error {
	m.mu.Lock()
	m.ReleaseLayersCalls = append(m.ReleaseLayersCalls, req)
	fn := m.ReleaseLayersFunc
	m.mu.Unlock()

	if fn != nil {
		return fn(ctx, req)
	}
	return nil
}

func (m *Streamer) ReconcileLeases(ctx context.Context, active []*imagestreaming.ActiveLease) error {
	m.mu.Lock()
	m.ReconcileLeasesCalls = append(m.ReconcileLeasesCalls, active)
	fn := m.ReconcileLeasesFunc
	m.mu.Unlock()

	if fn != nil {
		return fn(ctx, active)
	}
	return nil
}

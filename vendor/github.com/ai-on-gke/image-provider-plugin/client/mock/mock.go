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

// Package mock provides an in-memory client.Client for tests that need no
// live image provider plugin.
package mock

import (
	"context"
	"sync"

	imageprovider "github.com/ai-on-gke/image-provider-plugin/api/imageprovider/v1alpha1"
	"github.com/ai-on-gke/image-provider-plugin/client"
)

// Client is a configurable client.Client. Set the Func fields before use.
type Client struct {
	mu sync.Mutex

	ProviderName string
	// PluginState is returned by State. The zero value means SERVING.
	PluginState      imageprovider.PluginState
	CanProvideFunc   func(ctx context.Context, req *client.PrepareRequest) (bool, error)
	StateFunc        func(ctx context.Context) (imageprovider.PluginState, error)
	PrepareImageFunc func(ctx context.Context, req *client.PrepareRequest) (*client.PrepareResult, error)
	ReleaseImageFunc func(ctx context.Context, req *client.ReleaseRequest) error
	ReconcileFunc    func(ctx context.Context, leaseIDs []string) (*client.ReconcileResult, error)
	RefreshAuthFunc  func(ctx context.Context, req *client.RefreshRequest) (imageprovider.AuthRefresh, error)

	PrepareCalls   []*client.PrepareRequest
	ReleaseCalls   []*client.ReleaseRequest
	ReconcileCalls [][]string
	RefreshCalls   []*client.RefreshRequest
}

var _ client.Client = (*Client)(nil)

// New returns a Client named providerName, or "mock" if it is empty.
func New(providerName string) *Client {
	if providerName == "" {
		providerName = "mock"
	}
	return &Client{ProviderName: providerName}
}

// Name returns ProviderName.
func (m *Client) Name() string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.ProviderName
}

// CanProvide calls CanProvideFunc if set, and returns true otherwise.
func (m *Client) CanProvide(ctx context.Context, req *client.PrepareRequest) (bool, error) {
	m.mu.Lock()
	fn := m.CanProvideFunc
	m.mu.Unlock()
	if fn != nil {
		return fn(ctx, req)
	}
	return true, nil
}

// State calls StateFunc if set, and returns PluginState otherwise, with
// PLUGIN_STATE_UNSPECIFIED reported as PLUGIN_STATE_SERVING.
func (m *Client) State(ctx context.Context) (imageprovider.PluginState, error) {
	m.mu.Lock()
	fn := m.StateFunc
	state := m.PluginState
	m.mu.Unlock()
	if fn != nil {
		return fn(ctx)
	}
	if state == imageprovider.PluginState_PLUGIN_STATE_UNSPECIFIED {
		return imageprovider.PluginState_PLUGIN_STATE_SERVING, nil
	}
	return state, nil
}

// PrepareImage records the call and calls PrepareImageFunc if set. Otherwise
// it returns client.ErrNotSupported.
func (m *Client) PrepareImage(ctx context.Context, req *client.PrepareRequest) (*client.PrepareResult, error) {
	m.mu.Lock()
	m.PrepareCalls = append(m.PrepareCalls, req)
	fn := m.PrepareImageFunc
	m.mu.Unlock()
	if fn != nil {
		return fn(ctx, req)
	}
	return nil, client.ErrNotSupported
}

// ReleaseImage records the call and calls ReleaseImageFunc if set.
func (m *Client) ReleaseImage(ctx context.Context, req *client.ReleaseRequest) error {
	m.mu.Lock()
	m.ReleaseCalls = append(m.ReleaseCalls, req)
	fn := m.ReleaseImageFunc
	m.mu.Unlock()
	if fn != nil {
		return fn(ctx, req)
	}
	return nil
}

// ReconcileImages records the call and calls ReconcileFunc if set.
// Otherwise it restores every lease.
func (m *Client) ReconcileImages(ctx context.Context, leaseIDs []string) (*client.ReconcileResult, error) {
	m.mu.Lock()
	m.ReconcileCalls = append(m.ReconcileCalls, leaseIDs)
	fn := m.ReconcileFunc
	m.mu.Unlock()
	if fn != nil {
		return fn(ctx, leaseIDs)
	}
	return &client.ReconcileResult{}, nil
}

// RefreshAuth records the call and calls RefreshAuthFunc if set. Otherwise
// it returns client.ErrNotSupported.
func (m *Client) RefreshAuth(ctx context.Context, req *client.RefreshRequest) (imageprovider.AuthRefresh, error) {
	m.mu.Lock()
	m.RefreshCalls = append(m.RefreshCalls, req)
	fn := m.RefreshAuthFunc
	m.mu.Unlock()
	if fn != nil {
		return fn(ctx, req)
	}
	return imageprovider.AuthRefresh_AUTH_REFRESH_UNSPECIFIED, client.ErrNotSupported
}

// Close does nothing.
func (m *Client) Close() error {
	return nil
}

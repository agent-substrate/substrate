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
	"slices"
	"testing"
)

type dummyStreamer struct {
	name string
}

func (d *dummyStreamer) Name() string { return d.name }
func (d *dummyStreamer) CanStream(_ context.Context, _ *StreamRequest) (bool, error) {
	return true, nil
}
func (d *dummyStreamer) PrepareLayers(_ context.Context, _ *StreamRequest) (*StreamResult, error) {
	return &StreamResult{LayerDirs: []string{"/test/layer"}}, nil
}
func (d *dummyStreamer) ReleaseLayers(_ context.Context, _ *StreamRequest) error {
	return nil
}
func (d *dummyStreamer) ReconcileLeases(_ context.Context, _ []*ActiveLease) error {
	return nil
}

func TestRegistry_RegisterAndGet(t *testing.T) {
	defer unregisterAll()

	Register("dummy", func(_ context.Context, _ Config) (ImageStreamer, error) {
		return &dummyStreamer{name: "dummy"}, nil
	})

	s, err := Get(context.Background(), "dummy", nil)
	if err != nil {
		t.Fatalf("Get('dummy') returned unexpected error: %v", err)
	}
	if s.Name() != "dummy" {
		t.Errorf("s.Name() = %q, want 'dummy'", s.Name())
	}

	can, err := s.CanStream(context.Background(), &StreamRequest{ImageRef: "example.com/img@sha256:123"})
	if err != nil || !can {
		t.Errorf("CanStream() = (%v, %v), want (true, nil)", can, err)
	}

	res, err := s.PrepareLayers(context.Background(), &StreamRequest{ImageRef: "example.com/img@sha256:123"})
	if err != nil || len(res.LayerDirs) != 1 || res.LayerDirs[0] != "/test/layer" {
		t.Errorf("PrepareLayers() = (%v, %v), want (['/test/layer'], nil)", res, err)
	}
}

func TestRegistry_UnknownProvider(t *testing.T) {
	defer unregisterAll()

	_, err := Get(context.Background(), "nonexistent", nil)
	if err == nil {
		t.Fatalf("Get('nonexistent') expected error, got nil")
	}
}

func TestRegistry_ProvidersList(t *testing.T) {
	defer unregisterAll()

	Register("beta", func(_ context.Context, _ Config) (ImageStreamer, error) {
		return &dummyStreamer{name: "beta"}, nil
	})
	Register("alpha", func(_ context.Context, _ Config) (ImageStreamer, error) {
		return &dummyStreamer{name: "alpha"}, nil
	})

	list := Providers()
	want := []string{"alpha", "beta"}
	if !slices.Equal(list, want) {
		t.Errorf("Providers() = %v, want %v", list, want)
	}
}

func TestRegistry_DuplicatePanic(t *testing.T) {
	defer unregisterAll()

	Register("dup", func(_ context.Context, _ Config) (ImageStreamer, error) {
		return &dummyStreamer{name: "dup"}, nil
	})

	defer func() {
		if r := recover(); r == nil {
			t.Errorf("Registering duplicate provider did not panic")
		}
	}()

	Register("dup", func(_ context.Context, _ Config) (ImageStreamer, error) {
		return &dummyStreamer{name: "dup"}, nil
	})
}

func TestRegistry_NilFactoryPanic(t *testing.T) {
	defer unregisterAll()

	defer func() {
		if r := recover(); r == nil {
			t.Errorf("Registering nil factory did not panic")
		}
	}()

	Register("nil-factory", nil)
}

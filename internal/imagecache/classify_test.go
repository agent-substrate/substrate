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

package imagecache

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net"
	"net/url"
	"syscall"
	"testing"

	"github.com/google/go-containerregistry/pkg/v1/remote/transport"

	"github.com/agent-substrate/substrate/internal/ateerrors"
)

func TestClassifyRegistryErr(t *testing.T) {
	transient := []struct {
		name string
		err  error
	}{
		{"registry 503", &transport.Error{StatusCode: 503}},
		{"wrapped registry 502", fmt.Errorf("while resolving tag: %w", &transport.Error{StatusCode: 502})},
		{"net error", &net.OpError{Op: "read", Net: "tcp", Err: syscall.ECONNRESET}},
		{"dial failure via http client", &url.Error{Op: "Get", URL: "https://us-docker.pkg.dev/v2/ate-demo/counter/manifests/latest", Err: &net.OpError{Op: "dial", Net: "tcp", Err: syscall.ECONNREFUSED}}},
		{"layer stream cut mid-pull", fmt.Errorf("while reading layer diffID: %w", io.ErrUnexpectedEOF)},
	}
	for _, tt := range transient {
		t.Run(tt.name+" is tagged transient", func(t *testing.T) {
			got := classifyRegistryErr(tt.err)
			if !errors.Is(got, ateerrors.ReasonTransientImageRegistry) {
				t.Errorf("classifyRegistryErr(%v) = %v, want tagged %v", tt.err, got, ateerrors.ReasonTransientImageRegistry)
			}
		})
	}

	// A definitive registry answer: re-pulling the same ref cannot succeed.
	rejected := []struct {
		name string
		err  error
	}{
		{"manifest absent", &transport.Error{StatusCode: 404}},
		{"unauthorized", &transport.Error{StatusCode: 401}},
	}
	for _, tt := range rejected {
		t.Run(tt.name+" is tagged a definitive rejection", func(t *testing.T) {
			got := classifyRegistryErr(tt.err)
			if !errors.Is(got, ateerrors.ReasonFailedGetExternalObject) {
				t.Errorf("classifyRegistryErr(%v) = %v, want tagged %v", tt.err, got, ateerrors.ReasonFailedGetExternalObject)
			}
			if errors.Is(got, ateerrors.ReasonTransientImageRegistry) {
				t.Errorf("classifyRegistryErr(%v) = %v, also tagged transient", tt.err, got)
			}
		})
	}

	passedThrough := []struct {
		name string
		err  error
	}{
		{"nil", nil},
		{"local unpack disk full", &fs.PathError{Op: "write", Path: "/var/lib/ate/image-cache/layers", Err: syscall.ENOSPC}},
		{"context canceled", fmt.Errorf("while pulling: %w", context.Canceled)},
		{"already classified", fmt.Errorf("%w: empty argv", ateerrors.ReasonInvalidContainerConfig)},
		{"url error with non-transient cause", &url.Error{Op: "Get", URL: "https://us-docker.pkg.dev/v2/", Err: errors.New("http: no Host in request URL")}},
		{"unclassified", errors.New("layer 3 diffID mismatch")},
	}
	for _, tt := range passedThrough {
		t.Run(tt.name+" passes through", func(t *testing.T) {
			if got := classifyRegistryErr(tt.err); got != tt.err {
				t.Errorf("classifyRegistryErr(%v) = %v, want the same error back", tt.err, got)
			}
		})
	}
}

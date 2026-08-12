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

package extproc

import (
	"reflect"
	"testing"

	corev3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
)

func TestExtractMetadata(t *testing.T) {
	tests := []struct {
		name        string
		headers     []*corev3.HeaderValue
		wantHeaders map[string]string
		wantPath    string
		wantHost    string
	}{
		{
			name: "basic path and authority",
			headers: []*corev3.HeaderValue{
				{Key: ":path", Value: "/api/v1/test"},
				{Key: ":authority", Value: "example.com"},
				{Key: "X-Request-ID", Value: "req-123"},
			},
			wantHeaders: map[string]string{
				":path":        "/api/v1/test",
				":authority":   "example.com",
				"x-request-id": "req-123",
			},
			wantPath: "/api/v1/test",
			wantHost: "example.com",
		},
		{
			name: "host header overrides empty or authority",
			headers: []*corev3.HeaderValue{
				{Key: ":path", Value: "/api/v1/test"},
				{Key: ":authority", Value: "authority.com"},
				{Key: "Host", Value: "host.com"},
			},
			wantHeaders: map[string]string{
				":path":      "/api/v1/test",
				":authority": "authority.com",
				"host":       "host.com",
			},
			wantPath: "/api/v1/test",
			wantHost: "host.com",
		},
		{
			name: "authority header overrides host when it comes after",
			headers: []*corev3.HeaderValue{
				{Key: ":path", Value: "/api/v1/test"},
				{Key: "Host", Value: "host.com"},
				{Key: ":authority", Value: "authority.com"},
			},
			wantHeaders: map[string]string{
				":path":      "/api/v1/test",
				"host":       "host.com",
				":authority": "authority.com",
			},
			wantPath: "/api/v1/test",
			wantHost: "authority.com",
		},
		{
			name: "no authority or host headers",
			headers: []*corev3.HeaderValue{
				{Key: ":path", Value: "/api/v1/test"},
				{Key: "x-something-else", Value: "custom-value"},
			},
			wantHeaders: map[string]string{
				":path":            "/api/v1/test",
				"x-something-else": "custom-value",
			},
			wantPath: "/api/v1/test",
			wantHost: "",
		},
		{
			name: "headers are lowercased",
			headers: []*corev3.HeaderValue{
				{Key: "UPPER-KEY", Value: "UPPER-VALUE"},
				{Key: "camelCaseKey", Value: "camelValue"},
			},
			wantHeaders: map[string]string{
				"upper-key":    "UPPER-VALUE",
				"camelcasekey": "camelValue",
			},
			wantPath: "",
			wantHost: "",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := NewRequestMetadata(tc.headers)

			if !reflect.DeepEqual(got.Headers, tc.wantHeaders) {
				t.Errorf("NewRequestMetadata() headersMap = %v, want %v", got.Headers, tc.wantHeaders)
			}
			if got.Path != tc.wantPath {
				t.Errorf("NewRequestMetadata() path = %v, want %v", got.Path, tc.wantPath)
			}
			if got.Host != tc.wantHost {
				t.Errorf("NewRequestMetadata() host = %v, want %v", got.Host, tc.wantHost)
			}
		})
	}
}

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

package router

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
			got := newRequestMetadata(tc.headers)

			if !reflect.DeepEqual(got.headers, tc.wantHeaders) {
				t.Errorf("extractMetadata() headersMap = %v, want %v", got.headers, tc.wantHeaders)
			}
			if got.path != tc.wantPath {
				t.Errorf("extractMetadata() path = %v, want %v", got.path, tc.wantPath)
			}
			if got.host != tc.wantHost {
				t.Errorf("extractMetadata() host = %v, want %v", got.host, tc.wantHost)
			}
		})
	}
}

func TestParseActorID(t *testing.T) {
	tests := []struct {
		name    string
		host    string
		wantID  string
		wantErr bool
	}{
		{
			name:    "valid host without port",
			host:    "my-actor.actors.resources.substrate.ate.dev",
			wantID:  "my-actor",
			wantErr: false,
		},
		{
			name:    "valid host with port",
			host:    "my-actor.actors.resources.substrate.ate.dev:8443",
			wantID:  "my-actor",
			wantErr: false,
		},
		{
			name:    "valid host with trailing dot",
			host:    "my-actor.actors.resources.substrate.ate.dev.",
			wantID:  "my-actor",
			wantErr: false,
		},
		{
			name:    "valid host with trailing dot and port",
			host:    "my-actor.actors.resources.substrate.ate.dev.:8080",
			wantID:  "my-actor",
			wantErr: false,
		},
		{
			name:    "invalid suffix",
			host:    "my-actor.example.com",
			wantID:  "",
			wantErr: true,
		},
		{
			name:    "invalid host port format",
			host:    "my-actor.actors.resources.substrate.ate.dev:invalid:port",
			wantID:  "",
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			gotID, err := parseActorID(tc.host)
			if (err != nil) != tc.wantErr {
				t.Errorf("parseActorID(%q) error = %v, wantErr %v", tc.host, err, tc.wantErr)
				return
			}
			if gotID != tc.wantID {
				t.Errorf("parseActorID(%q) gotID = %v, want %v", tc.host, gotID, tc.wantID)
			}
		})
	}
}

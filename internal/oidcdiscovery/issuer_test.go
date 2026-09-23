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

package oidcdiscovery

import "testing"

func TestParseIssuer(t *testing.T) {
	tests := []struct {
		name    string
		raw     string
		want    string
		wantErr bool
	}{
		{name: "in-cluster default", raw: "https://idp.ate-system.svc", want: "https://idp.ate-system.svc"},
		{name: "public host", raw: "https://idp.example.com", want: "https://idp.example.com"},
		{name: "path", raw: "https://idp.example.com/clusters/prod", want: "https://idp.example.com/clusters/prod"},
		{name: "port", raw: "https://idp.example.com:8443", want: "https://idp.example.com:8443"},
		{name: "trailing slash", raw: "https://idp.example.com/", want: "https://idp.example.com"},
		{name: "path with trailing slashes", raw: "https://idp.example.com/prod//", want: "https://idp.example.com/prod"},

		{name: "empty", raw: "", wantErr: true},
		{name: "http", raw: "http://idp.example.com", wantErr: true},
		{name: "no scheme", raw: "idp.example.com", wantErr: true},
		{name: "uppercase scheme", raw: "HTTPS://idp.example.com", wantErr: true},
		{name: "no host", raw: "https:///prod", wantErr: true},
		{name: "port without host", raw: "https://:443", wantErr: true},
		{name: "opaque", raw: "https:idp.example.com", wantErr: true},
		{name: "user info", raw: "https://user:pass@idp.example.com", wantErr: true},
		{name: "query", raw: "https://idp.example.com?cluster=prod", wantErr: true},
		{name: "empty query", raw: "https://idp.example.com?", wantErr: true},
		{name: "fragment", raw: "https://idp.example.com#prod", wantErr: true},
		{name: "empty fragment", raw: "https://idp.example.com#", wantErr: true},
		{name: "unescaped space in path", raw: "https://idp.example.com/my cluster", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseIssuer(tt.raw)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("ParseIssuer(%q) = %q, want error", tt.raw, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("ParseIssuer(%q) returned error: %v", tt.raw, err)
			}
			if got != tt.want {
				t.Errorf("ParseIssuer(%q) = %q, want %q", tt.raw, got, tt.want)
			}
		})
	}
}

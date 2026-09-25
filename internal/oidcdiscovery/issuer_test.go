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

func TestValidateIssuer(t *testing.T) {
	tests := []struct {
		name    string
		issuer  string
		wantErr bool
	}{
		{name: "in-cluster default", issuer: "https://idp.ate-system.svc"},
		{name: "public host", issuer: "https://idp.example.com"},
		{name: "path", issuer: "https://idp.example.com/clusters/prod"},
		{name: "port", issuer: "https://idp.example.com:8443"},
		{name: "trailing slash", issuer: "https://idp.example.com/"},
		{name: "path with trailing slash", issuer: "https://idp.example.com/prod/"},

		{name: "empty", issuer: "", wantErr: true},
		{name: "http", issuer: "http://idp.example.com", wantErr: true},
		{name: "no scheme", issuer: "idp.example.com", wantErr: true},
		{name: "no host", issuer: "https:///prod", wantErr: true},
		{name: "port without host", issuer: "https://:443", wantErr: true},
		{name: "opaque", issuer: "https:idp.example.com", wantErr: true},
		{name: "user info", issuer: "https://user:pass@idp.example.com", wantErr: true},
		{name: "query", issuer: "https://idp.example.com?cluster=prod", wantErr: true},
		{name: "empty query", issuer: "https://idp.example.com?", wantErr: true},
		{name: "fragment", issuer: "https://idp.example.com#prod", wantErr: true},
		{name: "empty fragment", issuer: "https://idp.example.com#", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateIssuer(tt.issuer)
			if tt.wantErr && err == nil {
				t.Errorf("ValidateIssuer(%q) returned nil, want error", tt.issuer)
			}
			if !tt.wantErr && err != nil {
				t.Errorf("ValidateIssuer(%q) returned error: %v", tt.issuer, err)
			}
		})
	}
}

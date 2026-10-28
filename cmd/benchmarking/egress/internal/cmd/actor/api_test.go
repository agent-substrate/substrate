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

package actor

import "testing"

func TestStartRequestValidate(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name    string
		req     StartRequest
		wantErr bool
	}{
		{"ok", StartRequest{Endpoints: 10}, false},
		{"http", StartRequest{Endpoints: 10, Scheme: SchemeHTTP}, false},
		{"https", StartRequest{Endpoints: 10, Scheme: SchemeHTTPS}, false},
		{"bad scheme", StartRequest{Endpoints: 10, Scheme: "ftp"}, true},
		{"most endpoints", StartRequest{Endpoints: MaxEndpoints}, false},
		{"no endpoints", StartRequest{}, true},
		{"too many endpoints", StartRequest{Endpoints: MaxEndpoints + 1}, true},
		{"negative timeout", StartRequest{Endpoints: 1, RequestTimeoutMs: -1}, true},
		{"negative interval", StartRequest{Endpoints: 1, IntervalMs: -1}, true},
		{"offset", StartRequest{Endpoints: 3, IntervalMs: 100, OffsetMs: 50}, false},
		{"offset of one period", StartRequest{Endpoints: 3, IntervalMs: 100, OffsetMs: 300}, false},
		{"offset over one period", StartRequest{Endpoints: 3, IntervalMs: 100, OffsetMs: 301}, true},
		{"negative offset", StartRequest{Endpoints: 1, IntervalMs: 100, OffsetMs: -1}, true},
		{"offset without interval", StartRequest{Endpoints: 1, OffsetMs: 1}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if err := tc.req.Validate(); (err != nil) != tc.wantErr {
				t.Errorf("Validate() = %v, wantErr %v", err, tc.wantErr)
			}
		})
	}
}

func TestURLScheme(t *testing.T) {
	t.Parallel()
	if got := (&StartRequest{}).URLScheme(); got != SchemeHTTP {
		t.Errorf("URLScheme() with no scheme = %q, want %q", got, SchemeHTTP)
	}
	if got := (&StartRequest{Scheme: SchemeHTTPS}).URLScheme(); got != SchemeHTTPS {
		t.Errorf("URLScheme() with https = %q, want %q", got, SchemeHTTPS)
	}
}

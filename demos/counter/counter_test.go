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
	"net/http/httptest"
	"testing"
)

func TestEgressTargetURL(t *testing.T) {
	for _, tc := range []struct {
		name    string
		path    string
		want    string
		wantErr bool
	}{
		{
			name: "default",
			path: "/egress",
			want: defaultEgressURL,
		},
		{
			name: "custom url",
			path: "/egress?url=https%3A%2F%2Fhttpbin.org%2Fheaders",
			want: "https://httpbin.org/headers",
		},
		{
			name:    "reject missing host",
			path:    "/egress?url=https%3A%2F%2F",
			wantErr: true,
		},
		{
			name:    "reject unsupported scheme",
			path:    "/egress?url=ftp%3A%2F%2Fexample.com%2Ffile",
			wantErr: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest("POST", tc.path, nil)
			got, err := egressTargetURL(req)
			if tc.wantErr {
				if err == nil {
					t.Fatal("egressTargetURL() returned nil error, want error")
				}
				return
			}
			if err != nil {
				t.Fatalf("egressTargetURL() returned error: %v", err)
			}
			if got != tc.want {
				t.Fatalf("egressTargetURL() = %q, want %q", got, tc.want)
			}
		})
	}
}

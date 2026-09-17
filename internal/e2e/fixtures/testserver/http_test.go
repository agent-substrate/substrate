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
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
)

func TestCredentialHandler(t *testing.T) {
	path := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(path, []byte("expected-token"), 0600); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		header string
		status int
	}{
		{"", http.StatusUnauthorized},
		{"Bearer wrong-token", http.StatusUnauthorized},
		{"Bearer expected-token", http.StatusNoContent},
	} {
		req := httptest.NewRequest(http.MethodGet, "/credential", nil)
		req.Header.Set("Authorization", tc.header)
		resp := httptest.NewRecorder()
		credentialHandler(path)(resp, req)
		if resp.Code != tc.status || resp.Body.Len() != 0 {
			t.Errorf("header %q: status=%d body=%q, want status=%d and no body", tc.header, resp.Code, resp.Body.String(), tc.status)
		}
	}
	resp := httptest.NewRecorder()
	credentialHandler(path+"-missing")(resp, httptest.NewRequest(http.MethodGet, "/credential", nil))
	if resp.Code != http.StatusInternalServerError {
		t.Fatalf("unreadable credential file: status=%d, want 500", resp.Code)
	}
}

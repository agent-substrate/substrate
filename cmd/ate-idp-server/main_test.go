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
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestMissingFlags(t *testing.T) {
	tests := []struct {
		flags map[string]string
		want  []string
	}{
		{flags: map[string]string{"--issuer": "https://idp.ate-system.svc", "--jwks-file": "/run/jwks.json"}, want: nil},
		{flags: map[string]string{"--jwks-file": "", "--issuer": "", "--server-cred-bundle": "/run/bundle.pem"}, want: []string{"--issuer", "--jwks-file"}},
	}
	for _, tt := range tests {
		if diff := cmp.Diff(tt.want, missingFlags(tt.flags)); diff != "" {
			t.Errorf("missingFlags(%v) (-want +got):\n%s", tt.flags, diff)
		}
	}
}

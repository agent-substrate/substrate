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

package ateclient

import "testing"

func TestIsJWTAuthModeArg(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want bool
	}{
		{name: "equals jwt", args: []string{"--auth-mode=jwt"}, want: true},
		{name: "split jwt", args: []string{"--auth-mode", "jwt"}, want: true},
		{name: "equals mtls", args: []string{"--auth-mode=mtls"}, want: false},
		{name: "split mtls", args: []string{"--auth-mode", "mtls"}, want: false},
		{name: "missing value", args: []string{"--auth-mode"}, want: false},
		{name: "unrelated", args: []string{"--foo=bar"}, want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isJWTAuthModeArg(tt.args); got != tt.want {
				t.Fatalf("isJWTAuthModeArg(%v) = %v, want %v", tt.args, got, tt.want)
			}
		})
	}
}

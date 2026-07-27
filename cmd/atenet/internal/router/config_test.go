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
	"strings"
	"testing"
)

func TestRouterConfigValidate(t *testing.T) {
	tests := []struct {
		name    string
		cfg     routerConfig
		wantErr string // substring; empty means valid
	}{
		{
			name: "defaults are valid",
			cfg:  routerConfig{ExtProcMaxRequests: defaultExtProcMaxRequests, ParkedRequestMax: defaultParkedRequestMax},
		},
		{
			name:    "zero extproc-max-requests rejected",
			cfg:     routerConfig{ExtProcMaxRequests: 0, ParkedRequestMax: 0},
			wantErr: "must be positive",
		},
		{
			name:    "breaker below the lot rejected",
			cfg:     routerConfig{ExtProcMaxRequests: 512, ParkedRequestMax: 1024},
			wantErr: "must be >= --parked-request-max",
		},
		{
			name: "breaker equal to the lot accepted",
			cfg:  routerConfig{ExtProcMaxRequests: 1024, ParkedRequestMax: 1024},
		},
		{
			name: "parking disabled ignores the relation",
			cfg:  routerConfig{ExtProcMaxRequests: 8, ParkedRequestMax: 0},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.cfg.validate()
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("expected valid, got %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("expected error containing %q, got %v", tc.wantErr, err)
			}
		})
	}
}

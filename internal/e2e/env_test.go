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

package e2e

import (
	"reflect"
	"testing"
)

func TestCheckEnv(t *testing.T) {
	t.Setenv("TEST_ENV_FOO", "value_foo")
	t.Setenv("TEST_ENV_BAR", "value_bar")
	t.Setenv("TEST_ENV_EMPTY", "")

	tests := []struct {
		name    string
		keys    []string
		want    map[string]string
		wantErr bool
	}{
		{
			name: "all env vars present",
			keys: []string{"TEST_ENV_FOO", "TEST_ENV_BAR"},
			want: map[string]string{
				"TEST_ENV_FOO": "value_foo",
				"TEST_ENV_BAR": "value_bar",
			},
			wantErr: false,
		},
		{
			name:    "missing env var",
			keys:    []string{"TEST_ENV_FOO", "TEST_ENV_NONEXISTENT"},
			want:    nil,
			wantErr: true,
		},
		{
			name:    "env var is empty string",
			keys:    []string{"TEST_ENV_FOO", "TEST_ENV_EMPTY"},
			want:    nil,
			wantErr: true,
		},
		{
			name:    "no keys requested",
			keys:    []string{},
			want:    map[string]string{},
			wantErr: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := CheckEnv(tc.keys...)
			if (err != nil) != tc.wantErr {
				t.Errorf("CheckEnv() error = %v, wantErr %v", err, tc.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("CheckEnv() = %v, want %v", got, tc.want)
			}
		})
	}
}

// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cmd

import (
	"os"
	"testing"
)

func TestResolveMachineTypeDefault(t *testing.T) {
	tests := []struct {
		name   string
		newVal string
		oldVal string
		want   string
	}{
		{
			name: "neither set",
			want: "c3-standard-4",
		},
		{
			name:   "new name set",
			newVal: "n4-standard-8",
			want:   "n4-standard-8",
		},
		{
			name:   "old name still honored",
			oldVal: "n2-standard-8",
			want:   "n2-standard-8",
		},
		{
			name:   "new name wins over old",
			newVal: "n4-standard-8",
			oldVal: "n2-standard-8",
			want:   "n4-standard-8",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			setOrUnset(t, "NODE_MACHINE_TYPE", tt.newVal)
			setOrUnset(t, "GVISOR_NODE_MACHINE_TYPE", tt.oldVal)

			if got := resolveMachineTypeDefault(); got != tt.want {
				t.Errorf("resolveMachineTypeDefault() = %q, want %q", got, tt.want)
			}
		})
	}
}

// setOrUnset sets key to val, or removes key from the environment when val is
// empty. t.Setenv registers the restore either way, including for a key that
// started out unset.
func setOrUnset(t *testing.T, key, val string) {
	t.Helper()
	t.Setenv(key, val)
	if val == "" {
		os.Unsetenv(key)
	}
}

func TestGetEnv_String(t *testing.T) {
	const key = "TEST_ENV_STRING_VAR"

	// Ensure clean environment
	os.Unsetenv(key)
	defer os.Unsetenv(key)

	// Test fallback when environment variable is not set
	if got := getEnv(key, "default"); got != "default" {
		t.Errorf("getEnv(%q, %q) = %q; want %q", key, "default", got, "default")
	}

	// Test when environment variable is set
	os.Setenv(key, "hello")
	if got := getEnv(key, "default"); got != "hello" {
		t.Errorf("getEnv(%q, %q) = %q; want %q", key, "default", got, "hello")
	}
}

func TestGetEnv_Bool(t *testing.T) {
	const key = "TEST_ENV_BOOL_VAR"

	// Ensure clean environment
	os.Unsetenv(key)
	defer os.Unsetenv(key)

	// Test fallback when environment variable is not set
	if got := getEnv(key, true); got != true {
		t.Errorf("getEnv(%q, true) = %t; want true", key, got)
	}
	if got := getEnv(key, false); got != false {
		t.Errorf("getEnv(%q, false) = %t; want false", key, got)
	}

	// Test when environment variable is set to valid bool strings
	tests := []struct {
		envVal   string
		fallback bool
		want     bool
	}{
		{"true", false, true},
		{"TRUE", false, true},
		{"1", false, true},
		{"t", false, true},
		{"T", false, true},
		{"false", true, false},
		{"FALSE", true, false},
		{"0", true, false},
		{"f", true, false},
		{"F", true, false},
	}

	for _, tc := range tests {
		os.Setenv(key, tc.envVal)
		if got := getEnv(key, tc.fallback); got != tc.want {
			t.Errorf("getEnv(%q, %t) with env %q = %t; want %t", key, tc.fallback, tc.envVal, got, tc.want)
		}
	}

	// Test when environment variable is set to an invalid bool string
	os.Setenv(key, "invalid_bool")
	if got := getEnv(key, true); got != true {
		t.Errorf("getEnv(%q, true) with invalid env = %t; want true", key, got)
	}
	if got := getEnv(key, false); got != false {
		t.Errorf("getEnv(%q, false) with invalid env = %t; want false", key, got)
	}
}

func TestGetEnv_Int32(t *testing.T) {
	const key = "TEST_ENV_INT32_VAR"

	// Ensure clean environment
	os.Unsetenv(key)
	defer os.Unsetenv(key)

	// Test fallback when environment variable is not set
	if got := getEnv(key, int32(500)); got != int32(500) {
		t.Errorf("getEnv(%q, %d) = %d; want %d", key, 500, got, 500)
	}

	// Test when environment variable is set
	os.Setenv(key, "250")
	if got := getEnv(key, int32(500)); got != int32(250) {
		t.Errorf("getEnv(%q, %d) = %d; want %d", key, 500, got, 250)
	}

	// Test when environment variable is invalid int32 string
	os.Setenv(key, "invalid")
	if got := getEnv(key, int32(500)); got != int32(500) {
		t.Errorf("getEnv(%q, %d) with invalid env = %d; want %d", key, 500, got, 500)
	}

	// Test when environment variable exceeds int32 range
	os.Setenv(key, "5000000000")
	if got := getEnv(key, int32(500)); got != int32(500) {
		t.Errorf("getEnv(%q, %d) with out-of-range env = %d; want %d", key, 500, got, 500)
	}
}

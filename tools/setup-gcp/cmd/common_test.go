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

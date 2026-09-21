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
	"bytes"
	"log/slog"
	"os"
	"strings"
	"testing"

	"github.com/spf13/cobra"
)

func TestResolveMachineTypeDefault(t *testing.T) {
	tests := []struct {
		name   string
		newVal *string
		oldVal *string
		want   string
	}{
		{
			name: "neither set",
			want: "c3-standard-4",
		},
		{
			name:   "new name set",
			newVal: ptr("n4-standard-8"),
			want:   "n4-standard-8",
		},
		{
			name:   "old name still honored",
			oldVal: ptr("n2-standard-8"),
			want:   "n2-standard-8",
		},
		{
			name:   "new name wins over old",
			newVal: ptr("n4-standard-8"),
			oldVal: ptr("n2-standard-8"),
			want:   "n4-standard-8",
		},
		{
			name:   "empty new name does not suppress the old one",
			newVal: ptr(""),
			oldVal: ptr("n2-standard-8"),
			want:   "n2-standard-8",
		},
		{
			name:   "empty values fall back to the default",
			newVal: ptr(""),
			oldVal: ptr(""),
			want:   "c3-standard-4",
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

func TestWarnDeprecatedMachineTypeEnv(t *testing.T) {
	tests := []struct {
		name     string
		newVal   *string
		oldVal   *string
		flagArgs []string
		wantWarn bool
	}{
		{
			name:     "old name alone warns",
			oldVal:   ptr("n2-standard-8"),
			wantWarn: true,
		},
		{
			name: "neither set is quiet",
		},
		{
			name:   "new name alone is quiet",
			newVal: ptr("n4-standard-8"),
		},
		{
			name:   "new name takes precedence, so no warning",
			newVal: ptr("n4-standard-8"),
			oldVal: ptr("n2-standard-8"),
		},
		{
			name:     "explicit flag makes the variable moot",
			oldVal:   ptr("n2-standard-8"),
			flagArgs: []string{"--machine-type=n4-standard-8"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			setOrUnset(t, "NODE_MACHINE_TYPE", tt.newVal)
			setOrUnset(t, "GVISOR_NODE_MACHINE_TYPE", tt.oldVal)

			var buf bytes.Buffer
			restore := slog.Default()
			slog.SetDefault(slog.New(slog.NewTextHandler(&buf, nil)))
			t.Cleanup(func() { slog.SetDefault(restore) })

			cmd := &cobra.Command{Use: "test"}
			cmd.Flags().String("machine-type", resolveMachineTypeDefault(), "")
			if err := cmd.ParseFlags(tt.flagArgs); err != nil {
				t.Fatalf("ParseFlags(%v) = %v", tt.flagArgs, err)
			}

			warnDeprecatedMachineTypeEnv(cmd)

			if got := strings.Contains(buf.String(), "GVISOR_NODE_MACHINE_TYPE is deprecated"); got != tt.wantWarn {
				t.Errorf("warned = %t, want %t (log: %q)", got, tt.wantWarn, buf.String())
			}
		})
	}
}

func ptr[T any](v T) *T { return &v }

// setOrUnset exports key with the given value, or removes key from the
// environment when val is nil. t.Setenv registers the restore either way,
// including for a key that started out unset.
func setOrUnset(t *testing.T, key string, val *string) {
	t.Helper()
	if val == nil {
		t.Setenv(key, "")
		os.Unsetenv(key)
		return
	}
	t.Setenv(key, *val)
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

// ParseBool rejects spellings such as "off" and "no", so a value meant to turn a
// default-on knob off would otherwise leave it on without a word.
func TestGetEnv_WarnsOnUnparsableValue(t *testing.T) {
	const key = "TEST_ENV_UNPARSABLE_VAR"
	t.Setenv(key, "off")
	t.Cleanup(func() { warnedEnvKeys.Delete(key) })

	var buf bytes.Buffer
	restore := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, nil)))
	t.Cleanup(func() { slog.SetDefault(restore) })

	if got := getEnv(key, true); got != true {
		t.Errorf("getEnv(%q, true) = %t; want true", key, got)
	}
	if !strings.Contains(buf.String(), key) {
		t.Errorf("getEnv(%q, true) logged %q; want a warning naming the variable", key, buf.String())
	}

	// Several commands register a flag for the same variable, so the warning
	// must not repeat once per registration.
	buf.Reset()
	getEnv(key, true)
	if buf.Len() != 0 {
		t.Errorf("second getEnv(%q, true) logged %q; want nothing", key, buf.String())
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

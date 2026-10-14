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

package cmd

import (
	"bytes"
	"strings"
	"testing"
)

func resetRootFlags(t *testing.T) {
	t.Helper()
	rootCmd.SetArgs(nil)
	rootCmd.SetOut(nil)
	rootCmd.SetErr(nil)
	if f := rootCmd.Flags().Lookup("version"); f != nil {
		_ = f.Value.Set("false")
		f.Changed = false
	}
	if f := rootCmd.PersistentFlags().Lookup("verbose"); f != nil {
		_ = f.Value.Set("0")
		f.Changed = false
	}
	verbosity = 0
}

func TestVersionFlag(t *testing.T) {
	t.Cleanup(func() { resetRootFlags(t) })

	var out, errOut bytes.Buffer
	rootCmd.SetOut(&out)
	rootCmd.SetErr(&errOut)
	rootCmd.SetArgs([]string{"--version"})

	if err := rootCmd.Execute(); err != nil {
		t.Fatalf("Execute(--version) unexpected error: %v", err)
	}
	if !strings.HasPrefix(out.String(), "kubectl-ate version ") {
		t.Errorf("Execute(--version) output = %q, want prefix %q", out.String(), "kubectl-ate version ")
	}

	if f := rootCmd.Flags().Lookup("version"); f != nil && f.Shorthand != "" {
		t.Errorf("--version shorthand = %q, want empty", f.Shorthand)
	}

	resetRootFlags(t)
	out.Reset()
	errOut.Reset()
	rootCmd.SetOut(&out)
	rootCmd.SetErr(&errOut)
	rootCmd.SetArgs([]string{"-v"})
	if err := rootCmd.Execute(); err != nil {
		t.Fatalf("Execute(-v) unexpected error: %v", err)
	}
	if strings.HasPrefix(out.String(), "kubectl-ate version ") {
		t.Errorf("Execute(-v) printed version %q, want help output", out.String())
	}
}

func TestVerboseFlagCounting(t *testing.T) {
	t.Cleanup(func() { resetRootFlags(t) })

	tests := []struct {
		name string
		args []string
		want int
	}{
		{name: "default", args: nil, want: 0},
		{name: "single short", args: []string{"-v"}, want: 1},
		{name: "stacked short 2", args: []string{"-vv"}, want: 2},
		{name: "stacked short 3", args: []string{"-vvv"}, want: 3},
		{name: "stacked short 5", args: []string{"-vvvvv"}, want: 5},
		{name: "repeated short", args: []string{"-v", "-v", "-v"}, want: 3},
		{name: "single long", args: []string{"--verbose"}, want: 1},
		{name: "repeated long", args: []string{"--verbose", "--verbose"}, want: 2},
		{name: "mixed short and long", args: []string{"-vv", "--verbose"}, want: 3},
		{name: "explicit value", args: []string{"--verbose=4"}, want: 4},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			resetRootFlags(t)
			if err := rootCmd.PersistentFlags().Parse(tc.args); err != nil {
				t.Fatalf("Parse(%v) unexpected error: %v", tc.args, err)
			}
			if verbosity != tc.want {
				t.Errorf("verbosity = %d, want %d", verbosity, tc.want)
			}
		})
	}
}

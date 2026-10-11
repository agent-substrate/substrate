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

func TestVersionFlag(t *testing.T) {
	t.Cleanup(func() {
		rootCmd.SetArgs(nil)
		rootCmd.SetOut(nil)
		rootCmd.SetErr(nil)
		if f := rootCmd.Flags().Lookup("version"); f != nil {
			_ = f.Value.Set("false")
			f.Changed = false
		}
	})

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

	if f := rootCmd.Flags().Lookup("version"); f != nil {
		if f.Shorthand != "" {
			t.Errorf("--version shorthand = %q, want empty", f.Shorthand)
		}
		_ = f.Value.Set("false")
		f.Changed = false
	}

	out.Reset()
	errOut.Reset()
	rootCmd.SetArgs([]string{"-v"})
	err := rootCmd.Execute()
	if err == nil || !strings.Contains(err.Error(), "unknown shorthand flag: 'v'") {
		t.Fatalf("Execute(-v) error = %v, want unknown shorthand flag 'v'", err)
	}
}

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
	"testing"

	"github.com/spf13/cobra"
)

// The two commands share a name under different parents, which cobra allows
// and a mistake in init() would silently turn into one command registered
// twice under the same parent.
func TestMicroVMDepsIsRegisteredUnderBothVerbs(t *testing.T) {
	for _, tc := range []struct {
		parent *cobra.Command
		want   *cobra.Command
	}{
		{deployCmd, deployMicroVMDepsCmd},
		{deleteCmd, deleteMicroVMDepsCmd},
	} {
		t.Run(tc.parent.Name(), func(t *testing.T) {
			found, _, err := tc.parent.Find([]string{"microvm-deps"})
			if err != nil {
				t.Fatalf("%s microvm-deps: %v", tc.parent.Name(), err)
			}
			if found != tc.want {
				t.Fatalf("%s microvm-deps resolved to %v, want the %s subcommand",
					tc.parent.Name(), found.CommandPath(), tc.parent.Name())
			}
			// Staging assets and deleting a cluster-wide SandboxConfig are
			// both worth refusing on a typo rather than guessing at.
			if err := found.Args(found, []string{"stray"}); err == nil {
				t.Error("Args([stray]) = nil, want an error")
			}
		})
	}
}

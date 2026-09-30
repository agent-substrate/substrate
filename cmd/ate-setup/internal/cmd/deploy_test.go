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

import "testing"

// An upgrade or a rollback moves the control plane one component at a time,
// so the components that only deploy ate-system used to install have their own
// subcommands.
func TestComponentDeploySubcommands(t *testing.T) {
	for _, tc := range []struct {
		arg  string
		want string
	}{
		{"podcertificate-controller", "podcertificate-controller"},
		{"podcert", "podcertificate-controller"},
		{"sandboxconfig", "sandboxconfig"},
	} {
		t.Run(tc.arg, func(t *testing.T) {
			sub, _, err := deployCmd.Find([]string{tc.arg})
			if err != nil {
				t.Fatalf("deploy %s: %v", tc.arg, err)
			}
			if sub.Name() != tc.want {
				t.Fatalf("deploy %s resolved to %q, want %q", tc.arg, sub.Name(), tc.want)
			}
			if err := sub.Args(sub, []string{"extra"}); err == nil {
				t.Errorf("deploy %s accepted a stray argument", tc.arg)
			}
		})
	}
}

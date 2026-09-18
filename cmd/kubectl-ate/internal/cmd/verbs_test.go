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

// commandArgsTest checks a command's positional-argument validation.
type commandArgsTest struct {
	name    string
	command *cobra.Command
	args    []string
	wantErr bool
}

func runCommandArgsTests(t *testing.T, tests []commandArgsTest) {
	t.Helper()
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var err error
			if test.command.Args != nil {
				err = test.command.Args(test.command, test.args)
			}
			if (err != nil) != test.wantErr {
				t.Fatalf("Args(%q) error = %v, wantErr %t", test.args, err, test.wantErr)
			}
		})
	}
}

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
	"io"
	"strings"
	"testing"

	"github.com/spf13/cobra"

	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/steps"
)

// --keep-node-state leaves everything, caches included, so asking for the
// caches to be wiped as well is a contradiction. It is rejected before the
// command runs, rather than resolved by silently ignoring one of the two.
func TestDeleteFlags(t *testing.T) {
	for _, tc := range []struct {
		args    []string
		want    steps.DeleteOptions
		wantErr bool
	}{
		{args: nil},
		{args: []string{"--keep-node-state"}, want: steps.DeleteOptions{KeepNodeState: true}},
		{args: []string{"--wipe-node-caches"}, want: steps.DeleteOptions{WipeNodeCaches: true}},
		{args: []string{"--keep-node-state", "--wipe-node-caches"}, wantErr: true},
	} {
		t.Run(strings.Join(tc.args, " "), func(t *testing.T) {
			var got steps.DeleteOptions
			ran := false
			cmd := &cobra.Command{Use: "delete", RunE: func(*cobra.Command, []string) error {
				ran = true
				return nil
			}}
			registerDeleteFlags(cmd, &got)
			cmd.SetArgs(tc.args)
			cmd.SetOut(io.Discard)
			cmd.SetErr(io.Discard)

			err := cmd.Execute()
			if tc.wantErr {
				if err == nil || ran {
					t.Errorf("Execute(%q) = %v, ran = %v; want it rejected before running", tc.args, err, ran)
				}
				return
			}
			if err != nil {
				t.Fatalf("Execute(%q) = %v", tc.args, err)
			}
			if got != tc.want {
				t.Errorf("Execute(%q) set %+v, want %+v", tc.args, got, tc.want)
			}
		})
	}
}

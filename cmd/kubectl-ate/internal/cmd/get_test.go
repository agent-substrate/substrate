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
	"path/filepath"
	"strings"
	"testing"

	"github.com/spf13/cobra"
)

func TestGetScopeBeforeConnecting(t *testing.T) {
	previousKubeconfig := kubeconfig
	kubeconfig = filepath.Join(t.TempDir(), "missing-kubeconfig")
	t.Cleanup(func() { kubeconfig = previousKubeconfig })

	for _, resource := range []struct {
		name     string
		command  *cobra.Command
		atespace *string
		all      *bool
	}{
		{"actors", getActorsCmd, &getActorAtespaceFlag, &getActorAllAtespacesFlag},
		{"actor templates", getActorTemplatesCmd, &getActorTemplateAtespaceFlag, &getActorTemplateAllAtespacesFlag},
		{"tags", getTagsCmd, &getTagAtespaceFlag, &getTagAllAtespacesFlag},
	} {
		t.Run(resource.name, func(t *testing.T) {
			previousAtespace, previousAll := *resource.atespace, *resource.all
			previousContext := resource.command.Context()
			resource.command.SetContext(t.Context())
			t.Cleanup(func() {
				*resource.atespace, *resource.all = previousAtespace, previousAll
				resource.command.SetContext(previousContext)
			})

			for _, test := range []struct {
				name     string
				args     []string
				atespace string
				all      bool
				wantErr  string
			}{
				{
					name: "get without atespace", args: []string{"counter"},
					wantErr: "-a/--atespace is required when getting",
				},
				{
					name: "get with all atespaces", args: []string{"counter"}, all: true,
					wantErr: "-A/--all-atespaces cannot be used when getting",
				},
				{
					name: "get with both scopes", args: []string{"counter"}, atespace: "demo", all: true,
					wantErr: "-A/--all-atespaces cannot be used when getting",
				},
				{
					name:    "list without scope",
					wantErr: "-a/--atespace <name> or -A/--all-atespaces is required",
				},
				{
					name: "list with both scopes", atespace: "demo", all: true,
					wantErr: "-a/--atespace and -A/--all-atespaces are mutually exclusive",
				},
				// Valid scopes reach client initialization, where the missing kubeconfig fails.
				{
					name: "get in atespace", args: []string{"counter"}, atespace: "demo",
					wantErr: "failed to load kubeconfig",
				},
				{
					name: "get multiple in atespace", args: []string{"counter", "other"}, atespace: "demo",
					wantErr: "failed to load kubeconfig",
				},
				{
					name: "list in atespace", atespace: "demo",
					wantErr: "failed to load kubeconfig",
				},
				{
					name: "list all atespaces", all: true,
					wantErr: "failed to load kubeconfig",
				},
			} {
				t.Run(test.name, func(t *testing.T) {
					*resource.atespace, *resource.all = test.atespace, test.all
					err := resource.command.RunE(resource.command, test.args)
					if err == nil || !strings.Contains(err.Error(), test.wantErr) {
						t.Fatalf("RunE() error = %v, want error containing %q", err, test.wantErr)
					}
				})
			}
		})
	}
}

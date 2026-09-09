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
	"fmt"

	"github.com/agent-substrate/substrate/cmd/kubectl-ate/internal/printer"
	"github.com/agent-substrate/substrate/internal/ateclient"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/spf13/cobra"
)

var (
	getActorTemplatesAtespaceFlag string
	getActorTemplatesAllAtespaces bool
)

var getActorTemplatesCmd = &cobra.Command{
	Use:     "actor-template <template-name ...>",
	Aliases: []string{"actor-templates"},
	Short:   "List all actor templates or get one or more actor templates",
	Long:    "List or get actor templates.",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := cmd.Context()

		apiClient, err := ateclient.NewClient(ctx, kubeconfig, k8sContext, endpoint, tokenFile, traceEnabled)
		if err != nil {
			return fmt.Errorf("failed to connect to ate-api-server: %w", err)
		}
		defer apiClient.Close()

		if len(args) > 0 {
			// A template is addressed by (atespace, name), so the atespace is
			// mandatory and "all atespaces" is meaningless here.
			if getActorTemplatesAllAtespaces {
				return fmt.Errorf("-A/--all-atespaces cannot be used when getting actor templates; pass --atespace")
			}
			if getActorTemplatesAtespaceFlag == "" {
				return fmt.Errorf("--atespace is required when getting actor templates")
			}

			templates := make([]*ateapipb.ActorTemplate, 0, len(args))
			for _, name := range args {
				resp, err := apiClient.GetActorTemplate(ctx, &ateapipb.GetActorTemplateRequest{
					ActorTemplate: &ateapipb.ObjectRef{Atespace: getActorTemplatesAtespaceFlag, Name: name},
				})
				if err != nil {
					return fmt.Errorf("failed to get actor template %q: %w", name, err)
				}
				templates = append(templates, resp)
			}
			return printer.PrintActorTemplates(templates, outputFmt)
		}

		// Listing requires exactly one of --atespace (one atespace) or -A (all
		// atespaces). There is no default atespace to fall back on.
		if getActorTemplatesAllAtespaces && getActorTemplatesAtespaceFlag != "" {
			return fmt.Errorf("--atespace and -A/--all-atespaces are mutually exclusive")
		}
		if !getActorTemplatesAllAtespaces && getActorTemplatesAtespaceFlag == "" {
			return fmt.Errorf("specify --atespace <name> to list one atespace, or -A/--all-atespaces for all")
		}

		var allTemplates []*ateapipb.ActorTemplate
		pageToken := ""
		for {
			resp, err := apiClient.ListActorTemplates(ctx, &ateapipb.ListActorTemplatesRequest{
				PageSize:  100,
				PageToken: pageToken,
				Atespace:  getActorTemplatesAtespaceFlag,
			})
			if err != nil {
				return fmt.Errorf("failed to list actor templates: %w", err)
			}
			allTemplates = append(allTemplates, resp.GetActorTemplates()...)

			pageToken = resp.GetNextPageToken()
			if pageToken == "" {
				break
			}
		}

		return printer.PrintActorTemplates(allTemplates, outputFmt)
	},
}

func init() {
	getActorTemplatesCmd.Flags().StringVarP(&getActorTemplatesAtespaceFlag, "atespace", "a", "", "Atespace to list/get actor templates in. Required when getting templates; for listing, use this or -A.")
	getActorTemplatesCmd.Flags().BoolVarP(&getActorTemplatesAllAtespaces, "all-atespaces", "A", false, "List actor templates across all atespaces (listing only; mutually exclusive with --atespace)")
	getCmd.AddCommand(getActorTemplatesCmd)
}

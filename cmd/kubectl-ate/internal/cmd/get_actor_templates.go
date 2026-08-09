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

var getActorTemplatesCmd = &cobra.Command{
	Use:     "actor-templates <name ...>",
	Aliases: []string{"actor-template"},
	Short:   "List all actor templates or get one or more actor templates",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := cmd.Context()
		apiClient, err := ateclient.NewClient(ctx, kubeconfig, k8sContext, endpoint, traceEnabled)
		if err != nil {
			return fmt.Errorf("failed to connect to ate-api-server: %w", err)
		}
		defer apiClient.Close()

		if len(args) > 0 {
			templates := make([]*ateapipb.ActorTemplate, 0, len(args))
			for _, name := range args {
				resp, err := apiClient.GetActorTemplate(ctx, &ateapipb.GetActorTemplateRequest{
					ActorTemplate: &ateapipb.ObjectRef{Name: name},
				})
				if err != nil {
					return fmt.Errorf("failed to get actor template %q: %w", name, err)
				}
				templates = append(templates, resp)
			}
			return printer.PrintActorTemplates(templates, outputFmt)
		}

		var templates []*ateapipb.ActorTemplate
		pageToken := ""
		for {
			resp, err := apiClient.ListActorTemplates(ctx, &ateapipb.ListActorTemplatesRequest{
				PageSize:  1000,
				PageToken: pageToken,
			})
			if err != nil {
				return fmt.Errorf("failed to list actor templates: %w", err)
			}
			templates = append(templates, resp.GetActorTemplates()...)
			pageToken = resp.GetNextPageToken()
			if pageToken == "" {
				break
			}
		}
		return printer.PrintActorTemplates(templates, outputFmt)
	},
}

func init() {
	getCmd.AddCommand(getActorTemplatesCmd)
}

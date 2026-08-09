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

var getActorTemplateVersionsTemplateFlag string

var getActorTemplateVersionsCmd = &cobra.Command{
	Use:     "actor-template-versions <name ...>",
	Aliases: []string{"actor-template-version"},
	Short:   "List actor template versions or get one or more actor template versions",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := cmd.Context()
		apiClient, err := ateclient.NewClient(ctx, kubeconfig, k8sContext, endpoint, traceEnabled)
		if err != nil {
			return fmt.Errorf("failed to connect to ate-api-server: %w", err)
		}
		defer apiClient.Close()

		if len(args) > 0 {
			if getActorTemplateVersionsTemplateFlag != "" {
				return fmt.Errorf("--template only filters listings; it cannot be used when getting versions by name")
			}
			versions := make([]*ateapipb.ActorTemplateVersion, 0, len(args))
			for _, name := range args {
				resp, err := apiClient.GetActorTemplateVersion(ctx, &ateapipb.GetActorTemplateVersionRequest{
					ActorTemplateVersion: &ateapipb.ObjectRef{Name: name},
				})
				if err != nil {
					return fmt.Errorf("failed to get actor template version %q: %w", name, err)
				}
				versions = append(versions, resp)
			}
			return printer.PrintActorTemplateVersions(versions, outputFmt)
		}

		var versions []*ateapipb.ActorTemplateVersion
		pageToken := ""
		for {
			resp, err := apiClient.ListActorTemplateVersions(ctx, &ateapipb.ListActorTemplateVersionsRequest{
				ActorTemplate: getActorTemplateVersionsTemplateFlag,
				PageSize:      1000,
				PageToken:     pageToken,
			})
			if err != nil {
				return fmt.Errorf("failed to list actor template versions: %w", err)
			}
			versions = append(versions, resp.GetActorTemplateVersions()...)
			pageToken = resp.GetNextPageToken()
			if pageToken == "" {
				break
			}
		}
		return printer.PrintActorTemplateVersions(versions, outputFmt)
	},
}

func init() {
	getActorTemplateVersionsCmd.Flags().StringVar(&getActorTemplateVersionsTemplateFlag, "template", "", "Only list versions of this actor template")
	getCmd.AddCommand(getActorTemplateVersionsCmd)
}

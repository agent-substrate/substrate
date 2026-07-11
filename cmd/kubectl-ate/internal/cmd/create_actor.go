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
	"strings"

	"github.com/agent-substrate/substrate/cmd/kubectl-ate/internal/printer"
	"github.com/agent-substrate/substrate/internal/ateclient"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/spf13/cobra"
)

var templateFlag string
var atespaceFlag string

var createActorCmd = &cobra.Command{
	Use:   "actor <actor-name>",
	Short: "Create an actor",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := cmd.Context()
		apiClient, err := ateclient.NewClient(ctx, kubeconfig, k8sContext, endpoint, traceEnabled)
		if err != nil {
			return fmt.Errorf("failed to connect to ate-api-server: %w", err)
		}
		defer apiClient.Close()

		actorName := args[0]
		parts := strings.Split(templateFlag, "/")
		if len(parts) != 2 {
			return fmt.Errorf("malformed --template: %s (expected <namespace>/<name>)", templateFlag)
		}

		resp, err := apiClient.CreateActor(ctx, &ateapipb.CreateActorRequest{
			Actor: &ateapipb.Actor{
				Metadata: &ateapipb.ResourceMetadata{
					Atespace: atespaceFlag,
					Name:     actorName,
				},
				ActorTemplateNamespace: parts[0],
				ActorTemplateName:      parts[1],
			},
		})
		if err != nil {
			return fmt.Errorf("failed to create actor: %w", err)
		}

		return printer.PrintActor(resp, outputFmt)
	},
}

func init() {
	createActorCmd.Flags().StringVarP(&templateFlag, "template", "t", "", "Template to derive the actor from in <namespace>/<name> format (required)")
	_ = createActorCmd.MarkFlagRequired("template")
	createActorCmd.Flags().StringVarP(&atespaceFlag, "atespace", "a", "", "Atespace to create the actor in (required)")
	_ = createActorCmd.MarkFlagRequired("atespace")
	createCmd.AddCommand(createActorCmd)
}

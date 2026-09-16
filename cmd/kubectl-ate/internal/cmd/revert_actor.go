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
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/spf13/cobra"
)

var revertAtespaceFlag string

var revertActorCmd = &cobra.Command{
	Use:   "actor <actor-name>",
	Short: "Revert an actor to its latest snapshot",
	Long: `Revert an actor to SUSPENDED, discarding its current execution.

The actor's external snapshot is left untouched, so a later resume restores
from it. Only crashed, running, or paused actors can be reverted.`,
	Args: cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := cmd.Context()
		apiClient, err := ateclient.NewClient(ctx, kubeconfig, k8sContext, endpoint, tokenFile, traceEnabled)
		if err != nil {
			return fmt.Errorf("failed to connect to ate-api-server: %w", err)
		}
		defer apiClient.Close()

		actorRef := resources.ActorRef{Atespace: revertAtespaceFlag, Name: args[0]}
		resp, err := apiClient.RevertActor(ctx, &ateapipb.RevertActorRequest{
			Actor: actorRef.ToObjectRef(),
		})
		if err != nil {
			return fmt.Errorf("failed to revert actor: %w", err)
		}

		return printer.PrintActorTo(cmd.OutOrStdout(), resp.GetActor(), outputFmt)
	},
}

func init() {
	revertActorCmd.Flags().StringVarP(&revertAtespaceFlag, "atespace", "a", "", "Atespace the actor lives in")
	_ = revertActorCmd.MarkFlagRequired("atespace")
	revertCmd.AddCommand(revertActorCmd)
}

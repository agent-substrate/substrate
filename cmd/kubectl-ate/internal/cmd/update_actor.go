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
	"github.com/agent-substrate/substrate/internal/egress"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/spf13/cobra"
)

var updateActorAtespaceFlag string
var updateActorEgressPEPFlag string

var updateActorCmd = &cobra.Command{
	Use:   "actor <actor-name>",
	Short: "Update an actor",
	Long: "Update mutable fields on an actor. Changes take effect on the next resume. " +
		"Use --egress-pep to set the ate.dev/use-egress-pep selector to a <host>:<port> " +
		"PEP address; pass an empty value to clear it. Other mutable fields (e.g. the " +
		"worker selector) are preserved.",
	Args: cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := cmd.Context()
		apiClient, err := ateclient.NewClient(ctx, kubeconfig, k8sContext, endpoint, traceEnabled)
		if err != nil {
			return fmt.Errorf("failed to connect to ate-api-server: %w", err)
		}
		defer apiClient.Close()

		actorRef := &ateapipb.ObjectRef{Atespace: updateActorAtespaceFlag, Name: args[0]}

		// UpdateActor merges labels (empty value deletes the key), so only the
		// changed label is sent. The worker selector is still replaced wholesale,
		// so read the current actor first to echo it back unchanged.
		// TODO: UpdateActorRequest carries no version token, so a concurrent
		// update landing between this read and the write below is silently lost.
		current, err := apiClient.GetActor(ctx, &ateapipb.GetActorRequest{Actor: actorRef})
		if err != nil {
			return fmt.Errorf("failed to get actor: %w", err)
		}

		var labels map[string]string
		if cmd.Flags().Changed("egress-pep") {
			labels = map[string]string{egress.LabelUseEgressPEP: updateActorEgressPEPFlag}
		}

		resp, err := apiClient.UpdateActor(ctx, &ateapipb.UpdateActorRequest{
			Actor:          actorRef,
			WorkerSelector: current.GetWorkerSelector(),
			Labels:         labels,
		})
		if err != nil {
			return fmt.Errorf("failed to update actor: %w", err)
		}

		return printer.PrintActor(resp.GetActor(), outputFmt)
	},
}

func init() {
	updateActorCmd.Flags().StringVarP(&updateActorAtespaceFlag, "atespace", "a", "", "Atespace the actor lives in (required)")
	_ = updateActorCmd.MarkFlagRequired("atespace")
	updateActorCmd.Flags().StringVar(&updateActorEgressPEPFlag, "egress-pep", "", "Egress PEP address for this actor, as <host>:<port>. Sets the ate.dev/use-egress-pep selector; empty clears it. Takes effect on the next resume.")
	updateCmd.AddCommand(updateActorCmd)
}

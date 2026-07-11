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

var pauseAtespaceFlag string

var pauseActorCmd = &cobra.Command{
	Use:   "actor <actor-name>",
	Short: "Pause an actor",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := cmd.Context()
		apiClient, err := ateclient.NewClient(ctx, kubeconfig, k8sContext, endpoint, traceEnabled)
		if err != nil {
			return fmt.Errorf("failed to connect to ate-api-server: %w", err)
		}
		defer apiClient.Close()

		resp, err := apiClient.PauseActor(ctx, &ateapipb.PauseActorRequest{
			Actor: &ateapipb.ObjectRef{Atespace: pauseAtespaceFlag, Name: args[0]},
		})
		if err != nil {
			return fmt.Errorf("failed to pause actor: %w", err)
		}

		return printer.PrintActor(resp.GetActor(), outputFmt)
	},
}

func init() {
	pauseActorCmd.Flags().StringVarP(&pauseAtespaceFlag, "atespace", "a", "", "Atespace the actor lives in")
	_ = pauseActorCmd.MarkFlagRequired("atespace")
	pauseCmd.AddCommand(pauseActorCmd)
}

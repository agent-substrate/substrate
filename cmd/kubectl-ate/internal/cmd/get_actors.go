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

var getActorsCmd = &cobra.Command{
	Use:     "actors [actor-id]",
	Aliases: []string{"actor"},
	Short:   "List all actors or get a specific actor",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := cmd.Context()

		// 1. Connect to API Server
		apiClient, err := ateclient.NewClient(ctx, kubeconfig, k8sContext, endpoint, traceEnabled)
		if err != nil {
			return fmt.Errorf("failed to connect to ate-api-server: %w", err)
		}
		defer apiClient.Close()

		// 2. Handle Get Single Actor
		if len(args) > 0 {
			resp, err := apiClient.GetActor(ctx, &ateapipb.GetActorRequest{ActorId: args[0]})
			if err != nil {
				return fmt.Errorf("failed to get actor: %w", err)
			}
			return printer.PrintActor(resp.GetActor(), outputFmt)
		}

		// 3. Handle List All Actors
		resp, err := apiClient.ListActors(ctx, &ateapipb.ListActorsRequest{})
		if err != nil {
			return fmt.Errorf("failed to list actors: %w", err)
		}
		return printer.PrintActors(resp.GetActors(), outputFmt)
	},
}

func init() {
	getCmd.AddCommand(getActorsCmd)
}

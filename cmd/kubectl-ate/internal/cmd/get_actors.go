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
	getActorsAtespaceFlag string
	getActorsAllAtespaces bool
)

var getActorsCmd = &cobra.Command{
	Use:     "actors <actor-name>",
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
			// A single actor is addressed by (atespace, name), so the atespace is
			// mandatory and "all atespaces" is meaningless here.
			if getActorsAllAtespaces {
				return fmt.Errorf("-A/--all-atespaces cannot be used when getting a specific actor; pass --atespace")
			}
			if getActorsAtespaceFlag == "" {
				return fmt.Errorf("--atespace is required when getting a specific actor")
			}
			resp, err := apiClient.GetActor(ctx, &ateapipb.GetActorRequest{Actor: &ateapipb.ObjectRef{Atespace: getActorsAtespaceFlag, Name: args[0]}})
			if err != nil {
				return fmt.Errorf("failed to get actor: %w", err)
			}
			return printer.PrintActor(resp, outputFmt)
		}

		// Listing requires exactly one of --atespace (one atespace) or -A (all
		// atespaces). There is no default atespace to fall back on.
		if getActorsAllAtespaces && getActorsAtespaceFlag != "" {
			return fmt.Errorf("--atespace and -A/--all-atespaces are mutually exclusive")
		}
		if !getActorsAllAtespaces && getActorsAtespaceFlag == "" {
			return fmt.Errorf("specify --atespace <name> to list one atespace, or -A/--all-atespaces for all")
		}

		// 3. Handle List All Actors
		var allActors []*ateapipb.Actor
		pageToken := ""

		for {
			resp, err := apiClient.ListActors(ctx, &ateapipb.ListActorsRequest{
				PageSize:  1000,
				PageToken: pageToken,
				Atespace:  getActorsAtespaceFlag,
			})
			if err != nil {
				return fmt.Errorf("failed to list actors: %w", err)
			}
			allActors = append(allActors, resp.GetActors()...)

			pageToken = resp.GetNextPageToken()
			if pageToken == "" {
				break
			}
		}

		return printer.PrintActors(allActors, outputFmt)
	},
}

func init() {
	getActorsCmd.Flags().StringVarP(&getActorsAtespaceFlag, "atespace", "a", "", "Atespace to list/get actors in. Required for a single actor; for listing, use this or -A.")
	getActorsCmd.Flags().BoolVarP(&getActorsAllAtespaces, "all-atespaces", "A", false, "List actors across all atespaces (listing only; mutually exclusive with --atespace)")
	getCmd.AddCommand(getActorsCmd)
}

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
	"context"
	"fmt"

	"github.com/agent-substrate/substrate/cmd/kubectl-ate/internal/printer"
	"github.com/agent-substrate/substrate/internal/ateclient"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/spf13/cobra"
)

var getAtespacesWatch bool

var getAtespacesCmd = &cobra.Command{
	Use:     "atespaces [name ...]",
	Aliases: []string{"atespace"},
	Short:   "List all atespaces or get one or more atespaces",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := cmd.Context()
		apiClient, err := ateclient.NewClient(ctx, kubeconfig, k8sContext, endpoint, traceEnabled)
		if err != nil {
			return fmt.Errorf("failed to connect to ate-api-server: %w", err)
		}
		defer apiClient.Close()

		if len(args) > 0 {
			fetch := func(ctx context.Context) ([]*ateapipb.Atespace, error) {
				atespaces := make([]*ateapipb.Atespace, 0, len(args))
				for _, atespaceName := range args {
					resp, err := apiClient.GetAtespace(ctx, &ateapipb.GetAtespaceRequest{Atespace: &ateapipb.ObjectRef{Name: atespaceName}})
					if err != nil {
						return nil, fmt.Errorf("failed to get atespace %q: %w", atespaceName, err)
					}
					atespaces = append(atespaces, resp)
				}
				return atespaces, nil
			}
			return printOrWatch(ctx, cmd.OutOrStdout(), getAtespacesWatch, outputFmt, fetch, printer.PrintAtespacesTo, atespaceWatchKey)
		}

		fetch := func(ctx context.Context) ([]*ateapipb.Atespace, error) {
			return fetchAllPages(ctx, func(ctx context.Context, pageToken string) ([]*ateapipb.Atespace, string, error) {
				resp, err := apiClient.ListAtespaces(ctx, &ateapipb.ListAtespacesRequest{
					PageSize:  1000,
					PageToken: pageToken,
				})
				if err != nil {
					return nil, "", fmt.Errorf("failed to list atespaces: %w", err)
				}
				return resp.GetAtespaces(), resp.GetNextPageToken(), nil
			})
		}
		return printOrWatch(ctx, cmd.OutOrStdout(), getAtespacesWatch, outputFmt, fetch, printer.PrintAtespacesTo, atespaceWatchKey)
	},
}

func atespaceWatchKey(atespace *ateapipb.Atespace) string {
	return atespace.GetMetadata().GetName()
}

func init() {
	getAtespacesCmd.Flags().BoolVarP(&getAtespacesWatch, "watch", "w", false, "After listing/getting the requested atespace(s), watch for changes")
	getCmd.AddCommand(getAtespacesCmd)
}

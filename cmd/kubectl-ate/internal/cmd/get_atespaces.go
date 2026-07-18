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
			atespaces := make([]*ateapipb.Atespace, 0, len(args))
			for _, atespaceName := range args {
				resp, err := apiClient.GetAtespace(ctx, &ateapipb.GetAtespaceRequest{Atespace: &ateapipb.ObjectRef{Name: atespaceName}})
				if err != nil {
					return fmt.Errorf("failed to get atespace %q: %w", atespaceName, err)
				}
				atespaces = append(atespaces, resp)
			}
			return printer.PrintAtespaces(atespaces, outputFmt)
		}

		var allAtespaces []*ateapipb.Atespace
		pageToken := ""
		for {
			resp, err := apiClient.ListAtespaces(ctx, &ateapipb.ListAtespacesRequest{
				PageSize:  1000,
				PageToken: pageToken,
			})
			if err != nil {
				return fmt.Errorf("failed to list atespaces: %w", err)
			}
			allAtespaces = append(allAtespaces, resp.GetAtespaces()...)

			pageToken = resp.GetNextPageToken()
			if pageToken == "" {
				break
			}
		}
		return printer.PrintAtespaces(allAtespaces, outputFmt)
	},
}

func init() {
	getCmd.AddCommand(getAtespacesCmd)
}

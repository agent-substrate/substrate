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

var getWorkersWatch bool

var getWorkersCmd = &cobra.Command{
	Use:     "workers",
	Aliases: []string{"worker"},
	Short:   "List all workers",
	Args:    cobra.NoArgs,
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := cmd.Context()
		apiClient, err := ateclient.NewClient(ctx, kubeconfig, k8sContext, endpoint, traceEnabled)
		if err != nil {
			return fmt.Errorf("failed to connect to ate-api-server: %w", err)
		}
		defer apiClient.Close()

		fetch := func(ctx context.Context) ([]*ateapipb.Worker, error) {
			return fetchAllPages(ctx, func(ctx context.Context, pageToken string) ([]*ateapipb.Worker, string, error) {
				resp, err := apiClient.ListWorkers(ctx, &ateapipb.ListWorkersRequest{
					PageSize:  1000,
					PageToken: pageToken,
				})
				if err != nil {
					return nil, "", fmt.Errorf("failed to list workers: %w", err)
				}
				return resp.GetWorkers(), resp.GetNextPageToken(), nil
			})
		}
		return printOrWatch(ctx, cmd.OutOrStdout(), getWorkersWatch, outputFmt, fetch, printer.PrintWorkersTo, workerWatchKey)
	},
}

func workerWatchKey(worker *ateapipb.Worker) string {
	return worker.GetWorkerNamespace() + "/" + worker.GetWorkerPool() + "/" + worker.GetWorkerPod()
}

func init() {
	getWorkersCmd.Flags().BoolVarP(&getWorkersWatch, "watch", "w", false, "After listing the requested workers, watch for changes")
	getCmd.AddCommand(getWorkersCmd)
}

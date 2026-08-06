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
	"io"
	"os"

	"github.com/agent-substrate/substrate/cmd/kubectl-ate/internal/printer"
	"github.com/agent-substrate/substrate/internal/ateclient"
	"github.com/spf13/cobra"
)

var (
	getWorkerNamespaceFlag string
	getWorkerAtespaceFlag  string
	getWorkerSelectorFlag  string
)

var getWorkersCmd = &cobra.Command{
	Use:     "workers",
	Aliases: []string{"worker"},
	Short:   "List all workers",
	Args:    cobra.NoArgs,
	RunE:    runGetWorkers,
}

func init() {
	getWorkersCmd.Flags().StringVarP(&getWorkerNamespaceFlag, "namespace", "n", "", "Scope output to a specific Kubernetes namespace")
	getWorkersCmd.Flags().StringVarP(&getWorkerAtespaceFlag, "atespace", "a", "", "Filter worker pods hosting actors in a specific atespace")
	getWorkersCmd.Flags().StringVarP(&getWorkerSelectorFlag, "selector", "l", "", "Filter by worker pool labels")
	getCmd.AddCommand(getWorkersCmd)
}

// GetWorkersRunner executes the get workers command logic.
type GetWorkersRunner struct {
	workerLister WorkerLister
	namespace    string
	atespace     string
	selector     string
	outputFmt    string
	out          io.Writer
}

func (r *GetWorkersRunner) Run(ctx context.Context) error {
	workers, err := listAllWorkers(ctx, r.workerLister)
	if err != nil {
		return err
	}
	filtered, err := filterWorkers(workers, r.namespace, r.atespace, r.selector)
	if err != nil {
		return err
	}

	outWriter := r.out
	if outWriter == nil {
		outWriter = os.Stdout
	}
	return printer.PrintWorkersTo(outWriter, filtered, r.outputFmt)
}

func runGetWorkers(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()
	apiClient, err := ateclient.NewClient(ctx, kubeconfig, k8sContext, endpoint, traceEnabled)
	if err != nil {
		return fmt.Errorf("failed to connect to ate-api-server: %w", err)
	}
	defer apiClient.Close()

	runner := &GetWorkersRunner{
		workerLister: apiClient,
		namespace:    getWorkerNamespaceFlag,
		atespace:     getWorkerAtespaceFlag,
		selector:     getWorkerSelectorFlag,
		outputFmt:    outputFmt,
		out:          os.Stdout,
	}
	return runner.Run(ctx)
}

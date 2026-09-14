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

	"github.com/agent-substrate/substrate/cmd/kubectl-ate/internal/printer"
	"github.com/agent-substrate/substrate/internal/ateclient"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/spf13/cobra"
)

var (
	getWorkerNamespaceFlag string
	getWorkerAtespaceFlag  string
	getWorkerSelectorFlag  string
	getWorkerClassFlag     string
)

var getWorkersCmd = &cobra.Command{
	Use:     "workers <worker-name ...>",
	Aliases: []string{"worker"},
	Short:   "List all workers or get one or more workers",
	RunE:    runGetWorkers,
}

func init() {
	getWorkersCmd.Flags().StringVarP(&getWorkerNamespaceFlag, "namespace", "n", "", "Scope output to a specific Kubernetes namespace")
	getWorkersCmd.Flags().StringVarP(&getWorkerAtespaceFlag, "atespace", "a", "", "Filter worker pods hosting actors in a specific atespace")
	getWorkersCmd.Flags().StringVarP(&getWorkerSelectorFlag, "selector", "l", "", "Filter by worker pool labels")
	getWorkersCmd.Flags().StringVar(&getWorkerClassFlag, "sandbox-class", "", "Filter by sandbox class (e.g. gvisor, microvm)")
	getCmd.AddCommand(getWorkersCmd)
}

// GetWorkersRunner executes the get workers command logic.
type GetWorkersRunner struct {
	workerLister WorkerLister
	workerGetter WorkerGetter
	actorLister  ActorLister
	names        []string
	namespace    string
	atespace     string
	selector     string
	sandboxClass string
	outputFmt    string
	out          io.Writer
}

func (r *GetWorkersRunner) Run(ctx context.Context) error {
	if len(r.names) > 0 {
		if r.namespace != "" || r.atespace != "" || r.selector != "" || r.sandboxClass != "" {
			return fmt.Errorf("filter flags (--namespace, --atespace, --selector, --sandbox-class) cannot be used when getting workers by name")
		}
		workers := make([]*ateapipb.Worker, 0, len(r.names))
		for _, name := range r.names {
			worker, err := r.workerGetter.GetWorker(ctx, &ateapipb.GetWorkerRequest{Worker: &ateapipb.ObjectRef{Name: name}})
			if err != nil {
				return fmt.Errorf("failed to get worker %q: %w", name, err)
			}
			workers = append(workers, worker)
		}
		return printer.PrintWorkersTo(r.out, workers, r.outputFmt)
	}

	workers, err := listAllWorkers(ctx, r.workerLister)
	if err != nil {
		return err
	}
	filtered, err := filterWorkers(ctx, r.actorLister, workers, r.namespace, r.atespace, r.selector, r.sandboxClass)
	if err != nil {
		return err
	}
	return printer.PrintWorkersTo(r.out, filtered, r.outputFmt)
}

func runGetWorkers(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()
	apiClient, err := ateclient.NewClient(ctx, kubeconfig, k8sContext, endpoint, tokenFile, traceEnabled)
	if err != nil {
		return fmt.Errorf("failed to connect to ate-api-server: %w", err)
	}
	defer apiClient.Close()

	runner := &GetWorkersRunner{
		workerLister: apiClient,
		workerGetter: apiClient,
		actorLister:  apiClient,
		names:        args,
		namespace:    getWorkerNamespaceFlag,
		atespace:     getWorkerAtespaceFlag,
		selector:     getWorkerSelectorFlag,
		sandboxClass: getWorkerClassFlag,
		outputFmt:    outputFmt,
		out:          cmd.OutOrStdout(),
	}
	return runner.Run(ctx)
}

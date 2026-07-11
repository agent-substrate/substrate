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

var bootFlag bool
var resumeAtespaceFlag string

var resumeActorCmd = &cobra.Command{
	Use:   "actor <actor-name>",
	Short: "Resume an actor",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := cmd.Context()
		apiClient, err := ateclient.NewClient(ctx, kubeconfig, k8sContext, endpoint, traceEnabled)
		if err != nil {
			return fmt.Errorf("failed to connect to ate-api-server: %w", err)
		}
		defer apiClient.Close()

		resp, err := apiClient.ResumeActor(ctx, &ateapipb.ResumeActorRequest{
			Actor: &ateapipb.ObjectRef{Atespace: resumeAtespaceFlag, Name: args[0]},
			Boot:  bootFlag,
		})
		if err != nil {
			return fmt.Errorf("failed to resume actor: %w", err)
		}

		return printer.PrintActor(resp.GetActor(), outputFmt)
	},
}

func init() {
	resumeActorCmd.Flags().BoolVarP(&bootFlag, "boot", "", false, "Skip golden snapshot and boot from scratch.")
	resumeActorCmd.Flags().StringVarP(&resumeAtespaceFlag, "atespace", "a", "", "Atespace the actor lives in")
	_ = resumeActorCmd.MarkFlagRequired("atespace")
	resumeCmd.AddCommand(resumeActorCmd)
}

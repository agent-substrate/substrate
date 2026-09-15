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
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var getEgressPolicyAtespaceFlag string

var getEgressPolicyCmd = &cobra.Command{
	Use:     "egress-policy <actor-name>",
	Aliases: []string{"egress-policies"},
	Short:   "Get the egress policy of an actor",
	Long: "Get the egress policy of an actor. An actor has at most one egress policy; " +
		"without one, all of its egress is denied.",
	// TODO(#1550): accept several actors and print a list document.
	Args: cobra.ExactArgs(1),
	RunE: runGetEgressPolicy,
}

// EgressPolicyGetter abstracts GetActorEgressPolicy RPC calls.
type EgressPolicyGetter interface {
	GetActorEgressPolicy(ctx context.Context, req *ateapipb.GetActorEgressPolicyRequest, opts ...grpc.CallOption) (*ateapipb.EgressPolicy, error)
}

// GetEgressPolicyRunner executes the get egress-policy command logic.
type GetEgressPolicyRunner struct {
	getter    EgressPolicyGetter
	actor     *ateapipb.ObjectRef
	outputFmt string
	out       io.Writer
	errOut    io.Writer
}

func (r *GetEgressPolicyRunner) Run(ctx context.Context) error {
	policy, err := r.getter.GetActorEgressPolicy(ctx, &ateapipb.GetActorEgressPolicyRequest{Actor: r.actor})
	if status.Code(err) == codes.NotFound {
		// No policy is a valid state, not a failure: the gateway denies all egress.
		fmt.Fprintf(r.errOut, "actor %q in atespace %q has no egress policy; all egress is denied\n", r.actor.GetName(), r.actor.GetAtespace())
		return nil
	}
	if err != nil {
		return fmt.Errorf("failed to get egress policy for actor %q: %w", r.actor.GetName(), err)
	}
	return printer.PrintEgressPolicyTo(r.out, r.actor.GetName(), policy, r.outputFmt)
}

func runGetEgressPolicy(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()

	apiClient, err := ateclient.NewClient(ctx, kubeconfig, k8sContext, endpoint, tokenFile, traceEnabled)
	if err != nil {
		return fmt.Errorf("failed to connect to ate-api-server: %w", err)
	}
	defer apiClient.Close()

	runner := &GetEgressPolicyRunner{
		getter:    apiClient,
		actor:     &ateapipb.ObjectRef{Atespace: getEgressPolicyAtespaceFlag, Name: args[0]},
		outputFmt: outputFmt,
		out:       cmd.OutOrStdout(),
		errOut:    cmd.ErrOrStderr(),
	}
	return runner.Run(ctx)
}

func init() {
	getEgressPolicyCmd.Flags().StringVarP(&getEgressPolicyAtespaceFlag, "atespace", "a", "", "Atespace the actor lives in (required)")
	_ = getEgressPolicyCmd.MarkFlagRequired("atespace")
	getCmd.AddCommand(getEgressPolicyCmd)
}

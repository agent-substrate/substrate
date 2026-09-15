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
)

var (
	createEgressPolicyAtespaceFlag string
	createEgressPolicyFilenameFlag string
)

var createEgressPolicyCmd = &cobra.Command{
	Use:     "egress-policy <actor-name> -f <manifest>",
	Aliases: []string{"egress-policies"},
	Short:   "Create the egress policy of an actor",
	Long: `Create the egress policy of an actor from a manifest file.

The manifest is a single YAML (or JSON) document holding one ateapipb.EgressPolicy
message in its protojson form, exactly as printed by
"kubectl ate get egress-policy <actor-name> -a <atespace> -o yaml".
The policy's atespace comes from --atespace and its actor from the argument,
so metadata may be omitted. The actor must already exist and have no policy.`,
	Args: cobra.ExactArgs(1),
	RunE: runCreateEgressPolicy,
}

// EgressPolicyCreator abstracts CreateActorEgressPolicy RPC calls.
type EgressPolicyCreator interface {
	CreateActorEgressPolicy(ctx context.Context, req *ateapipb.CreateActorEgressPolicyRequest, opts ...grpc.CallOption) (*ateapipb.EgressPolicy, error)
}

// CreateEgressPolicyRunner executes the create egress-policy command logic.
type CreateEgressPolicyRunner struct {
	creator   EgressPolicyCreator
	actor     *ateapipb.ObjectRef
	policy    *ateapipb.EgressPolicy
	outputFmt string
	out       io.Writer
}

func (r *CreateEgressPolicyRunner) Run(ctx context.Context) error {
	created, err := r.creator.CreateActorEgressPolicy(ctx, &ateapipb.CreateActorEgressPolicyRequest{Actor: r.actor, EgressPolicy: r.policy})
	if err != nil {
		return fmt.Errorf("failed to create egress policy for actor %q: %w", r.actor.GetName(), err)
	}
	return printer.PrintEgressPolicyTo(r.out, r.actor.GetName(), created, r.outputFmt)
}

func runCreateEgressPolicy(cmd *cobra.Command, args []string) error {
	data, err := readFileOrStdin(cmd.InOrStdin(), createEgressPolicyFilenameFlag)
	if err != nil {
		return err
	}
	policy, err := egressPolicyFromManifest(data, createEgressPolicyAtespaceFlag)
	if err != nil {
		return fmt.Errorf("failed to parse egress policy manifest %q: %w", createEgressPolicyFilenameFlag, err)
	}

	ctx := cmd.Context()
	apiClient, err := ateclient.NewClient(ctx, kubeconfig, k8sContext, endpoint, tokenFile, traceEnabled)
	if err != nil {
		return fmt.Errorf("failed to connect to ate-api-server: %w", err)
	}
	defer apiClient.Close()

	runner := &CreateEgressPolicyRunner{
		creator:   apiClient,
		actor:     &ateapipb.ObjectRef{Atespace: createEgressPolicyAtespaceFlag, Name: args[0]},
		policy:    policy,
		outputFmt: outputFmt,
		out:       cmd.OutOrStdout(),
	}
	return runner.Run(ctx)
}

func init() {
	createEgressPolicyCmd.Flags().StringVarP(&createEgressPolicyAtespaceFlag, "atespace", "a", "", "Atespace the actor lives in (required)")
	createEgressPolicyCmd.Flags().StringVarP(&createEgressPolicyFilenameFlag, "filename", "f", "", "Manifest file holding a single protojson-shaped EgressPolicy document; use - for stdin (required)")
	_ = createEgressPolicyCmd.MarkFlagRequired("atespace")
	_ = createEgressPolicyCmd.MarkFlagRequired("filename")
	createCmd.AddCommand(createEgressPolicyCmd)
}

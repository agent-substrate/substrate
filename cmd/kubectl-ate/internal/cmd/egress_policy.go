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
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/agent-substrate/substrate/cmd/kubectl-ate/internal/printer"
	"github.com/agent-substrate/substrate/internal/ateclient"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	k8syaml "k8s.io/apimachinery/pkg/util/yaml"
	"sigs.k8s.io/yaml"
)

var (
	getEgressPolicyAtespaceFlag    string
	createEgressPolicyAtespaceFlag string
	createEgressPolicyFilenameFlag string
)

var getEgressPolicyCmd = &cobra.Command{
	Use:     "egress-policy <actor-name>",
	Aliases: []string{"egress-policies"},
	Short:   "Get the egress policy of an actor",
	// TODO(#1550): accept several actors and print a list document.
	Args: cobra.ExactArgs(1),
	RunE: runGetEgressPolicy,
}

var createEgressPolicyCmd = &cobra.Command{
	Use:     "egress-policy <actor-name> -f <manifest>",
	Aliases: []string{"egress-policies"},
	Short:   "Create an actor's egress policy from a manifest",
	Long: `Create the egress policy of an actor from a manifest file.

The manifest is a YAML or JSON EgressPolicy, as printed by
"kubectl ate get egress-policy <actor-name> -a <atespace> -o yaml".
Its metadata may be omitted.`,
	Args: cobra.ExactArgs(1),
	RunE: runCreateEgressPolicy,
}

// egressPolicyFromManifest parses a single protojson-shaped YAML or JSON
// document into an EgressPolicy. Parsing is strict: unknown fields are an
// error.
func egressPolicyFromManifest(data []byte) (*ateapipb.EgressPolicy, error) {
	jsonData, err := manifestToJSON(data)
	if err != nil {
		return nil, err
	}
	policy := &ateapipb.EgressPolicy{}
	if err := protojson.Unmarshal(jsonData, policy); err != nil {
		return nil, fmt.Errorf("invalid EgressPolicy: %w", err)
	}
	return policy, nil
}

// manifestToJSON converts a YAML or JSON manifest to JSON. YAML is a superset
// of JSON, so one reader parses both. The manifest must hold exactly one
// non-empty document; the empty documents a leading or trailing "---" leaves
// behind are skipped.
func manifestToJSON(data []byte) ([]byte, error) {
	reader := k8syaml.NewYAMLReader(bufio.NewReader(bytes.NewReader(data)))
	var jsonData []byte
	for {
		doc, err := reader.Read()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("invalid YAML: %w", err)
		}
		j, err := yaml.YAMLToJSON(doc)
		if err != nil {
			return nil, fmt.Errorf("invalid YAML: %w", err)
		}
		if string(j) == "null" {
			continue
		}
		if jsonData != nil {
			return nil, fmt.Errorf("manifest holds more than one document, expected one")
		}
		jsonData = j
	}
	if jsonData == nil {
		return nil, fmt.Errorf("manifest is empty")
	}
	return jsonData, nil
}

// policyName is the fixed name of an actor's egress policy.
const policyName = "default"

// overrideEgressPolicyMetadata sets metadata.atespace to atespace and
// metadata.name to policyName when the manifest omits them. A manifest that
// names another atespace or name is rejected, not retargeted.
func overrideEgressPolicyMetadata(policy *ateapipb.EgressPolicy, atespace string) error {
	if policy.Metadata == nil {
		policy.Metadata = &ateapipb.ResourceMetadata{}
	}
	switch policy.Metadata.Atespace {
	case "":
		policy.Metadata.Atespace = atespace
	case atespace:
	default:
		return fmt.Errorf("manifest metadata.atespace %q does not match --atespace %q", policy.Metadata.Atespace, atespace)
	}
	switch policy.Metadata.Name {
	case "":
		policy.Metadata.Name = policyName
	case policyName:
	default:
		return fmt.Errorf("manifest metadata.name %q must be %q", policy.Metadata.Name, policyName)
	}
	return nil
}

// egressPolicyGetter abstracts GetActorEgressPolicy RPC calls.
type egressPolicyGetter interface {
	GetActorEgressPolicy(ctx context.Context, req *ateapipb.GetActorEgressPolicyRequest, opts ...grpc.CallOption) (*ateapipb.EgressPolicy, error)
}

// getEgressPolicyRunner executes the get egress-policy command logic.
type getEgressPolicyRunner struct {
	getter    egressPolicyGetter
	actor     *ateapipb.ObjectRef
	outputFmt string
	stdout    io.Writer
	stderr    io.Writer
}

func (r *getEgressPolicyRunner) Run(ctx context.Context) error {
	policy, err := r.getter.GetActorEgressPolicy(ctx, &ateapipb.GetActorEgressPolicyRequest{Actor: r.actor})
	if status.Code(err) == codes.NotFound {
		// No policy is a valid state, not a failure: the gateway denies all egress.
		// TODO(#1703): the server answers NotFound for a missing actor too, so this
		// note cannot tell a mistyped name from an actor without a policy.
		fmt.Fprintf(r.stderr, "actor %q in atespace %q has no egress policy or does not exist; an actor without a policy has all egress denied\n", r.actor.GetName(), r.actor.GetAtespace())
		return nil
	}
	if err != nil {
		return fmt.Errorf("failed to get egress policy for actor %q in atespace %q: %w", r.actor.GetName(), r.actor.GetAtespace(), err)
	}
	return printer.PrintEgressPolicyTo(r.stdout, r.actor.GetName(), policy, r.outputFmt)
}

func runGetEgressPolicy(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()

	apiClient, err := ateclient.NewClient(ctx, kubeconfig, k8sContext, endpoint, tokenFile, traceEnabled)
	if err != nil {
		return fmt.Errorf("failed to connect to ate-api-server: %w", err)
	}
	defer apiClient.Close()

	runner := &getEgressPolicyRunner{
		getter:    apiClient,
		actor:     &ateapipb.ObjectRef{Atespace: getEgressPolicyAtespaceFlag, Name: args[0]},
		outputFmt: outputFmt,
		stdout:    cmd.OutOrStdout(),
		stderr:    cmd.ErrOrStderr(),
	}
	return runner.Run(ctx)
}

// egressPolicyCreator abstracts CreateActorEgressPolicy RPC calls.
type egressPolicyCreator interface {
	CreateActorEgressPolicy(ctx context.Context, req *ateapipb.CreateActorEgressPolicyRequest, opts ...grpc.CallOption) (*ateapipb.EgressPolicy, error)
}

// createEgressPolicyRunner executes the create egress-policy command logic.
type createEgressPolicyRunner struct {
	creator   egressPolicyCreator
	actor     *ateapipb.ObjectRef
	policy    *ateapipb.EgressPolicy
	outputFmt string
	stdout    io.Writer
}

func (r *createEgressPolicyRunner) Run(ctx context.Context) error {
	created, err := r.creator.CreateActorEgressPolicy(ctx, &ateapipb.CreateActorEgressPolicyRequest{Actor: r.actor, EgressPolicy: r.policy})
	if err != nil {
		return fmt.Errorf("failed to create egress policy for actor %q in atespace %q: %w", r.actor.GetName(), r.actor.GetAtespace(), err)
	}
	return printer.PrintEgressPolicyTo(r.stdout, r.actor.GetName(), created, r.outputFmt)
}

func runCreateEgressPolicy(cmd *cobra.Command, args []string) error {
	data, err := readFileOrStdin(cmd.InOrStdin(), createEgressPolicyFilenameFlag)
	if err != nil {
		return err
	}
	policy, err := egressPolicyFromManifest(data)
	if err != nil {
		return fmt.Errorf("failed to parse egress policy manifest %q: %w", createEgressPolicyFilenameFlag, err)
	}
	if err := overrideEgressPolicyMetadata(policy, createEgressPolicyAtespaceFlag); err != nil {
		return err
	}

	ctx := cmd.Context()
	apiClient, err := ateclient.NewClient(ctx, kubeconfig, k8sContext, endpoint, tokenFile, traceEnabled)
	if err != nil {
		return fmt.Errorf("failed to connect to ate-api-server: %w", err)
	}
	defer apiClient.Close()

	runner := &createEgressPolicyRunner{
		creator:   apiClient,
		actor:     &ateapipb.ObjectRef{Atespace: createEgressPolicyAtespaceFlag, Name: args[0]},
		policy:    policy,
		outputFmt: outputFmt,
		stdout:    cmd.OutOrStdout(),
	}
	return runner.Run(ctx)
}

func init() {
	getEgressPolicyCmd.Flags().StringVarP(&getEgressPolicyAtespaceFlag, "atespace", "a", "", "Atespace the actor lives in (required)")
	_ = getEgressPolicyCmd.MarkFlagRequired("atespace")
	getCmd.AddCommand(getEgressPolicyCmd)

	createEgressPolicyCmd.Flags().StringVarP(&createEgressPolicyAtespaceFlag, "atespace", "a", "", "Atespace the actor lives in (required)")
	createEgressPolicyCmd.Flags().StringVarP(&createEgressPolicyFilenameFlag, "filename", "f", "", "Manifest file holding one EgressPolicy; use - for stdin (required)")
	_ = createEgressPolicyCmd.MarkFlagRequired("atespace")
	_ = createEgressPolicyCmd.MarkFlagRequired("filename")
	createCmd.AddCommand(createEgressPolicyCmd)
}

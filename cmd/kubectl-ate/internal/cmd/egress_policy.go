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
	yamlv3 "gopkg.in/yaml.v3"
	"sigs.k8s.io/yaml"
)

var egressPolicyFlags struct {
	atespace string
	filename string
}

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

var updateEgressPolicyCmd = &cobra.Command{
	Use:     "egress-policy <actor-name> -f <manifest>",
	Aliases: []string{"egress-policies"},
	Short:   "Replace an actor's egress policy from a manifest",
	Long: `Replace the egress policy of an actor from a manifest file.

The manifest is a YAML or JSON EgressPolicy, as printed by
"kubectl ate get egress-policy <actor-name> -a <atespace> -o yaml".
Its metadata.uid and metadata.version are required preconditions; the server
rejects the update when they no longer match those of the stored egress policy.`,
	Args: cobra.ExactArgs(1),
	RunE: runUpdateEgressPolicy,
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
// of JSON, so one decoder parses both. The manifest must hold exactly one
// document, counted the way the YAML spec counts them: an empty document, such
// as the one a trailing "---" opens, is a document too, and is rejected.
func manifestToJSON(data []byte) ([]byte, error) {
	dec := yamlv3.NewDecoder(bytes.NewReader(data))
	if err := dec.Decode(&yamlv3.Node{}); err != nil {
		if errors.Is(err, io.EOF) {
			return nil, errors.New("manifest is empty")
		}
		return nil, fmt.Errorf("invalid YAML: %w", err)
	}
	// Decode again to check that the manifest holds no second document.
	if err := dec.Decode(&yamlv3.Node{}); !errors.Is(err, io.EOF) {
		return nil, errors.New("manifest holds more than one document, expected one")
	}
	j, err := yaml.YAMLToJSON(data)
	if err != nil {
		return nil, fmt.Errorf("invalid YAML: %w", err)
	}
	if string(j) == "null" {
		return nil, errors.New("manifest is empty")
	}
	return j, nil
}

// overrideEgressPolicyMetadata defaults each of metadata.atespace and
// metadata.name the manifest leaves empty, then checks that both match the
// command line: the atespace flag and the fixed policy name.
func overrideEgressPolicyMetadata(policy *ateapipb.EgressPolicy, atespace string) error {
	const policyNameDefault = "default"
	if policy.Metadata == nil {
		policy.Metadata = &ateapipb.ResourceMetadata{}
	}
	if policy.Metadata.Atespace == "" {
		policy.Metadata.Atespace = atespace
	}
	if policy.Metadata.Name == "" {
		policy.Metadata.Name = policyNameDefault
	}
	if policy.Metadata.Atespace != atespace {
		return fmt.Errorf("manifest metadata.atespace %q does not match --atespace %q", policy.Metadata.Atespace, atespace)
	}
	if policy.Metadata.Name != policyNameDefault {
		return fmt.Errorf("manifest metadata.name %q must be %q", policy.Metadata.Name, policyNameDefault)
	}
	return nil
}

// loadEgressPolicyManifest reads a manifest from filename, or from in when
// filename is "-", and parses it.
func loadEgressPolicyManifest(in io.Reader, filename string) (*ateapipb.EgressPolicy, error) {
	data, err := readFileOrStdin(in, filename)
	if err != nil {
		return nil, err
	}
	policy, err := egressPolicyFromManifest(data)
	if err != nil {
		return nil, fmt.Errorf("failed to parse egress policy manifest %q: %w", filename, err)
	}
	return policy, nil
}

// egressPolicyGetter abstracts the RPCs get egress-policy makes: the policy
// read, and the actor read that tells a missing actor from a missing policy.
type egressPolicyGetter interface {
	GetActorEgressPolicy(ctx context.Context, req *ateapipb.GetActorEgressPolicyRequest, opts ...grpc.CallOption) (*ateapipb.EgressPolicy, error)
	GetActor(ctx context.Context, req *ateapipb.GetActorRequest, opts ...grpc.CallOption) (*ateapipb.Actor, error)
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
		// The server answers NotFound for a missing actor too, so read the actor
		// to tell the two apart.
		if _, err := r.getter.GetActor(ctx, &ateapipb.GetActorRequest{Actor: r.actor}); err != nil {
			if status.Code(err) == codes.NotFound {
				return fmt.Errorf("actor %q in atespace %q not found", r.actor.GetName(), r.actor.GetAtespace())
			}
			return fmt.Errorf("failed to get actor %q in atespace %q: %w", r.actor.GetName(), r.actor.GetAtespace(), err)
		}
		// No policy is a valid state, not a failure: the gateway denies all egress.
		fmt.Fprintf(r.stderr, "actor %q in atespace %q has no egress policy\n", r.actor.GetName(), r.actor.GetAtespace())
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
		actor:     &ateapipb.ObjectRef{Atespace: egressPolicyFlags.atespace, Name: args[0]},
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
	policy, err := loadEgressPolicyManifest(cmd.InOrStdin(), egressPolicyFlags.filename)
	if err != nil {
		return err
	}
	if err := overrideEgressPolicyMetadata(policy, egressPolicyFlags.atespace); err != nil {
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
		actor:     &ateapipb.ObjectRef{Atespace: egressPolicyFlags.atespace, Name: args[0]},
		policy:    policy,
		outputFmt: outputFmt,
		stdout:    cmd.OutOrStdout(),
	}
	return runner.Run(ctx)
}

// egressPolicyUpdater abstracts the RPCs update egress-policy makes: the policy
// write, and the actor read that tells a missing actor from a missing policy.
type egressPolicyUpdater interface {
	UpdateActorEgressPolicy(ctx context.Context, req *ateapipb.UpdateActorEgressPolicyRequest, opts ...grpc.CallOption) (*ateapipb.EgressPolicy, error)
	GetActor(ctx context.Context, req *ateapipb.GetActorRequest, opts ...grpc.CallOption) (*ateapipb.Actor, error)
}

// updateEgressPolicyRunner executes the update egress-policy command logic.
type updateEgressPolicyRunner struct {
	updater   egressPolicyUpdater
	actor     *ateapipb.ObjectRef
	policy    *ateapipb.EgressPolicy
	outputFmt string
	stdout    io.Writer
}

func (r *updateEgressPolicyRunner) Run(ctx context.Context) error {
	updated, err := r.updater.UpdateActorEgressPolicy(ctx, &ateapipb.UpdateActorEgressPolicyRequest{Actor: r.actor, EgressPolicy: r.policy})
	if status.Code(err) == codes.NotFound {
		// The server answers NotFound for a missing actor too, so read the actor
		// to tell the two apart.
		if _, err := r.updater.GetActor(ctx, &ateapipb.GetActorRequest{Actor: r.actor}); err != nil {
			if status.Code(err) == codes.NotFound {
				return fmt.Errorf("actor %q in atespace %q not found", r.actor.GetName(), r.actor.GetAtespace())
			}
			return fmt.Errorf("failed to get actor %q in atespace %q: %w", r.actor.GetName(), r.actor.GetAtespace(), err)
		}
		return fmt.Errorf(`actor %q in atespace %q has no egress policy to update; create it with "kubectl ate create egress-policy"`,
			r.actor.GetName(), r.actor.GetAtespace())
	}
	if status.Code(err) == codes.Aborted {
		return fmt.Errorf(`manifest's uid and version preconditions do not match those of the stored egress policy for actor %q in atespace %q (changed since it was read, or read from another actor's policy); re-run "get egress-policy -o yaml" and reapply the edit: %w`,
			r.actor.GetName(), r.actor.GetAtespace(), err)
	}
	if err != nil {
		return fmt.Errorf("failed to update egress policy for actor %q in atespace %q: %w", r.actor.GetName(), r.actor.GetAtespace(), err)
	}
	return printer.PrintEgressPolicyTo(r.stdout, r.actor.GetName(), updated, r.outputFmt)
}

// requireEgressPolicyPreconditions rejects a manifest missing the uid and
// version preconditions an update requires. The client can only see that they
// are present; the server decides whether they still match.
func requireEgressPolicyPreconditions(policy *ateapipb.EgressPolicy, actor *ateapipb.ObjectRef) error {
	if policy.GetMetadata().GetUid() == "" || policy.GetMetadata().GetVersion() == 0 {
		return fmt.Errorf(`manifest for actor %q in atespace %q lacks the metadata.uid and metadata.version preconditions that "get egress-policy -o yaml" prints`,
			actor.GetName(), actor.GetAtespace())
	}
	return nil
}

func runUpdateEgressPolicy(cmd *cobra.Command, args []string) error {
	policy, err := loadEgressPolicyManifest(cmd.InOrStdin(), egressPolicyFlags.filename)
	if err != nil {
		return err
	}
	if err := overrideEgressPolicyMetadata(policy, egressPolicyFlags.atespace); err != nil {
		return err
	}
	actor := &ateapipb.ObjectRef{Atespace: egressPolicyFlags.atespace, Name: args[0]}
	// Checked before dialing, so an unusable manifest costs no connection.
	if err := requireEgressPolicyPreconditions(policy, actor); err != nil {
		return err
	}

	ctx := cmd.Context()
	apiClient, err := ateclient.NewClient(ctx, kubeconfig, k8sContext, endpoint, tokenFile, traceEnabled)
	if err != nil {
		return fmt.Errorf("failed to connect to ate-api-server: %w", err)
	}
	defer apiClient.Close()

	runner := &updateEgressPolicyRunner{
		updater:   apiClient,
		actor:     actor,
		policy:    policy,
		outputFmt: outputFmt,
		stdout:    cmd.OutOrStdout(),
	}
	return runner.Run(ctx)
}

func init() {
	getEgressPolicyCmd.Flags().StringVarP(&egressPolicyFlags.atespace, "atespace", "a", "", "Atespace the actor lives in (required)")
	_ = getEgressPolicyCmd.MarkFlagRequired("atespace")
	getCmd.AddCommand(getEgressPolicyCmd)

	createEgressPolicyCmd.Flags().StringVarP(&egressPolicyFlags.atespace, "atespace", "a", "", "Atespace the actor lives in (required)")
	createEgressPolicyCmd.Flags().StringVarP(&egressPolicyFlags.filename, "filename", "f", "", "Manifest file holding one EgressPolicy; use - for stdin (required)")
	_ = createEgressPolicyCmd.MarkFlagRequired("atespace")
	_ = createEgressPolicyCmd.MarkFlagRequired("filename")
	createCmd.AddCommand(createEgressPolicyCmd)

	updateEgressPolicyCmd.Flags().StringVarP(&egressPolicyFlags.atespace, "atespace", "a", "", "Atespace the actor lives in (required)")
	updateEgressPolicyCmd.Flags().StringVarP(&egressPolicyFlags.filename, "filename", "f", "", "Manifest file holding one EgressPolicy; use - for stdin (required)")
	_ = updateEgressPolicyCmd.MarkFlagRequired("atespace")
	_ = updateEgressPolicyCmd.MarkFlagRequired("filename")
	updateCmd.AddCommand(updateEgressPolicyCmd)
}

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
	"errors"
	"fmt"
	"io"

	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"

	"go.ate.dev/substrate/cmd/kubectl-ate/internal/printer"
	"go.ate.dev/substrate/internal/resources"
	"go.ate.dev/substrate/pkg/proto/ateapipb"
)

var accessPolicyFlags struct {
	atespace string
	global   bool
	filename string
	uid      string
	version  int64
}

const accessPolicyManifestHelp = `The manifest is a YAML or JSON AccessPolicy, as printed by
"kubectl ate get access-policy -o yaml". Its metadata may be omitted. Members
are principals with a "user:" prefix, e.g.
"user:system:serviceaccount:team-a:alice".

  bindings:
  - role: owner
    members:
    - user:system:serviceaccount:team-a:alice`

var getAccessPolicyCmd = &cobra.Command{
	Use:     "access-policy (-a <atespace> | --global)",
	Aliases: []string{"access-policies"},
	Short:   "Get the access policy of an atespace or the global access policy",
	Args:    cobra.NoArgs,
	RunE:    runGetAccessPolicy,
}

var createAccessPolicyCmd = &cobra.Command{
	Use:     "access-policy (-a <atespace> | --global) -f <manifest>",
	Aliases: []string{"access-policies"},
	Short:   "Create an access policy from a manifest",
	Long:    "Create the access policy of an atespace, or the global access policy.\n\n" + accessPolicyManifestHelp,
	Args:    cobra.NoArgs,
	RunE:    runCreateAccessPolicy,
}

var updateAccessPolicyCmd = &cobra.Command{
	Use:     "access-policy (-a <atespace> | --global) -f <manifest>",
	Aliases: []string{"access-policies"},
	Short:   "Replace an access policy from a manifest",
	Long: "Replace the access policy of an atespace, or the global access policy.\n\n" + accessPolicyManifestHelp + `

Its metadata.uid and metadata.version are required preconditions; the server
rejects the update when they no longer match those of the stored policy.`,
	Args: cobra.NoArgs,
	RunE: runUpdateAccessPolicy,
}

var deleteAccessPolicyCmd = &cobra.Command{
	Use:     "access-policy -a <atespace>",
	Aliases: []string{"access-policies"},
	Short:   "Delete the access policy of an atespace",
	Long: `Delete the access policy of an atespace. The global access policy cannot be
deleted; update it with no bindings instead.`,
	Args: cobra.NoArgs,
	RunE: runDeleteAccessPolicy,
}

// accessPolicyScope is the parent of an access policy: one atespace, or the
// global singleton when atespace is empty.
type accessPolicyScope struct {
	atespace string
}

func (s accessPolicyScope) global() bool { return s.atespace == "" }

func (s accessPolicyScope) String() string {
	if s.global() {
		return "global"
	}
	return fmt.Sprintf("atespace %q", s.atespace)
}

func (s accessPolicyScope) ref() *ateapipb.ObjectRef {
	return &ateapipb.ObjectRef{Name: s.atespace}
}

// scopeFromFlags requires exactly one of --atespace and --global.
func scopeFromFlags(atespace string, global bool) (accessPolicyScope, error) {
	switch {
	case atespace != "" && global:
		return accessPolicyScope{}, errors.New("--atespace and --global are mutually exclusive")
	case atespace == "" && !global:
		return accessPolicyScope{}, errors.New("one of --atespace or --global is required")
	}
	return accessPolicyScope{atespace: atespace}, nil
}

// accessPolicyFromManifest parses a single protojson-shaped YAML or JSON
// document into an AccessPolicy. Parsing is strict: unknown fields are an
// error.
func accessPolicyFromManifest(data []byte) (*ateapipb.AccessPolicy, error) {
	jsonData, err := manifestToJSON(data)
	if err != nil {
		return nil, err
	}
	policy := &ateapipb.AccessPolicy{}
	if err := protojson.Unmarshal(jsonData, policy); err != nil {
		return nil, fmt.Errorf("invalid AccessPolicy: %w", err)
	}
	return policy, nil
}

// loadAccessPolicyManifest reads a manifest from filename, or from in when
// filename is "-", parses it, and checks its metadata. An access policy's
// metadata.atespace is always empty, whatever its scope, and its name is
// always "default"; omitted metadata is filled in.
func loadAccessPolicyManifest(in io.Reader, filename string) (*ateapipb.AccessPolicy, error) {
	data, err := readFileOrStdin(in, filename)
	if err != nil {
		return nil, err
	}
	policy, err := accessPolicyFromManifest(data)
	if err != nil {
		return nil, fmt.Errorf("failed to parse access policy manifest %q: %w", filename, err)
	}
	if policy.Metadata == nil {
		policy.Metadata = &ateapipb.ResourceMetadata{}
	}
	if policy.Metadata.Name == "" {
		policy.Metadata.Name = resources.SingletonName
	}
	if policy.Metadata.Name != resources.SingletonName {
		return nil, fmt.Errorf("manifest metadata.name %q must be %q", policy.Metadata.Name, resources.SingletonName)
	}
	if policy.Metadata.Atespace != "" {
		return nil, fmt.Errorf("manifest metadata.atespace must be empty, got %q; choose the atespace with --atespace", policy.Metadata.Atespace)
	}
	return policy, nil
}

// atespaceGetter abstracts the atespace read that tells a missing atespace
// from a missing access policy.
type atespaceGetter interface {
	GetAtespace(ctx context.Context, req *ateapipb.GetAtespaceRequest, opts ...grpc.CallOption) (*ateapipb.Atespace, error)
}

// requireScope settles a NotFound from an access policy RPC. For an atespace
// the server answers NotFound for a missing atespace and for an atespace
// without a policy alike, so it reads the atespace. It returns nil when the
// scope exists, which means the policy is what is missing.
func requireScope(ctx context.Context, getter atespaceGetter, scope accessPolicyScope) error {
	if scope.global() {
		return nil
	}
	if _, err := getter.GetAtespace(ctx, &ateapipb.GetAtespaceRequest{Atespace: scope.ref()}); err != nil {
		if status.Code(err) == codes.NotFound {
			return fmt.Errorf("atespace %q not found", scope.atespace)
		}
		return fmt.Errorf("failed to get atespace %q: %w", scope.atespace, err)
	}
	return nil
}

// accessPolicyClient is the subset of the Control API the access policy
// commands call.
type accessPolicyClient interface {
	GetGlobalAccessPolicy(ctx context.Context, req *ateapipb.GetGlobalAccessPolicyRequest, opts ...grpc.CallOption) (*ateapipb.AccessPolicy, error)
	CreateGlobalAccessPolicy(ctx context.Context, req *ateapipb.CreateGlobalAccessPolicyRequest, opts ...grpc.CallOption) (*ateapipb.AccessPolicy, error)
	UpdateGlobalAccessPolicy(ctx context.Context, req *ateapipb.UpdateGlobalAccessPolicyRequest, opts ...grpc.CallOption) (*ateapipb.AccessPolicy, error)
	GetAtespaceAccessPolicy(ctx context.Context, req *ateapipb.GetAtespaceAccessPolicyRequest, opts ...grpc.CallOption) (*ateapipb.AccessPolicy, error)
	CreateAtespaceAccessPolicy(ctx context.Context, req *ateapipb.CreateAtespaceAccessPolicyRequest, opts ...grpc.CallOption) (*ateapipb.AccessPolicy, error)
	UpdateAtespaceAccessPolicy(ctx context.Context, req *ateapipb.UpdateAtespaceAccessPolicyRequest, opts ...grpc.CallOption) (*ateapipb.AccessPolicy, error)
	DeleteAtespaceAccessPolicy(ctx context.Context, req *ateapipb.DeleteAtespaceAccessPolicyRequest, opts ...grpc.CallOption) (*ateapipb.AccessPolicy, error)
	atespaceGetter
}

// accessPolicyRunner executes the access policy commands.
type accessPolicyRunner struct {
	client    accessPolicyClient
	scope     accessPolicyScope
	outputFmt string
	stdout    io.Writer
	stderr    io.Writer
}

func (r *accessPolicyRunner) get(ctx context.Context) error {
	var policy *ateapipb.AccessPolicy
	var err error
	if r.scope.global() {
		policy, err = r.client.GetGlobalAccessPolicy(ctx, &ateapipb.GetGlobalAccessPolicyRequest{Name: resources.SingletonName})
	} else {
		policy, err = r.client.GetAtespaceAccessPolicy(ctx, &ateapipb.GetAtespaceAccessPolicyRequest{Atespace: r.scope.ref(), Name: resources.SingletonName})
	}
	if status.Code(err) == codes.NotFound {
		if err := requireScope(ctx, r.client, r.scope); err != nil {
			return err
		}
		// No policy is a valid state, not a failure: it grants nothing.
		fmt.Fprintf(r.stderr, "%s has no access policy\n", r.scope)
		return nil
	}
	if err != nil {
		return fmt.Errorf("failed to get %s access policy: %w", r.scope, err)
	}
	return printer.PrintAccessPolicyTo(r.stdout, r.scope.atespace, policy, r.outputFmt)
}

func (r *accessPolicyRunner) create(ctx context.Context, policy *ateapipb.AccessPolicy) error {
	var created *ateapipb.AccessPolicy
	var err error
	if r.scope.global() {
		created, err = r.client.CreateGlobalAccessPolicy(ctx, &ateapipb.CreateGlobalAccessPolicyRequest{AccessPolicy: policy})
	} else {
		created, err = r.client.CreateAtespaceAccessPolicy(ctx, &ateapipb.CreateAtespaceAccessPolicyRequest{Atespace: r.scope.ref(), AccessPolicy: policy})
	}
	if status.Code(err) == codes.AlreadyExists {
		return fmt.Errorf(`%s already has an access policy; change it with "kubectl ate update access-policy": %w`, r.scope, err)
	}
	if err != nil {
		return fmt.Errorf("failed to create %s access policy: %w", r.scope, err)
	}
	return printer.PrintAccessPolicyTo(r.stdout, r.scope.atespace, created, r.outputFmt)
}

func (r *accessPolicyRunner) update(ctx context.Context, policy *ateapipb.AccessPolicy) error {
	var updated *ateapipb.AccessPolicy
	var err error
	if r.scope.global() {
		updated, err = r.client.UpdateGlobalAccessPolicy(ctx, &ateapipb.UpdateGlobalAccessPolicyRequest{AccessPolicy: policy})
	} else {
		updated, err = r.client.UpdateAtespaceAccessPolicy(ctx, &ateapipb.UpdateAtespaceAccessPolicyRequest{Atespace: r.scope.ref(), AccessPolicy: policy})
	}
	if status.Code(err) == codes.NotFound {
		if err := requireScope(ctx, r.client, r.scope); err != nil {
			return err
		}
		return fmt.Errorf(`%s has no access policy to update; create it with "kubectl ate create access-policy"`, r.scope)
	}
	if status.Code(err) == codes.Aborted {
		return fmt.Errorf(`manifest's uid and version preconditions do not match those of the stored %s access policy (changed since it was read, or read from another scope); re-run "get access-policy -o yaml" and reapply the edit: %w`, r.scope, err)
	}
	if err != nil {
		return fmt.Errorf("failed to update %s access policy: %w", r.scope, err)
	}
	return printer.PrintAccessPolicyTo(r.stdout, r.scope.atespace, updated, r.outputFmt)
}

func (r *accessPolicyRunner) delete(ctx context.Context, options *ateapipb.DeleteOptions) error {
	_, err := r.client.DeleteAtespaceAccessPolicy(ctx, &ateapipb.DeleteAtespaceAccessPolicyRequest{Atespace: r.scope.ref(), Name: resources.SingletonName, Options: options})
	if status.Code(err) == codes.NotFound {
		if err := requireScope(ctx, r.client, r.scope); err != nil {
			return err
		}
		return fmt.Errorf("%s has no access policy", r.scope)
	}
	if err != nil {
		return fmt.Errorf("failed to delete %s access policy: %w", r.scope, err)
	}
	_, err = fmt.Fprintf(r.stdout, "access policy for %s deleted\n", r.scope)
	return err
}

// newAccessPolicyRunner connects to ate-api-server. The returned close func
// releases the connection.
func newAccessPolicyRunner(cmd *cobra.Command, scope accessPolicyScope) (*accessPolicyRunner, func(), error) {
	apiClient, err := newAPIClient(cmd)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to connect to ate-api-server: %w", err)
	}
	return &accessPolicyRunner{
		client:    apiClient,
		scope:     scope,
		outputFmt: outputFmt,
		stdout:    cmd.OutOrStdout(),
		stderr:    cmd.ErrOrStderr(),
	}, apiClient.Close, nil
}

func runGetAccessPolicy(cmd *cobra.Command, _ []string) error {
	scope, err := scopeFromFlags(accessPolicyFlags.atespace, accessPolicyFlags.global)
	if err != nil {
		return err
	}
	runner, closeFn, err := newAccessPolicyRunner(cmd, scope)
	if err != nil {
		return err
	}
	defer closeFn()
	return runner.get(cmd.Context())
}

func runCreateAccessPolicy(cmd *cobra.Command, _ []string) error {
	scope, err := scopeFromFlags(accessPolicyFlags.atespace, accessPolicyFlags.global)
	if err != nil {
		return err
	}
	policy, err := loadAccessPolicyManifest(cmd.InOrStdin(), accessPolicyFlags.filename)
	if err != nil {
		return err
	}
	runner, closeFn, err := newAccessPolicyRunner(cmd, scope)
	if err != nil {
		return err
	}
	defer closeFn()
	return runner.create(cmd.Context(), policy)
}

func runUpdateAccessPolicy(cmd *cobra.Command, _ []string) error {
	scope, err := scopeFromFlags(accessPolicyFlags.atespace, accessPolicyFlags.global)
	if err != nil {
		return err
	}
	policy, err := loadAccessPolicyManifest(cmd.InOrStdin(), accessPolicyFlags.filename)
	if err != nil {
		return err
	}
	// Checked before dialing, so an unusable manifest costs no connection.
	if policy.GetMetadata().GetUid() == "" || policy.GetMetadata().GetVersion() == 0 {
		return fmt.Errorf(`manifest for the %s access policy lacks the metadata.uid and metadata.version preconditions that "get access-policy -o yaml" prints`, scope)
	}
	runner, closeFn, err := newAccessPolicyRunner(cmd, scope)
	if err != nil {
		return err
	}
	defer closeFn()
	return runner.update(cmd.Context(), policy)
}

func runDeleteAccessPolicy(cmd *cobra.Command, _ []string) error {
	options, err := deleteOptionsFromFlags(deleteGuardFlags{
		uidSet:     cmd.Flags().Changed("uid"),
		uid:        accessPolicyFlags.uid,
		versionSet: cmd.Flags().Changed("version"),
		version:    accessPolicyFlags.version,
	})
	if err != nil {
		return err
	}
	runner, closeFn, err := newAccessPolicyRunner(cmd, accessPolicyScope{atespace: accessPolicyFlags.atespace})
	if err != nil {
		return err
	}
	defer closeFn()
	return runner.delete(cmd.Context(), options)
}

func init() {
	for _, c := range []*cobra.Command{getAccessPolicyCmd, createAccessPolicyCmd, updateAccessPolicyCmd} {
		c.Flags().StringVarP(&accessPolicyFlags.atespace, "atespace", "a", "", "Atespace whose access policy to use")
		c.Flags().BoolVar(&accessPolicyFlags.global, "global", false, "Use the global access policy instead of an atespace's")
	}
	for _, c := range []*cobra.Command{createAccessPolicyCmd, updateAccessPolicyCmd} {
		c.Flags().StringVarP(&accessPolicyFlags.filename, "filename", "f", "", "Manifest file holding one AccessPolicy; use - for stdin (required)")
		_ = c.MarkFlagRequired("filename")
	}
	getCmd.AddCommand(getAccessPolicyCmd)
	createCmd.AddCommand(createAccessPolicyCmd)
	updateCmd.AddCommand(updateAccessPolicyCmd)

	deleteAccessPolicyCmd.Flags().StringVarP(&accessPolicyFlags.atespace, "atespace", "a", "", "Atespace whose access policy to delete (required)")
	_ = deleteAccessPolicyCmd.MarkFlagRequired("atespace")
	deleteAccessPolicyCmd.Flags().StringVar(&accessPolicyFlags.uid, "uid", "", "Delete only if the policy's metadata.uid matches; from get -o yaml")
	deleteAccessPolicyCmd.Flags().Int64Var(&accessPolicyFlags.version, "version", 0, "Delete only if the policy's metadata.version matches; from get -o yaml")
	deleteCmd.AddCommand(deleteAccessPolicyCmd)
}

// Copyright 2026 The Agent Substrate Authors
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
	"slices"

	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/cmd/kubectl-ate/internal/printer"
	"go.ate.dev/substrate/internal/resources"
)

const (
	externalVolumeClassAllowedAtespaceRole = "allowed_atespace"
	maxMemberUpdateAttempts                = 5
)

var accessPolicyFlags struct {
	atespace            string
	externalVolumeClass string
	global              bool
	filename            string
	addMembers          []string
	removeMembers       []string
	role                string
	uid                 string
	version             int64
}

const accessPolicyManifestHelp = `The manifest is a YAML or JSON AccessPolicy, as printed by
"kubectl ate get access-policy -o yaml". Its metadata may be omitted. For
--global and --atespace, members are principals with a "user:" prefix (e.g.
"user:system:serviceaccount:team-a:alice"); for --external-volume-class, role
is "allowed_atespace" and members have an "atespace:" prefix (e.g.
"atespace:team-a").

  bindings:
  - role: owner
    members:
    - user:system:serviceaccount:team-a:alice`

var getAccessPolicyCmd = &cobra.Command{
	Use:     "access-policy (-a <atespace> | -c <class> | --global)",
	Aliases: []string{"access-policies"},
	Short:   "Get the access policy of an atespace, an external volume class, or the global access policy",
	Args:    cobra.NoArgs,
	RunE:    runGetAccessPolicy,
}

var createAccessPolicyCmd = &cobra.Command{
	Use:     "access-policy (-a <atespace> | -c <class> | --global) -f <manifest>",
	Aliases: []string{"access-policies"},
	Short:   "Create an access policy from a manifest",
	Long:    "Create the access policy of an atespace, an external volume class, or the global access policy.\n\n" + accessPolicyManifestHelp,
	Args:    cobra.NoArgs,
	RunE:    runCreateAccessPolicy,
}

var updateAccessPolicyCmd = &cobra.Command{
	Use:     "access-policy (-a <atespace> | -c <class> | --global) (-f <manifest> | --add-member <member> | --remove-member <member>)",
	Aliases: []string{"access-policies"},
	Short:   "Replace an access policy from a manifest, or add/remove members",
	Long: "Replace the access policy of an atespace, an external volume class, or the global access policy from a manifest, or add and remove members in place.\n\n" + accessPolicyManifestHelp + `

When replacing from -f, metadata.uid and metadata.version are required
preconditions; the server rejects the update when they no longer match those of
the stored policy. When using --add-member or --remove-member, the CLI reads
the stored policy (or creates one if adding members when none exists) and
retries automatically on version conflicts.`,
	Args: cobra.NoArgs,
	RunE: runUpdateAccessPolicy,
}

var deleteAccessPolicyCmd = &cobra.Command{
	Use:     "access-policy (-a <atespace> | -c <class>)",
	Aliases: []string{"access-policies"},
	Short:   "Delete the access policy of an atespace or an external volume class",
	Long: `Delete the access policy of an atespace or an external volume class. The global
access policy cannot be deleted; update it with no bindings instead.`,
	Args: cobra.NoArgs,
	RunE: runDeleteAccessPolicy,
}

// accessPolicyScope is the parent of an access policy: one atespace, one
// external volume class, or the global singleton when both are empty.
type accessPolicyScope struct {
	atespace            string
	externalVolumeClass string
}

func (s accessPolicyScope) global() bool {
	return s.atespace == "" && s.externalVolumeClass == ""
}

func (s accessPolicyScope) isExternalVolumeClass() bool {
	return s.externalVolumeClass != ""
}

func (s accessPolicyScope) tableLabel() string {
	switch {
	case s.externalVolumeClass != "":
		return s.externalVolumeClass
	case s.atespace != "":
		return s.atespace
	default:
		return ""
	}
}

func (s accessPolicyScope) String() string {
	switch {
	case s.externalVolumeClass != "":
		return fmt.Sprintf("external volume class %q", s.externalVolumeClass)
	case s.atespace != "":
		return fmt.Sprintf("atespace %q", s.atespace)
	default:
		return "global"
	}
}

func (s accessPolicyScope) ref() *ateapi.ObjectRef {
	if s.externalVolumeClass != "" {
		return &ateapi.ObjectRef{Name: s.externalVolumeClass}
	}
	return &ateapi.ObjectRef{Name: s.atespace}
}

// scopeFromFlags requires exactly one of --atespace, --external-volume-class,
// and --global.
func scopeFromFlags(atespace, externalVolumeClass string, global bool) (accessPolicyScope, error) {
	set := 0
	if atespace != "" {
		set++
	}
	if externalVolumeClass != "" {
		set++
	}
	if global {
		set++
	}
	switch {
	case set > 1:
		return accessPolicyScope{}, errors.New("--atespace, --external-volume-class, and --global are mutually exclusive")
	case set == 0:
		return accessPolicyScope{}, errors.New("one of --atespace, --external-volume-class, or --global is required")
	}
	return accessPolicyScope{atespace: atespace, externalVolumeClass: externalVolumeClass}, nil
}

// deleteScopeFromFlags requires exactly one of --atespace and
// --external-volume-class.
func deleteScopeFromFlags(atespace, externalVolumeClass string) (accessPolicyScope, error) {
	switch {
	case atespace != "" && externalVolumeClass != "":
		return accessPolicyScope{}, errors.New("--atespace and --external-volume-class are mutually exclusive")
	case atespace == "" && externalVolumeClass == "":
		return accessPolicyScope{}, errors.New("one of --atespace or --external-volume-class is required")
	}
	return accessPolicyScope{atespace: atespace, externalVolumeClass: externalVolumeClass}, nil
}

// accessPolicyFromManifest parses a single protojson-shaped YAML or JSON
// document into an AccessPolicy. Parsing is strict: unknown fields are an
// error.
func accessPolicyFromManifest(data []byte) (*ateapi.AccessPolicy, error) {
	jsonData, err := manifestToJSON(data)
	if err != nil {
		return nil, err
	}
	policy := &ateapi.AccessPolicy{}
	if err := protojson.Unmarshal(jsonData, policy); err != nil {
		return nil, fmt.Errorf("invalid AccessPolicy: %w", err)
	}
	return policy, nil
}

// loadAccessPolicyManifest reads a manifest from filename, or from in when
// filename is "-", parses it, and checks its metadata. An access policy's
// metadata.atespace is always empty, whatever its scope, and its name is
// always "default"; omitted metadata is filled in.
func loadAccessPolicyManifest(in io.Reader, filename string) (*ateapi.AccessPolicy, error) {
	data, err := readFileOrStdin(in, filename)
	if err != nil {
		return nil, err
	}
	policy, err := accessPolicyFromManifest(data)
	if err != nil {
		return nil, fmt.Errorf("failed to parse access policy manifest %q: %w", filename, err)
	}
	if policy.Metadata == nil {
		policy.Metadata = &ateapi.ResourceMetadata{}
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

// scopeGetter abstracts the parent-resource reads that tell a missing parent
// from a missing access policy.
type scopeGetter interface {
	GetAtespace(ctx context.Context, req *ateapi.GetAtespaceRequest, opts ...grpc.CallOption) (*ateapi.Atespace, error)
	GetExternalVolumeClass(ctx context.Context, req *ateapi.GetExternalVolumeClassRequest, opts ...grpc.CallOption) (*ateapi.ExternalVolumeClass, error)
}

// requireScope settles a NotFound from an access policy RPC. For an atespace
// or external volume class the server answers NotFound for a missing parent
// and for a parent without a policy alike, so it reads the parent. It returns
// nil when the scope exists, which means the policy is what is missing.
func requireScope(ctx context.Context, getter scopeGetter, scope accessPolicyScope) error {
	switch {
	case scope.global():
		return nil
	case scope.isExternalVolumeClass():
		if _, err := getter.GetExternalVolumeClass(ctx, &ateapi.GetExternalVolumeClassRequest{ExternalVolumeClass: scope.ref()}); err != nil {
			if status.Code(err) == codes.NotFound {
				return fmt.Errorf("external volume class %q not found", scope.externalVolumeClass)
			}
			return fmt.Errorf("failed to get external volume class %q: %w", scope.externalVolumeClass, err)
		}
		return nil
	default:
		if _, err := getter.GetAtespace(ctx, &ateapi.GetAtespaceRequest{Atespace: scope.ref()}); err != nil {
			if status.Code(err) == codes.NotFound {
				return fmt.Errorf("atespace %q not found", scope.atespace)
			}
			return fmt.Errorf("failed to get atespace %q: %w", scope.atespace, err)
		}
		return nil
	}
}

// accessPolicyClient is the subset of the Control API the access policy
// commands call.
type accessPolicyClient interface {
	GetGlobalAccessPolicy(ctx context.Context, req *ateapi.GetGlobalAccessPolicyRequest, opts ...grpc.CallOption) (*ateapi.AccessPolicy, error)
	CreateGlobalAccessPolicy(ctx context.Context, req *ateapi.CreateGlobalAccessPolicyRequest, opts ...grpc.CallOption) (*ateapi.AccessPolicy, error)
	UpdateGlobalAccessPolicy(ctx context.Context, req *ateapi.UpdateGlobalAccessPolicyRequest, opts ...grpc.CallOption) (*ateapi.AccessPolicy, error)
	GetAtespaceAccessPolicy(ctx context.Context, req *ateapi.GetAtespaceAccessPolicyRequest, opts ...grpc.CallOption) (*ateapi.AccessPolicy, error)
	CreateAtespaceAccessPolicy(ctx context.Context, req *ateapi.CreateAtespaceAccessPolicyRequest, opts ...grpc.CallOption) (*ateapi.AccessPolicy, error)
	UpdateAtespaceAccessPolicy(ctx context.Context, req *ateapi.UpdateAtespaceAccessPolicyRequest, opts ...grpc.CallOption) (*ateapi.AccessPolicy, error)
	DeleteAtespaceAccessPolicy(ctx context.Context, req *ateapi.DeleteAtespaceAccessPolicyRequest, opts ...grpc.CallOption) (*ateapi.AccessPolicy, error)
	GetExternalVolumeClassAccessPolicy(ctx context.Context, req *ateapi.GetExternalVolumeClassAccessPolicyRequest, opts ...grpc.CallOption) (*ateapi.AccessPolicy, error)
	CreateExternalVolumeClassAccessPolicy(ctx context.Context, req *ateapi.CreateExternalVolumeClassAccessPolicyRequest, opts ...grpc.CallOption) (*ateapi.AccessPolicy, error)
	UpdateExternalVolumeClassAccessPolicy(ctx context.Context, req *ateapi.UpdateExternalVolumeClassAccessPolicyRequest, opts ...grpc.CallOption) (*ateapi.AccessPolicy, error)
	DeleteExternalVolumeClassAccessPolicy(ctx context.Context, req *ateapi.DeleteExternalVolumeClassAccessPolicyRequest, opts ...grpc.CallOption) (*ateapi.AccessPolicy, error)
	scopeGetter
}

// accessPolicyRunner executes the access policy commands.
type accessPolicyRunner struct {
	client    accessPolicyClient
	scope     accessPolicyScope
	outputFmt string
	stdout    io.Writer
	stderr    io.Writer
}

func (r *accessPolicyRunner) fetch(ctx context.Context) (*ateapi.AccessPolicy, error) {
	switch {
	case r.scope.global():
		return r.client.GetGlobalAccessPolicy(ctx, &ateapi.GetGlobalAccessPolicyRequest{Name: resources.SingletonName})
	case r.scope.isExternalVolumeClass():
		return r.client.GetExternalVolumeClassAccessPolicy(ctx, &ateapi.GetExternalVolumeClassAccessPolicyRequest{ExternalVolumeClass: r.scope.ref(), Name: resources.SingletonName})
	default:
		return r.client.GetAtespaceAccessPolicy(ctx, &ateapi.GetAtespaceAccessPolicyRequest{Atespace: r.scope.ref(), Name: resources.SingletonName})
	}
}

func (r *accessPolicyRunner) sendCreate(ctx context.Context, policy *ateapi.AccessPolicy) (*ateapi.AccessPolicy, error) {
	switch {
	case r.scope.global():
		return r.client.CreateGlobalAccessPolicy(ctx, &ateapi.CreateGlobalAccessPolicyRequest{AccessPolicy: policy})
	case r.scope.isExternalVolumeClass():
		return r.client.CreateExternalVolumeClassAccessPolicy(ctx, &ateapi.CreateExternalVolumeClassAccessPolicyRequest{ExternalVolumeClass: r.scope.ref(), AccessPolicy: policy})
	default:
		return r.client.CreateAtespaceAccessPolicy(ctx, &ateapi.CreateAtespaceAccessPolicyRequest{Atespace: r.scope.ref(), AccessPolicy: policy})
	}
}

func (r *accessPolicyRunner) sendUpdate(ctx context.Context, policy *ateapi.AccessPolicy) (*ateapi.AccessPolicy, error) {
	switch {
	case r.scope.global():
		return r.client.UpdateGlobalAccessPolicy(ctx, &ateapi.UpdateGlobalAccessPolicyRequest{AccessPolicy: policy})
	case r.scope.isExternalVolumeClass():
		return r.client.UpdateExternalVolumeClassAccessPolicy(ctx, &ateapi.UpdateExternalVolumeClassAccessPolicyRequest{ExternalVolumeClass: r.scope.ref(), AccessPolicy: policy})
	default:
		return r.client.UpdateAtespaceAccessPolicy(ctx, &ateapi.UpdateAtespaceAccessPolicyRequest{Atespace: r.scope.ref(), AccessPolicy: policy})
	}
}

func (r *accessPolicyRunner) get(ctx context.Context) error {
	policy, err := r.fetch(ctx)
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
	return printer.PrintAccessPolicyTo(r.stdout, r.scope.tableLabel(), policy, r.outputFmt)
}

func (r *accessPolicyRunner) create(ctx context.Context, policy *ateapi.AccessPolicy) error {
	created, err := r.sendCreate(ctx, policy)
	if status.Code(err) == codes.AlreadyExists {
		return fmt.Errorf(`%s already has an access policy; change it with "kubectl ate update access-policy": %w`, r.scope, err)
	}
	if err != nil {
		return fmt.Errorf("failed to create %s access policy: %w", r.scope, err)
	}
	return printer.PrintAccessPolicyTo(r.stdout, r.scope.tableLabel(), created, r.outputFmt)
}

func (r *accessPolicyRunner) update(ctx context.Context, policy *ateapi.AccessPolicy) error {
	updated, err := r.sendUpdate(ctx, policy)
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
	return printer.PrintAccessPolicyTo(r.stdout, r.scope.tableLabel(), updated, r.outputFmt)
}

// mutateMembers reads the stored policy (or creates one when adding members if
// none exists), applies addMembers/removeMembers, and writes it back, retrying
// on concurrent version conflicts.
func (r *accessPolicyRunner) mutateMembers(ctx context.Context, role string, addMembers, removeMembers []string) error {
	targetRole, err := resolveMemberEditRole(r.scope, role, addMembers, removeMembers)
	if err != nil {
		return err
	}

	var lastErr error
	for range maxMemberUpdateAttempts {
		current, err := r.fetch(ctx)
		if status.Code(err) == codes.NotFound {
			if len(addMembers) == 0 {
				if err := requireScope(ctx, r.client, r.scope); err != nil {
					return err
				}
				return fmt.Errorf(`%s has no access policy to update; create it with "kubectl ate create access-policy"`, r.scope)
			}
			initial := &ateapi.AccessPolicy{Metadata: &ateapi.ResourceMetadata{Name: resources.SingletonName}}
			applyMemberEdits(initial, targetRole, addMembers, nil)
			created, createErr := r.sendCreate(ctx, initial)
			if status.Code(createErr) == codes.AlreadyExists {
				lastErr = createErr
				continue
			}
			if createErr != nil {
				return fmt.Errorf("failed to create %s access policy: %w", r.scope, createErr)
			}
			return printer.PrintAccessPolicyTo(r.stdout, r.scope.tableLabel(), created, r.outputFmt)
		}
		if err != nil {
			return fmt.Errorf("failed to get %s access policy: %w", r.scope, err)
		}

		edited := proto.CloneOf(current)
		applyMemberEdits(edited, targetRole, addMembers, removeMembers)
		updated, updateErr := r.sendUpdate(ctx, edited)
		if status.Code(updateErr) == codes.Aborted || status.Code(updateErr) == codes.NotFound {
			lastErr = updateErr
			continue
		}
		if updateErr != nil {
			return fmt.Errorf("failed to update %s access policy: %w", r.scope, updateErr)
		}
		return printer.PrintAccessPolicyTo(r.stdout, r.scope.tableLabel(), updated, r.outputFmt)
	}
	return fmt.Errorf("failed to update %s access policy after %d attempts due to concurrent modifications: %w", r.scope, maxMemberUpdateAttempts, lastErr)
}

func resolveMemberEditRole(scope accessPolicyScope, role string, addMembers, removeMembers []string) (string, error) {
	for _, m := range addMembers {
		if slices.Contains(removeMembers, m) {
			return "", fmt.Errorf("member %q cannot be passed to both --add-member and --remove-member", m)
		}
	}
	if scope.isExternalVolumeClass() {
		if role != "" && role != externalVolumeClassAllowedAtespaceRole {
			return "", fmt.Errorf("external volume class access policies only support role %q, got %q", externalVolumeClassAllowedAtespaceRole, role)
		}
		return externalVolumeClassAllowedAtespaceRole, nil
	}
	if len(addMembers) > 0 && role == "" {
		return "", errors.New("--role is required with --add-member for --atespace and --global")
	}
	return role, nil
}

// applyMemberEdits removes removeMembers (from bindings matching role, or from
// all bindings when role is empty), drops any binding whose member list
// becomes empty, and appends addMembers to the binding for role.
func applyMemberEdits(policy *ateapi.AccessPolicy, role string, addMembers, removeMembers []string) {
	if len(removeMembers) > 0 {
		var keptBindings []*ateapi.Binding
		for _, b := range policy.GetBindings() {
			if role != "" && b.GetRole() != role {
				keptBindings = append(keptBindings, b)
				continue
			}
			keptMembers := slices.DeleteFunc(slices.Clone(b.GetMembers()), func(m string) bool {
				return slices.Contains(removeMembers, m)
			})
			if len(keptMembers) > 0 {
				b.Members = keptMembers
				keptBindings = append(keptBindings, b)
			}
		}
		policy.Bindings = keptBindings
	}
	if len(addMembers) == 0 {
		return
	}
	var target *ateapi.Binding
	for _, b := range policy.GetBindings() {
		if b.GetRole() == role {
			target = b
			break
		}
	}
	if target == nil {
		target = &ateapi.Binding{Role: role}
		policy.Bindings = append(policy.Bindings, target)
	}
	for _, m := range addMembers {
		if !slices.Contains(target.Members, m) {
			target.Members = append(target.Members, m)
		}
	}
}

func (r *accessPolicyRunner) delete(ctx context.Context, options *ateapi.DeleteOptions) error {
	var err error
	if r.scope.isExternalVolumeClass() {
		_, err = r.client.DeleteExternalVolumeClassAccessPolicy(ctx, &ateapi.DeleteExternalVolumeClassAccessPolicyRequest{
			ExternalVolumeClass: r.scope.ref(),
			Name:                resources.SingletonName,
			Options:             options,
		})
	} else {
		_, err = r.client.DeleteAtespaceAccessPolicy(ctx, &ateapi.DeleteAtespaceAccessPolicyRequest{
			Atespace: r.scope.ref(),
			Name:     resources.SingletonName,
			Options:  options,
		})
	}
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
	scope, err := scopeFromFlags(accessPolicyFlags.atespace, accessPolicyFlags.externalVolumeClass, accessPolicyFlags.global)
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
	scope, err := scopeFromFlags(accessPolicyFlags.atespace, accessPolicyFlags.externalVolumeClass, accessPolicyFlags.global)
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
	scope, err := scopeFromFlags(accessPolicyFlags.atespace, accessPolicyFlags.externalVolumeClass, accessPolicyFlags.global)
	if err != nil {
		return err
	}
	hasFile := accessPolicyFlags.filename != ""
	hasMemberFlags := len(accessPolicyFlags.addMembers) > 0 || len(accessPolicyFlags.removeMembers) > 0
	switch {
	case hasFile && accessPolicyFlags.role != "":
		return errors.New("--role cannot be combined with --filename; the manifest sets each binding's role")
	case hasFile && hasMemberFlags:
		return errors.New("--filename cannot be combined with --add-member or --remove-member")
	case !hasFile && !hasMemberFlags:
		return errors.New("one of --filename, --add-member, or --remove-member is required")
	}

	if hasMemberFlags {
		if _, err := resolveMemberEditRole(scope, accessPolicyFlags.role, accessPolicyFlags.addMembers, accessPolicyFlags.removeMembers); err != nil {
			return err
		}
		runner, closeFn, err := newAccessPolicyRunner(cmd, scope)
		if err != nil {
			return err
		}
		defer closeFn()
		return runner.mutateMembers(cmd.Context(), accessPolicyFlags.role, accessPolicyFlags.addMembers, accessPolicyFlags.removeMembers)
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
	scope, err := deleteScopeFromFlags(accessPolicyFlags.atespace, accessPolicyFlags.externalVolumeClass)
	if err != nil {
		return err
	}
	options, err := deleteOptionsFromFlags(deleteGuardFlags{
		uidSet:     cmd.Flags().Changed("uid"),
		uid:        accessPolicyFlags.uid,
		versionSet: cmd.Flags().Changed("version"),
		version:    accessPolicyFlags.version,
	})
	if err != nil {
		return err
	}
	runner, closeFn, err := newAccessPolicyRunner(cmd, scope)
	if err != nil {
		return err
	}
	defer closeFn()
	return runner.delete(cmd.Context(), options)
}

func init() {
	for _, c := range []*cobra.Command{getAccessPolicyCmd, createAccessPolicyCmd, updateAccessPolicyCmd} {
		c.Flags().StringVarP(&accessPolicyFlags.atespace, "atespace", "a", "", "Atespace whose access policy to use")
		c.Flags().StringVarP(&accessPolicyFlags.externalVolumeClass, "external-volume-class", "c", "", "External volume class whose access policy to use")
		c.Flags().BoolVar(&accessPolicyFlags.global, "global", false, "Use the global access policy instead of an atespace's or external volume class's")
	}
	createAccessPolicyCmd.Flags().StringVarP(&accessPolicyFlags.filename, "filename", "f", "", "Manifest file holding one AccessPolicy; use - for stdin (required)")
	_ = createAccessPolicyCmd.MarkFlagRequired("filename")

	updateAccessPolicyCmd.Flags().StringVarP(&accessPolicyFlags.filename, "filename", "f", "", "Manifest file holding one AccessPolicy; use - for stdin")
	updateAccessPolicyCmd.Flags().StringSliceVar(&accessPolicyFlags.addMembers, "add-member", nil, "Add one or more members (e.g. atespace:team-a); may be repeated or comma-separated")
	updateAccessPolicyCmd.Flags().StringSliceVar(&accessPolicyFlags.removeMembers, "remove-member", nil, "Remove one or more members (e.g. atespace:team-a); may be repeated or comma-separated")
	updateAccessPolicyCmd.Flags().StringVar(&accessPolicyFlags.role, "role", "", "Role for --add-member/--remove-member (defaults to allowed_atespace for -c/--external-volume-class)")

	getCmd.AddCommand(getAccessPolicyCmd)
	createCmd.AddCommand(createAccessPolicyCmd)
	updateCmd.AddCommand(updateAccessPolicyCmd)

	deleteAccessPolicyCmd.Flags().StringVarP(&accessPolicyFlags.atespace, "atespace", "a", "", "Atespace whose access policy to delete")
	deleteAccessPolicyCmd.Flags().StringVarP(&accessPolicyFlags.externalVolumeClass, "external-volume-class", "c", "", "External volume class whose access policy to delete")
	deleteAccessPolicyCmd.Flags().StringVar(&accessPolicyFlags.uid, "uid", "", "Delete only if the policy's metadata.uid matches; from get -o yaml")
	deleteAccessPolicyCmd.Flags().Int64Var(&accessPolicyFlags.version, "version", 0, "Delete only if the policy's metadata.version matches; from get -o yaml")
	deleteCmd.AddCommand(deleteAccessPolicyCmd)
}

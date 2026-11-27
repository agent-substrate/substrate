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
	"fmt"
	"maps"
	"strings"

	"github.com/spf13/cobra"
	"go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/cmd/kubectl-ate/internal/printer"
	"go.ate.dev/substrate/internal/resources"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/apimachinery/pkg/api/validate/content"
)

const maxResourceLabels = 64

var labelFlags struct {
	atespace  string
	overwrite bool
}

type labelChange struct {
	key    string
	value  string
	remove bool
}

var labelActorCmd = &cobra.Command{
	Use:   "actor <actor-name> <key=value|key->...",
	Short: "Set or remove labels on an actor",
	Long: `Set or remove labels on an actor.

A key=value argument sets that label. A key- argument removes it.
Changing an existing value requires --overwrite. Labels do not affect scheduling.`,
	Args: labelResourceArgs("<actor-name>"),
	RunE: runLabelActor,
}

var labelActorTemplateCmd = &cobra.Command{
	Use:   "actor-template <template-name> <key=value|key->...",
	Short: "Set or remove labels on an actor template",
	Long: `Set or remove labels on an actor template.

A key=value argument sets that label. A key- argument removes it.
Changing an existing value requires --overwrite. Only labels change;
the rest of the template stays as stored.`,
	Args: labelResourceArgs("<template-name>"),
	RunE: runLabelActorTemplate,
}

var labelEgressPolicyCmd = &cobra.Command{
	Use:   "egress-policy <actor-name> <key=value|key->...",
	Short: "Set or remove labels on an actor's egress policy",
	Long: `Set or remove labels on an actor's egress policy.

A key=value argument sets that label. A key- argument removes it.
Changing an existing value requires --overwrite. The policy rules stay as stored.`,
	Args: labelResourceArgs("<actor-name>"),
	RunE: runLabelEgressPolicy,
}

func labelResourceArgs(argPlaceholder string) cobra.PositionalArgs {
	return func(cmd *cobra.Command, args []string) error {
		if err := checkSwallowedFlags(cmd); err != nil {
			return err
		}
		missingFlags := missingRequiredFlags(cmd, []string{"atespace"})
		if len(args) == 0 {
			if len(missingFlags) > 0 {
				return usageErrorf("missing required argument %s and %s", argPlaceholder, formatMissingFlagsClause(missingFlags))
			}
			return usageErrorf("missing required argument %s", argPlaceholder)
		}
		if len(args) < 2 {
			return usageErrorf("at least one label update is required")
		}
		if len(missingFlags) > 0 {
			return usageErrorf("missing %s", formatMissingFlagsClause(missingFlags))
		}
		return nil
	}
}

func runLabelActor(cmd *cobra.Command, args []string) error {
	changes, err := parseLabelChanges(args[1:])
	if err != nil {
		return wrapUsageError(err)
	}
	ctx := cmd.Context()
	apiClient, err := newAPIClient(cmd)
	if err != nil {
		return fmt.Errorf("failed to connect to ate-api-server: %w", err)
	}
	defer apiClient.Close()

	ref := resources.ActorRef{Atespace: labelFlags.atespace, Name: args[0]}.ToObjectRef()
	updated, err := labelActor(ctx, apiClient, ref, changes, labelFlags.overwrite)
	if err != nil {
		return err
	}
	return printer.PrintActorTo(cmd.OutOrStdout(), updated, outputFmt)
}

func runLabelActorTemplate(cmd *cobra.Command, args []string) error {
	changes, err := parseLabelChanges(args[1:])
	if err != nil {
		return wrapUsageError(err)
	}
	ctx := cmd.Context()
	apiClient, err := newAPIClient(cmd)
	if err != nil {
		return fmt.Errorf("failed to connect to ate-api-server: %w", err)
	}
	defer apiClient.Close()

	ref := &ateapi.ObjectRef{Atespace: labelFlags.atespace, Name: args[0]}
	updated, err := labelActorTemplate(ctx, apiClient, ref, changes, labelFlags.overwrite)
	if err != nil {
		return err
	}
	return printer.PrintActorTemplateTo(cmd.OutOrStdout(), updated, outputFmt)
}

func runLabelEgressPolicy(cmd *cobra.Command, args []string) error {
	changes, err := parseLabelChanges(args[1:])
	if err != nil {
		return wrapUsageError(err)
	}
	ctx := cmd.Context()
	apiClient, err := newAPIClient(cmd)
	if err != nil {
		return fmt.Errorf("failed to connect to ate-api-server: %w", err)
	}
	defer apiClient.Close()

	ref := resources.ActorRef{Atespace: labelFlags.atespace, Name: args[0]}.ToObjectRef()
	updated, err := labelEgressPolicy(ctx, apiClient, ref, changes, labelFlags.overwrite)
	if err != nil {
		return err
	}
	return printer.PrintEgressPolicyTo(cmd.OutOrStdout(), ref.GetName(), updated, outputFmt)
}

func labelActor(ctx context.Context, client actorUpdater, ref *ateapi.ObjectRef, changes []labelChange, overwrite bool) (*ateapi.Actor, error) {
	actor, err := client.GetActor(ctx, &ateapi.GetActorRequest{Actor: ref})
	if err != nil {
		return nil, fmt.Errorf("failed to get actor %q: %w", ref.GetName(), err)
	}
	if actor.Metadata == nil {
		actor.Metadata = &ateapi.ResourceMetadata{}
	}
	labels, err := applyLabelChanges(actor.GetMetadata().GetLabels(), changes, overwrite)
	if err != nil {
		return nil, err
	}
	actor.Metadata.Labels = labels
	resp, err := client.UpdateActor(ctx, &ateapi.UpdateActorRequest{Actor: actor})
	if err != nil {
		return nil, fmt.Errorf("failed to update actor: %w", err)
	}
	return resp, nil
}

type actorTemplateLabeler interface {
	GetActorTemplate(ctx context.Context, in *ateapi.GetActorTemplateRequest, opts ...grpc.CallOption) (*ateapi.ActorTemplate, error)
	UpdateActorTemplate(ctx context.Context, in *ateapi.UpdateActorTemplateRequest, opts ...grpc.CallOption) (*ateapi.ActorTemplate, error)
}

func labelActorTemplate(ctx context.Context, client actorTemplateLabeler, ref *ateapi.ObjectRef, changes []labelChange, overwrite bool) (*ateapi.ActorTemplate, error) {
	tmpl, err := client.GetActorTemplate(ctx, &ateapi.GetActorTemplateRequest{ActorTemplate: ref})
	if err != nil {
		return nil, fmt.Errorf("failed to get actor template %q: %w", ref.GetName(), err)
	}
	labels, err := applyLabelChanges(tmpl.GetMetadata().GetLabels(), changes, overwrite)
	if err != nil {
		return nil, err
	}
	updated, err := client.UpdateActorTemplate(ctx, &ateapi.UpdateActorTemplateRequest{
		ActorTemplate: &ateapi.ActorTemplate{
			Metadata: &ateapi.ResourceMetadata{
				Atespace: tmpl.GetMetadata().GetAtespace(),
				Name:     tmpl.GetMetadata().GetName(),
				Uid:      tmpl.GetMetadata().GetUid(),
				Version:  tmpl.GetMetadata().GetVersion(),
				Labels:   labels,
			},
		},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to update actor template: %w", err)
	}
	return updated, nil
}

type egressPolicyLabeler interface {
	GetActorEgressPolicy(ctx context.Context, req *ateapi.GetActorEgressPolicyRequest, opts ...grpc.CallOption) (*ateapi.EgressPolicy, error)
	UpdateActorEgressPolicy(ctx context.Context, req *ateapi.UpdateActorEgressPolicyRequest, opts ...grpc.CallOption) (*ateapi.EgressPolicy, error)
	actorGetter
}

func labelEgressPolicy(ctx context.Context, client egressPolicyLabeler, actor *ateapi.ObjectRef, changes []labelChange, overwrite bool) (*ateapi.EgressPolicy, error) {
	policy, err := client.GetActorEgressPolicy(ctx, &ateapi.GetActorEgressPolicyRequest{Actor: actor, Name: resources.SingletonName})
	if status.Code(err) == codes.NotFound {
		if err := requireActor(ctx, client, actor); err != nil {
			return nil, err
		}
		return nil, fmt.Errorf("actor %q in atespace %q has no egress policy", actor.GetName(), actor.GetAtespace())
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get egress policy for actor %q in atespace %q: %w", actor.GetName(), actor.GetAtespace(), err)
	}
	if policy.Metadata == nil {
		policy.Metadata = &ateapi.ResourceMetadata{}
	}
	labels, err := applyLabelChanges(policy.GetMetadata().GetLabels(), changes, overwrite)
	if err != nil {
		return nil, err
	}
	policy.Metadata.Labels = labels
	updated, err := client.UpdateActorEgressPolicy(ctx, &ateapi.UpdateActorEgressPolicyRequest{Actor: actor, EgressPolicy: policy})
	if err != nil {
		return nil, fmt.Errorf("failed to update egress policy for actor %q in atespace %q: %w", actor.GetName(), actor.GetAtespace(), err)
	}
	return updated, nil
}

// parseLabelSelector parses a comma-separated key=value selector. An empty
// string returns nil, which lists every resource.
func parseLabelSelector(raw string) (*ateapi.Selector, error) {
	if raw == "" {
		return nil, nil
	}
	matchLabels := make(map[string]string)
	for entry := range strings.SplitSeq(raw, ",") {
		key, val, ok := strings.Cut(entry, "=")
		if !ok {
			return nil, fmt.Errorf("invalid label selector %q: entry %q must be in key=value format", raw, entry)
		}
		if errs := content.IsLabelKey(key); len(errs) > 0 {
			return nil, fmt.Errorf("invalid label selector %q: invalid label key %q: %s", raw, key, strings.Join(errs, "; "))
		}
		if errs := content.IsLabelValue(val); len(errs) > 0 {
			return nil, fmt.Errorf("invalid label selector %q: invalid label value %q for key %q: %s", raw, val, key, strings.Join(errs, "; "))
		}
		if _, exists := matchLabels[key]; exists {
			return nil, fmt.Errorf("invalid label selector %q: duplicate label key %q", raw, key)
		}
		matchLabels[key] = val
	}
	if len(matchLabels) > maxWorkerSelectorLabels {
		return nil, fmt.Errorf("invalid label selector %q: at most %d labels are allowed, got %d", raw, maxWorkerSelectorLabels, len(matchLabels))
	}
	return &ateapi.Selector{MatchLabels: matchLabels}, nil
}

func parseLabelChanges(args []string) ([]labelChange, error) {
	seen := map[string]bool{}
	changes := make([]labelChange, 0, len(args))
	for _, arg := range args {
		if key, val, ok := strings.Cut(arg, "="); ok {
			if err := validateLabelPair(key, val); err != nil {
				return nil, err
			}
			if seen[key] {
				return nil, fmt.Errorf("duplicate label key %q", key)
			}
			seen[key] = true
			changes = append(changes, labelChange{key: key, value: val})
			continue
		}
		if !strings.HasSuffix(arg, "-") || len(arg) < 2 {
			return nil, fmt.Errorf("invalid label update %q: must be key=value or key-", arg)
		}
		key := strings.TrimSuffix(arg, "-")
		if errs := content.IsLabelKey(key); len(errs) > 0 {
			return nil, fmt.Errorf("invalid label key %q: %s", key, strings.Join(errs, "; "))
		}
		if seen[key] {
			return nil, fmt.Errorf("duplicate label key %q", key)
		}
		seen[key] = true
		changes = append(changes, labelChange{key: key, remove: true})
	}
	return changes, nil
}

func validateLabelPair(key, val string) error {
	if errs := content.IsLabelKey(key); len(errs) > 0 {
		return fmt.Errorf("invalid label key %q: %s", key, strings.Join(errs, "; "))
	}
	if errs := content.IsLabelValue(val); len(errs) > 0 {
		return fmt.Errorf("invalid label value %q for key %q: %s", val, key, strings.Join(errs, "; "))
	}
	return nil
}

// applyLabelChanges returns the labels after changes. A nil result clears them.
func applyLabelChanges(have map[string]string, changes []labelChange, overwrite bool) (map[string]string, error) {
	out := maps.Clone(have)
	if out == nil {
		out = map[string]string{}
	}
	for _, change := range changes {
		if change.remove {
			if _, ok := out[change.key]; !ok {
				return nil, fmt.Errorf("label %q not found", change.key)
			}
			delete(out, change.key)
			continue
		}
		if cur, ok := out[change.key]; ok && cur != change.value && !overwrite {
			return nil, fmt.Errorf("label %q already has value %q, use --overwrite to replace it", change.key, cur)
		}
		out[change.key] = change.value
	}
	if len(out) > maxResourceLabels {
		return nil, fmt.Errorf("at most %d labels are allowed, got %d", maxResourceLabels, len(out))
	}
	if len(out) == 0 {
		return nil, nil
	}
	return out, nil
}

func labelsMatch(have, want map[string]string) bool {
	for k, v := range want {
		if have[k] != v {
			return false
		}
	}
	return true
}

func init() {
	for _, c := range []*cobra.Command{labelActorCmd, labelActorTemplateCmd, labelEgressPolicyCmd} {
		c.Flags().StringVarP(&labelFlags.atespace, "atespace", "a", "", "the resource's atespace")
		c.Flags().BoolVar(&labelFlags.overwrite, "overwrite", false, "replace the value of a label that is already set")
		labelCmd.AddCommand(c)
	}
}

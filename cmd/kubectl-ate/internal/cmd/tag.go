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
	"strings"

	"github.com/spf13/cobra"
	"go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/cmd/kubectl-ate/internal/printer"
	"google.golang.org/grpc"
)

var (
	getTagAtespaceFlag       string
	getTagAllAtespacesFlag   bool
	createTagAtespaceFlag    string
	createTagActorFlag       string
	createTagScopeFlag       string
	createTagWithVolumesFlag volumeSelectionValue
	updateTagAtespaceFlag    string
	updateTagScopeFlag       string
	deleteTagAtespaceFlag    string
)

var getTagsCmd = &cobra.Command{
	Use:     "tag [tag-name ...]",
	Aliases: []string{"tags"},
	Short:   "List or get tags",
	Args:    cobra.ArbitraryArgs,
	RunE: func(cmd *cobra.Command, args []string) error {
		if err := validateAtespaceFlags("tags", args, getTagAtespaceFlag, getTagAllAtespacesFlag); err != nil {
			return err
		}

		ctx := cmd.Context()
		client, err := newAPIClient(cmd)
		if err != nil {
			return fmt.Errorf("failed to connect to ate-api-server: %w", err)
		}
		defer client.Close()

		if len(args) > 0 {
			tags := make([]*ateapi.Tag, 0, len(args))
			for _, name := range args {
				tag, err := client.GetTag(ctx, &ateapi.GetTagRequest{
					Tag: &ateapi.ObjectRef{Atespace: getTagAtespaceFlag, Name: name},
				})
				if err != nil {
					return fmt.Errorf("failed to get tag %q: %w", name, err)
				}
				tags = append(tags, tag)
			}
			if len(tags) == 1 {
				return printer.PrintTagTo(cmd.OutOrStdout(), tags[0], outputFmt)
			}
			return printer.PrintTagsTo(cmd.OutOrStdout(), tags, outputFmt)
		}

		var tags []*ateapi.Tag
		pageToken := ""
		for {
			resp, err := client.ListTags(ctx, &ateapi.ListTagsRequest{Atespace: getTagAtespaceFlag, Options: &ateapi.ListOptions{PageSize: 1000, PageToken: pageToken}})
			if err != nil {
				return fmt.Errorf("failed to list tags: %w", err)
			}
			tags = append(tags, resp.GetTags()...)
			pageToken = resp.GetNextPageToken()
			if pageToken == "" {
				break
			}
		}
		return printer.PrintTagsTo(cmd.OutOrStdout(), tags, outputFmt)
	},
}

var createTagCmd = &cobra.Command{
	Use:   "tag <tag-name>",
	Short: "Tag the external snapshot a suspended actor holds",
	Long: "Tag the external snapshot a suspended actor holds.\n\n" +
		"The tag gets its own copy of that snapshot, so suspending the actor\n" +
		"again or deleting it cannot collect what the tag names.",
	Args: createTagArgs,
	RunE: func(cmd *cobra.Command, args []string) error {
		scope, err := parseTagScope(createTagScopeFlag)
		if err != nil {
			return wrapUsageError(err)
		}
		ctx := cmd.Context()
		client, err := newAPIClient(cmd)
		if err != nil {
			return fmt.Errorf("failed to connect to ate-api-server: %w", err)
		}
		defer client.Close()

		tag, err := client.CreateTag(ctx, &ateapi.CreateTagRequest{
			Tag: &ateapi.Tag{
				Metadata:    &ateapi.ResourceMetadata{Atespace: createTagAtespaceFlag, Name: args[0]},
				Scope:       scope,
				SourceActor: &ateapi.ObjectRef{Atespace: createTagAtespaceFlag, Name: createTagActorFlag},
			},
			VolumeSelection: createTagWithVolumesFlag.sel,
		})
		if err != nil {
			return fmt.Errorf("failed to create tag: %w", err)
		}
		return printer.PrintTagTo(cmd.OutOrStdout(), tag, outputFmt)
	},
}

var updateTagCmd = &cobra.Command{
	Use:   "tag <tag-name>",
	Short: "Publish or unpublish a tag",
	Args:  exactlyOneArg("<tag-name>", "atespace", "scope"),
	RunE: func(cmd *cobra.Command, args []string) error {
		scope, err := parseTagScope(updateTagScopeFlag)
		if err != nil {
			return wrapUsageError(err)
		}
		ctx := cmd.Context()
		client, err := newAPIClient(cmd)
		if err != nil {
			return fmt.Errorf("failed to connect to ate-api-server: %w", err)
		}
		defer client.Close()

		ref := &ateapi.ObjectRef{Atespace: updateTagAtespaceFlag, Name: args[0]}
		resp, err := updateTagScope(ctx, client, ref, scope)
		if err != nil {
			return err
		}
		return printer.PrintTagTo(cmd.OutOrStdout(), resp, outputFmt)
	},
}

var deleteTagCmd = &cobra.Command{
	Use:   "tag <tag-name>",
	Short: "Delete a tag",
	Args:  exactlyOneArg("<tag-name>", "atespace"),
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := cmd.Context()
		client, err := newAPIClient(cmd)
		if err != nil {
			return fmt.Errorf("failed to connect to ate-api-server: %w", err)
		}
		defer client.Close()

		_, err = client.DeleteTag(ctx, &ateapi.DeleteTagRequest{Tag: &ateapi.ObjectRef{Atespace: deleteTagAtespaceFlag, Name: args[0]}})
		if err != nil {
			return fmt.Errorf("failed to delete tag: %w", err)
		}
		fmt.Printf("tag %q deleted\n", args[0])
		return nil
	},
}

type tagClient interface {
	GetTag(ctx context.Context, in *ateapi.GetTagRequest, opts ...grpc.CallOption) (*ateapi.Tag, error)
	UpdateTag(ctx context.Context, in *ateapi.UpdateTagRequest, opts ...grpc.CallOption) (*ateapi.Tag, error)
}

func updateTagScope(ctx context.Context, client tagClient, ref *ateapi.ObjectRef, scope ateapi.TagScope) (*ateapi.Tag, error) {
	tag, err := client.GetTag(ctx, &ateapi.GetTagRequest{Tag: ref})
	if err != nil {
		return nil, fmt.Errorf("failed to get tag %q: %w", ref.GetName(), err)
	}
	tag.Scope = scope

	resp, err := client.UpdateTag(ctx, &ateapi.UpdateTagRequest{Tag: tag})
	if err != nil {
		return nil, fmt.Errorf("failed to update tag: %w", err)
	}
	return resp, nil
}

func parseTagScope(value string) (ateapi.TagScope, error) {
	switch strings.ToLower(value) {
	case "atespace":
		return ateapi.TagScope_TAG_SCOPE_ATESPACE, nil
	case "published":
		return ateapi.TagScope_TAG_SCOPE_PUBLISHED, nil
	default:
		return ateapi.TagScope_TAG_SCOPE_UNSPECIFIED, fmt.Errorf("invalid scope %q; must be atespace or published", value)
	}
}

func init() {
	getTagsCmd.Flags().StringVarP(&getTagAtespaceFlag, "atespace", "a", "", "get tags in a specific atespace (mutually exclusive with --all-atespaces)")
	getTagsCmd.Flags().BoolVarP(&getTagAllAtespacesFlag, "all-atespaces", "A", false, "get tags across all atespaces (mutually exclusive with --atespace)")
	getCmd.AddCommand(getTagsCmd)

	createTagCmd.Flags().StringVarP(&createTagAtespaceFlag, "atespace", "a", "", "the actor's atespace; the tag is created here")
	createTagCmd.Flags().StringVar(&createTagActorFlag, "actor", "", "suspended actor whose external snapshot to tag")
	createTagCmd.Flags().StringVar(&createTagScopeFlag, "scope", "atespace", "tag scope: atespace or published")
	addWithVolumesFlag(createTagCmd, &createTagWithVolumesFlag)
	createCmd.AddCommand(createTagCmd)

	updateTagCmd.Flags().StringVarP(&updateTagAtespaceFlag, "atespace", "a", "", "the tag's atespace")
	updateTagCmd.Flags().StringVar(&updateTagScopeFlag, "scope", "", "tag scope: atespace or published")
	updateCmd.AddCommand(updateTagCmd)

	deleteTagCmd.Flags().StringVarP(&deleteTagAtespaceFlag, "atespace", "a", "", "the tag's atespace")
	deleteCmd.AddCommand(deleteTagCmd)
}

// allVolumes is what a bare --with-volumes sets. pflag lets a flag go without
// a value only if NoOptDefVal is non-empty, and "*" cannot be mistaken for a
// volume name, which must be a k8s-short-name.
const allVolumes = "*"

// volumeSelectionValue backs --with-volumes. A bare --with-volumes selects
// every external volume; --with-volumes=a,b (comma-separated or repeated)
// selects only those. Left unset, sel stays nil and no volume is captured.
type volumeSelectionValue struct {
	sel *ateapi.ExternalVolumeSelection
}

// all reports whether every external volume is selected.
func (v *volumeSelectionValue) all() bool {
	return v.sel != nil && len(v.sel.GetNames()) == 0
}

func (v *volumeSelectionValue) String() string {
	if v.all() {
		return allVolumes
	}
	return strings.Join(v.sel.GetNames(), ",")
}

func (v *volumeSelectionValue) Set(s string) error {
	if s == "" {
		return fmt.Errorf("no volume names after =; drop the = to capture every volume")
	}
	if v.sel != nil && (s == allVolumes) != v.all() {
		return fmt.Errorf("cannot both capture every volume and name volumes")
	}
	if v.sel == nil {
		v.sel = &ateapi.ExternalVolumeSelection{}
	}
	if s != allVolumes {
		v.sel.Names = append(v.sel.Names, strings.Split(s, ",")...)
	}
	return nil
}

func (v *volumeSelectionValue) Type() string { return "names" }

// addWithVolumesFlag registers --with-volumes on cmd, backed by v.
func addWithVolumesFlag(cmd *cobra.Command, v *volumeSelectionValue) {
	cmd.Flags().Var(v, "with-volumes", "also snapshot the actor's external volumes (preview): all of them, or only those named with --with-volumes=a,b")
	cmd.Flags().Lookup("with-volumes").NoOptDefVal = allVolumes
}

// createTagArgs checks create tag's arguments. On top of exactlyOneArg, it
// explains the likely cause of an extra argument after a bare --with-volumes:
// the names were separated from the flag by a space instead of =, so pflag
// read them as an argument.
func createTagArgs(cmd *cobra.Command, args []string) error {
	err := exactlyOneArg("<tag-name>", "atespace", "actor")(cmd, args)
	if err != nil && len(args) > 1 && createTagWithVolumesFlag.all() {
		return usageErrorf("%v; to capture only some volumes, join the names to the flag with =, e.g. --with-volumes=%s", err, args[len(args)-1])
	}
	return err
}

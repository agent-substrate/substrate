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
	"strings"

	"github.com/agent-substrate/substrate/cmd/kubectl-ate/internal/printer"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
)

var (
	getTagAtespaceFlag     string
	getTagAllAtespacesFlag bool
	createTagAtespaceFlag  string
	createTagActorFlag     string
	createTagScopeFlag     string
	updateTagAtespaceFlag  string
	updateTagScopeFlag     string
	deleteTagAtespaceFlag  string
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
			tags := make([]*ateapipb.Tag, 0, len(args))
			for _, name := range args {
				tag, err := client.GetTag(ctx, &ateapipb.GetTagRequest{
					Tag: &ateapipb.ObjectRef{Atespace: getTagAtespaceFlag, Name: name},
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

		var tags []*ateapipb.Tag
		pageToken := ""
		for {
			resp, err := client.ListTags(ctx, &ateapipb.ListTagsRequest{Atespace: getTagAtespaceFlag, Options: &ateapipb.ListOptions{PageSize: 1000, PageToken: pageToken}})
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
	Args: exactlyOneArg("<tag-name>", "atespace", "actor"),
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

		tag, err := client.CreateTag(ctx, &ateapipb.CreateTagRequest{
			Tag: &ateapipb.Tag{
				Metadata:    &ateapipb.ResourceMetadata{Atespace: createTagAtespaceFlag, Name: args[0]},
				Scope:       scope,
				SourceActor: &ateapipb.ObjectRef{Atespace: createTagAtespaceFlag, Name: createTagActorFlag},
			},
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

		ref := &ateapipb.ObjectRef{Atespace: updateTagAtespaceFlag, Name: args[0]}
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

		_, err = client.DeleteTag(ctx, &ateapipb.DeleteTagRequest{Tag: &ateapipb.ObjectRef{Atespace: deleteTagAtespaceFlag, Name: args[0]}})
		if err != nil {
			return fmt.Errorf("failed to delete tag: %w", err)
		}
		fmt.Printf("tag %q deleted\n", args[0])
		return nil
	},
}

type tagClient interface {
	GetTag(ctx context.Context, in *ateapipb.GetTagRequest, opts ...grpc.CallOption) (*ateapipb.Tag, error)
	UpdateTag(ctx context.Context, in *ateapipb.UpdateTagRequest, opts ...grpc.CallOption) (*ateapipb.Tag, error)
}

func updateTagScope(ctx context.Context, client tagClient, ref *ateapipb.ObjectRef, scope ateapipb.TagScope) (*ateapipb.Tag, error) {
	tag, err := client.GetTag(ctx, &ateapipb.GetTagRequest{Tag: ref})
	if err != nil {
		return nil, fmt.Errorf("failed to get tag %q: %w", ref.GetName(), err)
	}
	tag.Scope = scope

	resp, err := client.UpdateTag(ctx, &ateapipb.UpdateTagRequest{Tag: tag})
	if err != nil {
		return nil, fmt.Errorf("failed to update tag: %w", err)
	}
	return resp, nil
}

func parseTagScope(value string) (ateapipb.TagScope, error) {
	switch strings.ToLower(value) {
	case "atespace":
		return ateapipb.TagScope_TAG_SCOPE_ATESPACE, nil
	case "published":
		return ateapipb.TagScope_TAG_SCOPE_PUBLISHED, nil
	default:
		return ateapipb.TagScope_TAG_SCOPE_UNSPECIFIED, fmt.Errorf("invalid scope %q; must be atespace or published", value)
	}
}

func init() {
	getTagsCmd.Flags().StringVarP(&getTagAtespaceFlag, "atespace", "a", "", "get tags in a specific atespace (mutually exclusive with --all-atespaces)")
	getTagsCmd.Flags().BoolVarP(&getTagAllAtespacesFlag, "all-atespaces", "A", false, "get tags across all atespaces (mutually exclusive with --atespace)")
	getCmd.AddCommand(getTagsCmd)

	createTagCmd.Flags().StringVarP(&createTagAtespaceFlag, "atespace", "a", "", "the actor's atespace; the tag is created here")
	createTagCmd.Flags().StringVar(&createTagActorFlag, "actor", "", "suspended actor whose external snapshot to tag")
	createTagCmd.Flags().StringVar(&createTagScopeFlag, "scope", "atespace", "tag scope: atespace or published")
	createCmd.AddCommand(createTagCmd)

	updateTagCmd.Flags().StringVarP(&updateTagAtespaceFlag, "atespace", "a", "", "the tag's atespace")
	updateTagCmd.Flags().StringVar(&updateTagScopeFlag, "scope", "", "tag scope: atespace or published")
	updateCmd.AddCommand(updateTagCmd)

	deleteTagCmd.Flags().StringVarP(&deleteTagAtespaceFlag, "atespace", "a", "", "the tag's atespace")
	deleteCmd.AddCommand(deleteTagCmd)
}

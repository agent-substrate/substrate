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
	"github.com/agent-substrate/substrate/internal/ateclient"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
)

var (
	tagAtespaceFlag       string
	tagAllAtespacesFlag   bool
	createTagAtespaceFlag string
	createTagActorFlag    string
	createTagScopeFlag    string
	updateTagAtespaceFlag string
	updateTagScopeFlag    string
	deleteTagAtespaceFlag string
)

var updateCmd = &cobra.Command{Use: "update", Short: "Update a resource"}

var getTagsCmd = &cobra.Command{
	Use:     "tags [tag-name ...]",
	Aliases: []string{"tag"},
	Short:   "List or get tags",
	RunE: func(cmd *cobra.Command, args []string) error {
		if tagAllAtespacesFlag && tagAtespaceFlag != "" {
			return fmt.Errorf("--atespace and -A/--all-atespaces are mutually exclusive")
		}
		if len(args) > 0 && tagAtespaceFlag == "" {
			return fmt.Errorf("--atespace is required when getting tags")
		}
		if len(args) == 0 && !tagAllAtespacesFlag && tagAtespaceFlag == "" {
			return fmt.Errorf("specify --atespace <name>, or -A/--all-atespaces")
		}

		ctx := cmd.Context()
		client, err := ateclient.NewClient(ctx, kubeconfig, k8sContext, endpoint, tokenFile, traceEnabled)
		if err != nil {
			return fmt.Errorf("failed to connect to ate-api-server: %w", err)
		}
		defer client.Close()

		var tags []*ateapipb.Tag
		if len(args) > 0 {
			for _, name := range args {
				tag, err := client.GetTag(ctx, &ateapipb.GetTagRequest{
					Tag: &ateapipb.ObjectRef{Atespace: tagAtespaceFlag, Name: name},
				})
				if err != nil {
					return fmt.Errorf("failed to get tag %q: %w", name, err)
				}
				tags = append(tags, tag)
			}
		} else {
			pageToken := ""
			for {
				resp, err := client.ListTags(ctx, &ateapipb.ListTagsRequest{Atespace: tagAtespaceFlag, PageSize: 1000, PageToken: pageToken})
				if err != nil {
					return fmt.Errorf("failed to list tags: %w", err)
				}
				tags = append(tags, resp.GetTags()...)
				pageToken = resp.GetNextPageToken()
				if pageToken == "" {
					break
				}
			}
		}
		return printer.PrintTags(tags, outputFmt)
	},
}

var createTagCmd = &cobra.Command{
	Use:     "tag <tag-name>",
	Aliases: []string{"tags"},
	Short:   "Tag the external snapshot a suspended actor holds",
	Long: "Tag the external snapshot a suspended actor holds.\n\n" +
		"The tag gets its own copy of that snapshot, so suspending the actor\n" +
		"again or deleting it cannot collect what the tag names.",
	Args: cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		scope, err := parseTagScope(createTagScopeFlag)
		if err != nil {
			return err
		}
		ctx := cmd.Context()
		client, err := ateclient.NewClient(ctx, kubeconfig, k8sContext, endpoint, tokenFile, traceEnabled)
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
		return printer.PrintTag(tag, outputFmt)
	},
}

var updateTagCmd = &cobra.Command{
	Use:     "tag <tag-name>",
	Aliases: []string{"tags"},
	Short:   "Publish or unpublish a tag",
	Args:    cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		scope, err := parseTagScope(updateTagScopeFlag)
		if err != nil {
			return err
		}
		ctx := cmd.Context()
		client, err := ateclient.NewClient(ctx, kubeconfig, k8sContext, endpoint, tokenFile, traceEnabled)
		if err != nil {
			return fmt.Errorf("failed to connect to ate-api-server: %w", err)
		}
		defer client.Close()

		ref := &ateapipb.ObjectRef{Atespace: updateTagAtespaceFlag, Name: args[0]}
		resp, err := updateTagScope(ctx, client, ref, scope)
		if err != nil {
			return err
		}
		return printer.PrintTag(resp, outputFmt)
	},
}

var deleteTagCmd = &cobra.Command{
	Use:     "tag <tag-name>",
	Aliases: []string{"tags"},
	Short:   "Delete a tag",
	Args:    cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := cmd.Context()
		client, err := ateclient.NewClient(ctx, kubeconfig, k8sContext, endpoint, tokenFile, traceEnabled)
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
	getTagsCmd.Flags().StringVarP(&tagAtespaceFlag, "atespace", "a", "", "Atespace to list/get tags in")
	getTagsCmd.Flags().BoolVarP(&tagAllAtespacesFlag, "all-atespaces", "A", false, "List tags across all atespaces")
	getCmd.AddCommand(getTagsCmd)

	createTagCmd.Flags().StringVarP(&createTagAtespaceFlag, "atespace", "a", "", "Atespace the actor lives in; the tag is created here (required)")
	_ = createTagCmd.MarkFlagRequired("atespace")
	createTagCmd.Flags().StringVar(&createTagActorFlag, "actor", "", "Name of the suspended actor whose external snapshot to tag (required)")
	_ = createTagCmd.MarkFlagRequired("actor")
	createTagCmd.Flags().StringVar(&createTagScopeFlag, "scope", "atespace", "Tag scope: atespace or published")
	createCmd.AddCommand(createTagCmd)

	rootCmd.AddCommand(updateCmd)
	updateTagCmd.Flags().StringVarP(&updateTagAtespaceFlag, "atespace", "a", "", "Atespace owning the tag (required)")
	updateTagCmd.Flags().StringVar(&updateTagScopeFlag, "scope", "", "Tag scope: atespace or published (required)")
	_ = updateTagCmd.MarkFlagRequired("atespace")
	_ = updateTagCmd.MarkFlagRequired("scope")
	updateCmd.AddCommand(updateTagCmd)

	deleteTagCmd.Flags().StringVarP(&deleteTagAtespaceFlag, "atespace", "a", "", "Atespace owning the tag (required)")
	_ = deleteTagCmd.MarkFlagRequired("atespace")
	deleteCmd.AddCommand(deleteTagCmd)
}

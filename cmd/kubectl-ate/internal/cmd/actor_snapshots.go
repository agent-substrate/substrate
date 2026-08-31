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
	snapshotAtespaceFlag     string
	snapshotAllAtespacesFlag bool
	snapshotTagRefFlag       bool
	createTagAtespaceFlag    string
	createTagSnapshotFlag    string
	createTagScopeFlag       string
	updateTagAtespaceFlag    string
	updateTagScopeFlag       string
	deleteTagAtespaceFlag    string
)

var updateCmd = &cobra.Command{Use: "update", Short: "Update a resource"}

var getActorSnapshotsCmd = &cobra.Command{
	Use:     "actor-snapshots [snapshot-name ...]",
	Aliases: []string{"actor-snapshot", "snapshots", "snapshot"},
	Short:   "List or get actor snapshots",
	RunE: func(cmd *cobra.Command, args []string) error {
		if snapshotAllAtespacesFlag && snapshotAtespaceFlag != "" {
			return fmt.Errorf("--atespace and -A/--all-atespaces are mutually exclusive")
		}
		if len(args) > 0 && snapshotAtespaceFlag == "" {
			return fmt.Errorf("--atespace is required when getting snapshots")
		}
		if len(args) == 0 && !snapshotAllAtespacesFlag && snapshotAtespaceFlag == "" {
			return fmt.Errorf("specify --atespace <name>, or -A/--all-atespaces")
		}
		if snapshotTagRefFlag && len(args) == 0 {
			return fmt.Errorf("--tag requires at least one tag name")
		}

		ctx := cmd.Context()
		client, err := ateclient.NewClient(ctx, kubeconfig, k8sContext, endpoint, tokenFile, traceEnabled)
		if err != nil {
			return fmt.Errorf("failed to connect to ate-api-server: %w", err)
		}
		defer client.Close()

		var snapshots []*ateapipb.ActorSnapshot
		if len(args) > 0 {
			for _, name := range args {
				ref := &ateapipb.ObjectRef{Atespace: snapshotAtespaceFlag, Name: name}
				if snapshotTagRefFlag {
					tag, err := client.GetActorSnapshotTag(ctx, &ateapipb.GetActorSnapshotTagRequest{ActorSnapshotTag: ref})
					if err != nil {
						return fmt.Errorf("failed to get actor snapshot tag %q: %w", name, err)
					}
					ref = tag.GetSnapshot()
				}
				snapshot, err := client.GetActorSnapshot(ctx, &ateapipb.GetActorSnapshotRequest{ActorSnapshot: ref})
				if err != nil {
					return fmt.Errorf("failed to get actor snapshot %q: %w", name, err)
				}
				snapshots = append(snapshots, snapshot)
			}
		} else {
			pageToken := ""
			for {
				resp, err := client.ListActorSnapshots(ctx, &ateapipb.ListActorSnapshotsRequest{Atespace: snapshotAtespaceFlag, PageSize: 1000, PageToken: pageToken})
				if err != nil {
					return fmt.Errorf("failed to list actor snapshots: %w", err)
				}
				snapshots = append(snapshots, resp.GetActorSnapshots()...)
				pageToken = resp.GetNextPageToken()
				if pageToken == "" {
					break
				}
			}
		}
		return printer.PrintActorSnapshots(snapshots, outputFmt)
	},
}

var createActorSnapshotTagCmd = &cobra.Command{
	Use:     "actor-snapshot-tag <tag-name>",
	Aliases: []string{"snapshot-tag"},
	Short:   "Tag an actor snapshot",
	Args:    cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		scope, err := parseActorSnapshotTagScope(createTagScopeFlag)
		if err != nil {
			return err
		}
		ctx := cmd.Context()
		client, err := ateclient.NewClient(ctx, kubeconfig, k8sContext, endpoint, tokenFile, traceEnabled)
		if err != nil {
			return fmt.Errorf("failed to connect to ate-api-server: %w", err)
		}
		defer client.Close()

		resp, err := client.CreateActorSnapshotTag(ctx, &ateapipb.CreateActorSnapshotTagRequest{
			ActorSnapshotTag: &ateapipb.ActorSnapshotTag{
				Metadata: &ateapipb.ResourceMetadata{Atespace: createTagAtespaceFlag, Name: args[0]},
				Snapshot: &ateapipb.ObjectRef{Atespace: createTagAtespaceFlag, Name: createTagSnapshotFlag},
				Scope:    scope,
			},
		})
		if err != nil {
			return fmt.Errorf("failed to tag actor snapshot: %w", err)
		}
		return printer.PrintActorSnapshotTag(resp, outputFmt)
	},
}

var updateActorSnapshotTagCmd = &cobra.Command{
	Use:     "actor-snapshot-tag <tag-name>",
	Aliases: []string{"snapshot-tag"},
	Short:   "Publish or unpublish an actor snapshot tag",
	Args:    cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		scope, err := parseActorSnapshotTagScope(updateTagScopeFlag)
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
		resp, err := updateActorSnapshotTagScope(ctx, client, ref, scope)
		if err != nil {
			return err
		}
		return printer.PrintActorSnapshotTag(resp, outputFmt)
	},
}

var deleteActorSnapshotTagCmd = &cobra.Command{
	Use:     "actor-snapshot-tag <tag-name>",
	Aliases: []string{"snapshot-tag"},
	Short:   "Delete an actor snapshot tag",
	Args:    cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := cmd.Context()
		client, err := ateclient.NewClient(ctx, kubeconfig, k8sContext, endpoint, tokenFile, traceEnabled)
		if err != nil {
			return fmt.Errorf("failed to connect to ate-api-server: %w", err)
		}
		defer client.Close()

		_, err = client.DeleteActorSnapshotTag(ctx, &ateapipb.DeleteActorSnapshotTagRequest{ActorSnapshotTag: &ateapipb.ObjectRef{Atespace: deleteTagAtespaceFlag, Name: args[0]}})
		if err != nil {
			return fmt.Errorf("failed to delete actor snapshot tag: %w", err)
		}
		fmt.Printf("actor snapshot tag %q deleted\n", args[0])
		return nil
	},
}

type actorSnapshotTagClient interface {
	GetActorSnapshotTag(ctx context.Context, in *ateapipb.GetActorSnapshotTagRequest, opts ...grpc.CallOption) (*ateapipb.ActorSnapshotTag, error)
	UpdateActorSnapshotTag(ctx context.Context, in *ateapipb.UpdateActorSnapshotTagRequest, opts ...grpc.CallOption) (*ateapipb.ActorSnapshotTag, error)
}

func updateActorSnapshotTagScope(ctx context.Context, client actorSnapshotTagClient, ref *ateapipb.ObjectRef, scope ateapipb.ActorSnapshotTagScope) (*ateapipb.ActorSnapshotTag, error) {
	tag, err := client.GetActorSnapshotTag(ctx, &ateapipb.GetActorSnapshotTagRequest{ActorSnapshotTag: ref})
	if err != nil {
		return nil, fmt.Errorf("failed to get actor snapshot tag %q: %w", ref.GetName(), err)
	}
	tag.Scope = scope

	resp, err := client.UpdateActorSnapshotTag(ctx, &ateapipb.UpdateActorSnapshotTagRequest{ActorSnapshotTag: tag})
	if err != nil {
		return nil, fmt.Errorf("failed to update actor snapshot tag: %w", err)
	}
	return resp, nil
}

func parseActorSnapshotTagScope(value string) (ateapipb.ActorSnapshotTagScope, error) {
	switch strings.ToLower(value) {
	case "atespace":
		return ateapipb.ActorSnapshotTagScope_ACTOR_SNAPSHOT_TAG_SCOPE_ATESPACE, nil
	case "published":
		return ateapipb.ActorSnapshotTagScope_ACTOR_SNAPSHOT_TAG_SCOPE_PUBLISHED, nil
	default:
		return ateapipb.ActorSnapshotTagScope_ACTOR_SNAPSHOT_TAG_SCOPE_UNSPECIFIED, fmt.Errorf("invalid scope %q; must be atespace or published", value)
	}
}

func parseNamespacedName(value string) (*ateapipb.ObjectRef, error) {
	atespace, name, ok := strings.Cut(value, "/")
	if !ok || atespace == "" || name == "" || strings.Contains(name, "/") {
		return nil, fmt.Errorf("malformed reference %q (expected <atespace>/<name>)", value)
	}
	return &ateapipb.ObjectRef{Atespace: atespace, Name: name}, nil
}

func init() {
	getActorSnapshotsCmd.Flags().StringVarP(&snapshotAtespaceFlag, "atespace", "a", "", "Atespace to list/get snapshots in")
	getActorSnapshotsCmd.Flags().BoolVarP(&snapshotAllAtespacesFlag, "all-atespaces", "A", false, "List snapshots across all atespaces")
	getActorSnapshotsCmd.Flags().BoolVar(&snapshotTagRefFlag, "tag", false, "Resolve the supplied names as tags")
	getCmd.AddCommand(getActorSnapshotsCmd)

	createActorSnapshotTagCmd.Flags().StringVarP(&createTagAtespaceFlag, "atespace", "a", "", "Atespace owning the snapshot and tag (required)")
	createActorSnapshotTagCmd.Flags().StringVar(&createTagSnapshotFlag, "snapshot", "", "Snapshot name to tag (required)")
	createActorSnapshotTagCmd.Flags().StringVar(&createTagScopeFlag, "scope", "atespace", "Tag scope: atespace or published")
	_ = createActorSnapshotTagCmd.MarkFlagRequired("atespace")
	_ = createActorSnapshotTagCmd.MarkFlagRequired("snapshot")
	createCmd.AddCommand(createActorSnapshotTagCmd)

	rootCmd.AddCommand(updateCmd)
	updateActorSnapshotTagCmd.Flags().StringVarP(&updateTagAtespaceFlag, "atespace", "a", "", "Atespace owning the tag (required)")
	updateActorSnapshotTagCmd.Flags().StringVar(&updateTagScopeFlag, "scope", "", "Tag scope: atespace or published (required)")
	_ = updateActorSnapshotTagCmd.MarkFlagRequired("atespace")
	_ = updateActorSnapshotTagCmd.MarkFlagRequired("scope")
	updateCmd.AddCommand(updateActorSnapshotTagCmd)

	deleteActorSnapshotTagCmd.Flags().StringVarP(&deleteTagAtespaceFlag, "atespace", "a", "", "Atespace owning the tag (required)")
	_ = deleteActorSnapshotTagCmd.MarkFlagRequired("atespace")
	deleteCmd.AddCommand(deleteActorSnapshotTagCmd)
}

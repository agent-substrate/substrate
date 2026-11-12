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
	"fmt"
	"slices"

	"github.com/spf13/cobra"
	"go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/cmd/kubectl-ate/internal/printer"
)

var (
	getExternalVolumeSnapshotAtespaceFlag     string
	getExternalVolumeSnapshotAllAtespacesFlag bool
	getExternalVolumeSnapshotTagFlag          string
)

var getExternalVolumeSnapshotsCmd = &cobra.Command{
	Use:     "externalvolumesnapshot [snapshot-name ...]",
	Aliases: []string{"externalvolumesnapshots"},
	Short:   "List or get external volume snapshots",
	Long: "List or get the external volume snapshots that tags own.\n\n" +
		"This needs the ExternalVolumes preview gate on the server, and --preview.",
	Args: cobra.ArbitraryArgs,
	RunE: func(cmd *cobra.Command, args []string) error {
		if err := validateAtespaceFlags("external volume snapshots", args, getExternalVolumeSnapshotAtespaceFlag, getExternalVolumeSnapshotAllAtespacesFlag); err != nil {
			return err
		}
		if getExternalVolumeSnapshotTagFlag != "" {
			if len(args) > 0 {
				return usageErrorf("--tag cannot be used when getting external volume snapshots by name")
			}
			// A tag name is unique only within an atespace.
			if getExternalVolumeSnapshotAtespaceFlag == "" {
				return usageErrorf("--tag requires -a/--atespace")
			}
		}

		ctx := cmd.Context()
		client, err := newAPIClient(cmd)
		if err != nil {
			return fmt.Errorf("failed to connect to ate-api-server: %w", err)
		}
		defer client.Close()

		if len(args) > 0 {
			snapshots := make([]*ateapi.ExternalVolumeSnapshot, 0, len(args))
			for _, name := range args {
				snapshot, err := client.GetExternalVolumeSnapshot(ctx, &ateapi.GetExternalVolumeSnapshotRequest{
					ExternalVolumeSnapshot: &ateapi.ObjectRef{Atespace: getExternalVolumeSnapshotAtespaceFlag, Name: name},
				})
				if err != nil {
					return fmt.Errorf("failed to get external volume snapshot %q: %w", name, err)
				}
				snapshots = append(snapshots, snapshot)
			}
			if len(snapshots) == 1 {
				return printer.PrintExternalVolumeSnapshotTo(cmd.OutOrStdout(), snapshots[0], outputFmt)
			}
			return printer.PrintExternalVolumeSnapshotsTo(cmd.OutOrStdout(), snapshots, outputFmt)
		}

		var snapshots []*ateapi.ExternalVolumeSnapshot
		pageToken := ""
		for {
			resp, err := client.ListExternalVolumeSnapshots(ctx, &ateapi.ListExternalVolumeSnapshotsRequest{
				Atespace: getExternalVolumeSnapshotAtespaceFlag,
				Options:  &ateapi.ListOptions{PageSize: 1000, PageToken: pageToken},
			})
			if err != nil {
				return fmt.Errorf("failed to list external volume snapshots: %w", err)
			}
			snapshots = append(snapshots, resp.GetExternalVolumeSnapshots()...)
			pageToken = resp.GetNextPageToken()
			if pageToken == "" {
				break
			}
		}
		snapshots = filterByTag(snapshots, getExternalVolumeSnapshotTagFlag)
		return printer.PrintExternalVolumeSnapshotsTo(cmd.OutOrStdout(), snapshots, outputFmt)
	},
}

// filterByTag keeps the snapshots whose origin tag is named tag. The API has
// no filter, so the CLI filters what it listed in one atespace. An empty tag
// keeps all.
func filterByTag(snapshots []*ateapi.ExternalVolumeSnapshot, tag string) []*ateapi.ExternalVolumeSnapshot {
	if tag == "" {
		return snapshots
	}
	return slices.DeleteFunc(snapshots, func(s *ateapi.ExternalVolumeSnapshot) bool {
		return s.GetOrigin().GetTag().GetTag().GetName() != tag
	})
}

func init() {
	getExternalVolumeSnapshotsCmd.Flags().StringVarP(&getExternalVolumeSnapshotAtespaceFlag, "atespace", "a", "", "get snapshots in a specific atespace (mutually exclusive with --all-atespaces)")
	getExternalVolumeSnapshotsCmd.Flags().BoolVarP(&getExternalVolumeSnapshotAllAtespacesFlag, "all-atespaces", "A", false, "get snapshots across all atespaces (mutually exclusive with --atespace)")
	getExternalVolumeSnapshotsCmd.Flags().StringVar(&getExternalVolumeSnapshotTagFlag, "tag", "", "list only the snapshots this tag owns; requires --atespace")
	getCmd.AddCommand(getExternalVolumeSnapshotsCmd)
}

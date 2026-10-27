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

	"github.com/spf13/cobra"
	"go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/cmd/kubectl-ate/internal/printer"
	"go.ate.dev/substrate/internal/ateclient"
	"google.golang.org/grpc"
)

var (
	externalVolumeAtespaceFlag            string
	externalVolumeAllAtespacesFlag        bool
	createExternalVolumeAtespaceFlag      string
	createExternalVolumeVolumeIDFlag      string
	createExternalVolumeTypeFlag          string
	createExternalVolumeContextFlag       map[string]string
	createExternalVolumeDeleteTriggerFlag string
	updateExternalVolumeAtespaceFlag      string
	updateExternalVolumeDeleteTriggerFlag string
	deleteExternalVolumeAtespaceFlag      string
)

var getExternalVolumesCmd = &cobra.Command{
	Use:     "externalvolumes [volume-name ...]",
	Aliases: []string{"externalvolume", "ev"},
	Short:   "List or get shared volumes",
	RunE: func(cmd *cobra.Command, args []string) error {
		if externalVolumeAllAtespacesFlag && externalVolumeAtespaceFlag != "" {
			return fmt.Errorf("--atespace and -A/--all-atespaces are mutually exclusive")
		}
		if len(args) > 0 && externalVolumeAtespaceFlag == "" {
			return fmt.Errorf("--atespace is required when getting shared volumes")
		}
		if len(args) == 0 && !externalVolumeAllAtespacesFlag && externalVolumeAtespaceFlag == "" {
			return fmt.Errorf("specify --atespace <name>, or -A/--all-atespaces")
		}

		ctx := cmd.Context()
		client, err := ateclient.NewClient(ctx, kubeconfig, k8sContext, endpoint, tokenFile, traceEnabled)
		if err != nil {
			return fmt.Errorf("failed to connect to ate-api-server: %w", err)
		}
		defer client.Close()

		if len(args) > 0 {
			volumes := make([]*ateapi.ExternalVolume, 0, len(args))
			for _, name := range args {
				volume, err := client.GetExternalVolume(ctx, &ateapi.GetExternalVolumeRequest{
					ExternalVolume: &ateapi.ObjectRef{Atespace: externalVolumeAtespaceFlag, Name: name},
				})
				if err != nil {
					return fmt.Errorf("failed to get shared volume %q: %w", name, err)
				}
				volumes = append(volumes, volume)
			}
			if len(volumes) == 1 {
				return printer.PrintExternalVolumeTo(cmd.OutOrStdout(), volumes[0], outputFmt)
			}
			return printer.PrintExternalVolumesTo(cmd.OutOrStdout(), volumes, outputFmt)
		}

		var volumes []*ateapi.ExternalVolume
		pageToken := ""
		for {
			resp, err := client.ListExternalVolumes(ctx, &ateapi.ListExternalVolumesRequest{
				Atespace: externalVolumeAtespaceFlag, Options: &ateapi.ListOptions{PageSize: 1000, PageToken: pageToken},
			})
			if err != nil {
				return fmt.Errorf("failed to list shared volumes: %w", err)
			}
			volumes = append(volumes, resp.GetExternalVolumes()...)
			pageToken = resp.GetNextPageToken()
			if pageToken == "" {
				break
			}
		}
		return printer.PrintExternalVolumesTo(cmd.OutOrStdout(), volumes, outputFmt)
	},
}

var createExternalVolumeCmd = &cobra.Command{
	Use:     "externalvolume <volume-name>",
	Aliases: []string{"externalvolumes", "ev"},
	Short:   "Register an existing volume for actors to share",
	Long: "Register a volume that already exists in the storage system so actors can\n" +
		"reference it from their templates. Name it by the driver's --volume-id and\n" +
		"--volume-type.",
	Args: cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		trigger, err := parseDeleteTrigger(createExternalVolumeDeleteTriggerFlag)
		if err != nil {
			return err
		}
		ctx := cmd.Context()
		client, err := ateclient.NewClient(ctx, kubeconfig, k8sContext, endpoint, tokenFile, traceEnabled)
		if err != nil {
			return fmt.Errorf("failed to connect to ate-api-server: %w", err)
		}
		defer client.Close()

		volume, err := client.CreateExternalVolume(ctx, &ateapi.CreateExternalVolumeRequest{
			ExternalVolume: &ateapi.ExternalVolume{
				Metadata:      &ateapi.ResourceMetadata{Atespace: createExternalVolumeAtespaceFlag, Name: args[0]},
				DeleteTrigger: trigger,
				VolumeId:      createExternalVolumeVolumeIDFlag,
				VolumeType:    createExternalVolumeTypeFlag,
				VolumeContext: createExternalVolumeContextFlag,
			},
		})
		if err != nil {
			return fmt.Errorf("failed to create shared volume: %w", err)
		}
		return printer.PrintExternalVolumeTo(cmd.OutOrStdout(), volume, outputFmt)
	},
}

var updateExternalVolumeCmd = &cobra.Command{
	Use:     "externalvolume <volume-name>",
	Aliases: []string{"externalvolumes", "ev"},
	Short:   "Change a shared volume's delete trigger",
	Args:    cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		trigger, err := parseDeleteTrigger(updateExternalVolumeDeleteTriggerFlag)
		if err != nil {
			return err
		}
		ctx := cmd.Context()
		client, err := ateclient.NewClient(ctx, kubeconfig, k8sContext, endpoint, tokenFile, traceEnabled)
		if err != nil {
			return fmt.Errorf("failed to connect to ate-api-server: %w", err)
		}
		defer client.Close()

		ref := &ateapi.ObjectRef{Atespace: updateExternalVolumeAtespaceFlag, Name: args[0]}
		resp, err := updateExternalVolumeDeleteTrigger(ctx, client, ref, trigger)
		if err != nil {
			return err
		}
		return printer.PrintExternalVolumeTo(cmd.OutOrStdout(), resp, outputFmt)
	},
}

var deleteExternalVolumeCmd = &cobra.Command{
	Use:     "externalvolume <volume-name>",
	Aliases: []string{"externalvolumes", "ev"},
	Short:   "Delete a shared volume and its storage",
	Args:    cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := cmd.Context()
		client, err := ateclient.NewClient(ctx, kubeconfig, k8sContext, endpoint, tokenFile, traceEnabled)
		if err != nil {
			return fmt.Errorf("failed to connect to ate-api-server: %w", err)
		}
		defer client.Close()

		_, err = client.DeleteExternalVolume(ctx, &ateapi.DeleteExternalVolumeRequest{
			ExternalVolume: &ateapi.ObjectRef{Atespace: deleteExternalVolumeAtespaceFlag, Name: args[0]},
		})
		if err != nil {
			return fmt.Errorf("failed to delete shared volume: %w", err)
		}
		fmt.Fprintf(cmd.OutOrStdout(), "shared volume %q deleted\n", args[0])
		return nil
	},
}

type externalVolumeClient interface {
	GetExternalVolume(ctx context.Context, in *ateapi.GetExternalVolumeRequest, opts ...grpc.CallOption) (*ateapi.ExternalVolume, error)
	UpdateExternalVolume(ctx context.Context, in *ateapi.UpdateExternalVolumeRequest, opts ...grpc.CallOption) (*ateapi.ExternalVolume, error)
}

// updateExternalVolumeDeleteTrigger reads the volume first so the update carries
// the uid and version the server requires as preconditions.
func updateExternalVolumeDeleteTrigger(ctx context.Context, client externalVolumeClient, ref *ateapi.ObjectRef, trigger ateapi.DeleteTrigger) (*ateapi.ExternalVolume, error) {
	volume, err := client.GetExternalVolume(ctx, &ateapi.GetExternalVolumeRequest{ExternalVolume: ref})
	if err != nil {
		return nil, fmt.Errorf("failed to get shared volume %q: %w", ref.GetName(), err)
	}
	volume.DeleteTrigger = trigger

	resp, err := client.UpdateExternalVolume(ctx, &ateapi.UpdateExternalVolumeRequest{ExternalVolume: volume})
	if err != nil {
		return nil, fmt.Errorf("failed to update shared volume: %w", err)
	}
	return resp, nil
}

func parseDeleteTrigger(value string) (ateapi.DeleteTrigger, error) {
	switch strings.ToLower(value) {
	case "":
		return ateapi.DeleteTrigger_DELETE_TRIGGER_UNSPECIFIED, nil
	case "manual":
		return ateapi.DeleteTrigger_DELETE_TRIGGER_MANUAL, nil
	case "last-actor", "lastactor":
		return ateapi.DeleteTrigger_DELETE_TRIGGER_LAST_ACTOR, nil
	default:
		return ateapi.DeleteTrigger_DELETE_TRIGGER_UNSPECIFIED, fmt.Errorf("invalid delete trigger %q; must be manual or last-actor", value)
	}
}

func init() {
	getExternalVolumesCmd.Flags().StringVarP(&externalVolumeAtespaceFlag, "atespace", "a", "", "Atespace to list/get shared volumes in")
	getExternalVolumesCmd.Flags().BoolVarP(&externalVolumeAllAtespacesFlag, "all-atespaces", "A", false, "List shared volumes across all atespaces")
	getCmd.AddCommand(getExternalVolumesCmd)

	createExternalVolumeCmd.Flags().StringVarP(&createExternalVolumeAtespaceFlag, "atespace", "a", "", "Atespace to create the shared volume in (required)")
	_ = createExternalVolumeCmd.MarkFlagRequired("atespace")
	createExternalVolumeCmd.Flags().StringVar(&createExternalVolumeVolumeIDFlag, "volume-id", "", "Volume ID of the existing volume in the storage system (required)")
	createExternalVolumeCmd.Flags().StringVar(&createExternalVolumeTypeFlag, "volume-type", "", "CSI driver name (required)")
	_ = createExternalVolumeCmd.MarkFlagRequired("volume-id")
	_ = createExternalVolumeCmd.MarkFlagRequired("volume-type")
	createExternalVolumeCmd.Flags().StringToStringVar(&createExternalVolumeContextFlag, "volume-context", nil, "Driver metadata for mounting, as key=value pairs")
	createExternalVolumeCmd.Flags().StringVar(&createExternalVolumeDeleteTriggerFlag, "delete-trigger", "", "What removes the volume: manual (default) or last-actor")
	createCmd.AddCommand(createExternalVolumeCmd)

	updateExternalVolumeCmd.Flags().StringVarP(&updateExternalVolumeAtespaceFlag, "atespace", "a", "", "Atespace owning the shared volume (required)")
	updateExternalVolumeCmd.Flags().StringVar(&updateExternalVolumeDeleteTriggerFlag, "delete-trigger", "", "What removes the volume: manual or last-actor (required)")
	_ = updateExternalVolumeCmd.MarkFlagRequired("atespace")
	_ = updateExternalVolumeCmd.MarkFlagRequired("delete-trigger")
	updateCmd.AddCommand(updateExternalVolumeCmd)

	deleteExternalVolumeCmd.Flags().StringVarP(&deleteExternalVolumeAtespaceFlag, "atespace", "a", "", "Atespace owning the shared volume (required)")
	_ = deleteExternalVolumeCmd.MarkFlagRequired("atespace")
	deleteCmd.AddCommand(deleteExternalVolumeCmd)
}

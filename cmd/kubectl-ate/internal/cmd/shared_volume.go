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
	sharedVolumeAtespaceFlag            string
	sharedVolumeAllAtespacesFlag        bool
	createSharedVolumeAtespaceFlag      string
	createSharedVolumeClassFlag         string
	createSharedVolumeCapacityFlag      string
	createSharedVolumeVolumeIDFlag      string
	createSharedVolumeTypeFlag          string
	createSharedVolumeContextFlag       map[string]string
	createSharedVolumeDeleteTriggerFlag string
	createSharedVolumeAccessModeFlag    string
	updateSharedVolumeAtespaceFlag      string
	updateSharedVolumeDeleteTriggerFlag string
	deleteSharedVolumeAtespaceFlag      string
)

var getSharedVolumesCmd = &cobra.Command{
	Use:     "sharedvolumes [volume-name ...]",
	Aliases: []string{"sharedvolume", "sv"},
	Short:   "List or get shared volumes",
	RunE: func(cmd *cobra.Command, args []string) error {
		if sharedVolumeAllAtespacesFlag && sharedVolumeAtespaceFlag != "" {
			return fmt.Errorf("--atespace and -A/--all-atespaces are mutually exclusive")
		}
		if len(args) > 0 && sharedVolumeAtespaceFlag == "" {
			return fmt.Errorf("--atespace is required when getting shared volumes")
		}
		if len(args) == 0 && !sharedVolumeAllAtespacesFlag && sharedVolumeAtespaceFlag == "" {
			return fmt.Errorf("specify --atespace <name>, or -A/--all-atespaces")
		}

		ctx := cmd.Context()
		client, err := ateclient.NewClient(ctx, kubeconfig, k8sContext, endpoint, tokenFile, traceEnabled)
		if err != nil {
			return fmt.Errorf("failed to connect to ate-api-server: %w", err)
		}
		defer client.Close()

		if len(args) > 0 {
			volumes := make([]*ateapipb.SharedVolume, 0, len(args))
			for _, name := range args {
				volume, err := client.GetSharedVolume(ctx, &ateapipb.GetSharedVolumeRequest{
					SharedVolume: &ateapipb.ObjectRef{Atespace: sharedVolumeAtespaceFlag, Name: name},
				})
				if err != nil {
					return fmt.Errorf("failed to get shared volume %q: %w", name, err)
				}
				volumes = append(volumes, volume)
			}
			if len(volumes) == 1 {
				return printer.PrintSharedVolumeTo(cmd.OutOrStdout(), volumes[0], outputFmt)
			}
			return printer.PrintSharedVolumesTo(cmd.OutOrStdout(), volumes, outputFmt)
		}

		var volumes []*ateapipb.SharedVolume
		pageToken := ""
		for {
			resp, err := client.ListSharedVolumes(ctx, &ateapipb.ListSharedVolumesRequest{
				Atespace: sharedVolumeAtespaceFlag, PageSize: 1000, PageToken: pageToken,
			})
			if err != nil {
				return fmt.Errorf("failed to list shared volumes: %w", err)
			}
			volumes = append(volumes, resp.GetSharedVolumes()...)
			pageToken = resp.GetNextPageToken()
			if pageToken == "" {
				break
			}
		}
		return printer.PrintSharedVolumesTo(cmd.OutOrStdout(), volumes, outputFmt)
	},
}

var createSharedVolumeCmd = &cobra.Command{
	Use:     "sharedvolume <volume-name>",
	Aliases: []string{"sharedvolumes", "sv"},
	Short:   "Create an shared volume actors can share",
	Long: "Create an shared volume actors can share.\n\n" +
		"Name a storage class with --storage-class to provision a new volume, or\n" +
		"supply --volume-id to register a volume that already exists, but not both.",
	Args: cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		trigger, err := parseDeleteTrigger(createSharedVolumeDeleteTriggerFlag)
		if err != nil {
			return err
		}
		mode, err := parseAccessMode(createSharedVolumeAccessModeFlag)
		if err != nil {
			return err
		}
		ctx := cmd.Context()
		client, err := ateclient.NewClient(ctx, kubeconfig, k8sContext, endpoint, tokenFile, traceEnabled)
		if err != nil {
			return fmt.Errorf("failed to connect to ate-api-server: %w", err)
		}
		defer client.Close()

		volume, err := client.CreateSharedVolume(ctx, &ateapipb.CreateSharedVolumeRequest{
			SharedVolume: &ateapipb.SharedVolume{
				Metadata:         &ateapipb.ResourceMetadata{Atespace: createSharedVolumeAtespaceFlag, Name: args[0]},
				DeleteTrigger:    trigger,
				AccessMode:       mode,
				StorageClassName: createSharedVolumeClassFlag,
				Capacity:         createSharedVolumeCapacityFlag,
				VolumeId:         createSharedVolumeVolumeIDFlag,
				VolumeType:       createSharedVolumeTypeFlag,
				VolumeContext:    createSharedVolumeContextFlag,
			},
		})
		if err != nil {
			return fmt.Errorf("failed to create shared volume: %w", err)
		}
		return printer.PrintSharedVolumeTo(cmd.OutOrStdout(), volume, outputFmt)
	},
}

var updateSharedVolumeCmd = &cobra.Command{
	Use:     "sharedvolume <volume-name>",
	Aliases: []string{"sharedvolumes", "sv"},
	Short:   "Change an shared volume's delete trigger",
	Args:    cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		trigger, err := parseDeleteTrigger(updateSharedVolumeDeleteTriggerFlag)
		if err != nil {
			return err
		}
		ctx := cmd.Context()
		client, err := ateclient.NewClient(ctx, kubeconfig, k8sContext, endpoint, tokenFile, traceEnabled)
		if err != nil {
			return fmt.Errorf("failed to connect to ate-api-server: %w", err)
		}
		defer client.Close()

		ref := &ateapipb.ObjectRef{Atespace: updateSharedVolumeAtespaceFlag, Name: args[0]}
		resp, err := updateSharedVolumeDeleteTrigger(ctx, client, ref, trigger)
		if err != nil {
			return err
		}
		return printer.PrintSharedVolumeTo(cmd.OutOrStdout(), resp, outputFmt)
	},
}

var deleteSharedVolumeCmd = &cobra.Command{
	Use:     "sharedvolume <volume-name>",
	Aliases: []string{"sharedvolumes", "sv"},
	Short:   "Delete an shared volume and its storage",
	Args:    cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := cmd.Context()
		client, err := ateclient.NewClient(ctx, kubeconfig, k8sContext, endpoint, tokenFile, traceEnabled)
		if err != nil {
			return fmt.Errorf("failed to connect to ate-api-server: %w", err)
		}
		defer client.Close()

		_, err = client.DeleteSharedVolume(ctx, &ateapipb.DeleteSharedVolumeRequest{
			SharedVolume: &ateapipb.ObjectRef{Atespace: deleteSharedVolumeAtespaceFlag, Name: args[0]},
		})
		if err != nil {
			return fmt.Errorf("failed to delete shared volume: %w", err)
		}
		fmt.Fprintf(cmd.OutOrStdout(), "shared volume %q deleted\n", args[0])
		return nil
	},
}

type sharedVolumeClient interface {
	GetSharedVolume(ctx context.Context, in *ateapipb.GetSharedVolumeRequest, opts ...grpc.CallOption) (*ateapipb.SharedVolume, error)
	UpdateSharedVolume(ctx context.Context, in *ateapipb.UpdateSharedVolumeRequest, opts ...grpc.CallOption) (*ateapipb.SharedVolume, error)
}

// updateSharedVolumeDeleteTrigger reads the volume first so the update carries
// the uid and version the server requires as preconditions.
func updateSharedVolumeDeleteTrigger(ctx context.Context, client sharedVolumeClient, ref *ateapipb.ObjectRef, trigger ateapipb.DeleteTrigger) (*ateapipb.SharedVolume, error) {
	volume, err := client.GetSharedVolume(ctx, &ateapipb.GetSharedVolumeRequest{SharedVolume: ref})
	if err != nil {
		return nil, fmt.Errorf("failed to get shared volume %q: %w", ref.GetName(), err)
	}
	volume.DeleteTrigger = trigger

	resp, err := client.UpdateSharedVolume(ctx, &ateapipb.UpdateSharedVolumeRequest{SharedVolume: volume})
	if err != nil {
		return nil, fmt.Errorf("failed to update shared volume: %w", err)
	}
	return resp, nil
}

func parseDeleteTrigger(value string) (ateapipb.DeleteTrigger, error) {
	switch strings.ToLower(value) {
	case "":
		return ateapipb.DeleteTrigger_DELETE_TRIGGER_UNSPECIFIED, nil
	case "manual":
		return ateapipb.DeleteTrigger_DELETE_TRIGGER_MANUAL, nil
	case "last-actor", "lastactor":
		return ateapipb.DeleteTrigger_DELETE_TRIGGER_LAST_ACTOR, nil
	default:
		return ateapipb.DeleteTrigger_DELETE_TRIGGER_UNSPECIFIED, fmt.Errorf("invalid delete trigger %q; must be manual or last-actor", value)
	}
}

func parseAccessMode(value string) (ateapipb.AccessMode, error) {
	switch strings.ToLower(value) {
	case "":
		return ateapipb.AccessMode_ACCESS_MODE_UNSPECIFIED, nil
	case "rwo", "readwriteonce":
		return ateapipb.AccessMode_ACCESS_MODE_READ_WRITE_ONCE, nil
	case "rwx", "readwritemany":
		return ateapipb.AccessMode_ACCESS_MODE_READ_WRITE_MANY, nil
	default:
		return ateapipb.AccessMode_ACCESS_MODE_UNSPECIFIED, fmt.Errorf("invalid access mode %q; must be rwo or rwx", value)
	}
}

func init() {
	getSharedVolumesCmd.Flags().StringVarP(&sharedVolumeAtespaceFlag, "atespace", "a", "", "Atespace to list/get shared volumes in")
	getSharedVolumesCmd.Flags().BoolVarP(&sharedVolumeAllAtespacesFlag, "all-atespaces", "A", false, "List shared volumes across all atespaces")
	getCmd.AddCommand(getSharedVolumesCmd)

	createSharedVolumeCmd.Flags().StringVarP(&createSharedVolumeAtespaceFlag, "atespace", "a", "", "Atespace to create the shared volume in (required)")
	_ = createSharedVolumeCmd.MarkFlagRequired("atespace")
	createSharedVolumeCmd.Flags().StringVar(&createSharedVolumeClassFlag, "storage-class", "", "StorageClass to provision the volume from")
	createSharedVolumeCmd.Flags().StringVar(&createSharedVolumeCapacityFlag, "capacity", "", "Size to provision, as a Kubernetes quantity (e.g. 10Gi); required with --storage-class")
	createSharedVolumeCmd.Flags().StringVar(&createSharedVolumeVolumeIDFlag, "volume-id", "", "Volume ID of a volume that already exists, to register instead of provisioning")
	createSharedVolumeCmd.Flags().StringVar(&createSharedVolumeTypeFlag, "volume-type", "", "CSI driver name; required with --volume-id unless --storage-class is set")
	createSharedVolumeCmd.Flags().StringToStringVar(&createSharedVolumeContextFlag, "volume-context", nil, "Driver metadata for mounting, as key=value pairs; only valid with --volume-id")
	createSharedVolumeCmd.Flags().StringVar(&createSharedVolumeDeleteTriggerFlag, "delete-trigger", "", "What removes the volume: manual or last-actor (default: last-actor when provisioned, manual when registered)")
	createSharedVolumeCmd.Flags().StringVar(&createSharedVolumeAccessModeFlag, "access-mode", "", "How many resumed actors may hold the volume: rwo (default) or rwx")
	createCmd.AddCommand(createSharedVolumeCmd)

	updateSharedVolumeCmd.Flags().StringVarP(&updateSharedVolumeAtespaceFlag, "atespace", "a", "", "Atespace owning the shared volume (required)")
	updateSharedVolumeCmd.Flags().StringVar(&updateSharedVolumeDeleteTriggerFlag, "delete-trigger", "", "What removes the volume: manual or last-actor (required)")
	_ = updateSharedVolumeCmd.MarkFlagRequired("atespace")
	_ = updateSharedVolumeCmd.MarkFlagRequired("delete-trigger")
	updateCmd.AddCommand(updateSharedVolumeCmd)

	deleteSharedVolumeCmd.Flags().StringVarP(&deleteSharedVolumeAtespaceFlag, "atespace", "a", "", "Atespace owning the shared volume (required)")
	_ = deleteSharedVolumeCmd.MarkFlagRequired("atespace")
	deleteCmd.AddCommand(deleteSharedVolumeCmd)
}

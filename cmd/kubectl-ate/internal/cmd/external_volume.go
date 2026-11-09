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

	"github.com/spf13/cobra"
	"go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/cmd/kubectl-ate/internal/printer"
)

var (
	getExternalVolumeAtespaceFlag      string
	getExternalVolumeAllAtespacesFlag  bool
	createExternalVolumeAtespaceFlag   string
	createExternalVolumeVolumeIDFlag   string
	createExternalVolumeVolumeTypeFlag string
	createExternalVolumeContextFlag    map[string]string
	deleteExternalVolumeAtespaceFlag   string
)

var getExternalVolumesCmd = &cobra.Command{
	Use:     "externalvolume [volume-name ...]",
	Aliases: []string{"externalvolumes", "external-volume", "external-volumes"},
	Short:   "List or get external volumes",
	Args:    cobra.ArbitraryArgs,
	RunE: func(cmd *cobra.Command, args []string) error {
		if err := validateAtespaceFlags("external volumes", args, getExternalVolumeAtespaceFlag, getExternalVolumeAllAtespacesFlag); err != nil {
			return err
		}

		ctx := cmd.Context()
		client, err := newAPIClient(cmd)
		if err != nil {
			return fmt.Errorf("failed to connect to ate-api-server: %w", err)
		}
		defer client.Close()

		if len(args) > 0 {
			volumes := make([]*ateapi.ExternalVolume, 0, len(args))
			for _, name := range args {
				volume, err := client.GetExternalVolume(ctx, &ateapi.GetExternalVolumeRequest{
					ExternalVolume: &ateapi.ObjectRef{Atespace: getExternalVolumeAtespaceFlag, Name: name},
				})
				if err != nil {
					return fmt.Errorf("failed to get external volume %q: %w", name, err)
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
				Atespace: getExternalVolumeAtespaceFlag,
				Options:  &ateapi.ListOptions{PageSize: 1000, PageToken: pageToken},
			})
			if err != nil {
				return fmt.Errorf("failed to list external volumes: %w", err)
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
	Aliases: []string{"external-volume"},
	Short:   "Register a volume that already exists in the storage system",
	Long: "Register a volume that already exists in the storage system as an\n" +
		"ExternalVolume, named by the CSI driver (--volume-type) and the volume ID\n" +
		"the driver reports (--volume-id). Requires the ExternalVolumes preview\n" +
		"gate and --preview.",
	Args: exactlyOneArg("<volume-name>", "atespace", "volume-id", "volume-type"),
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := cmd.Context()
		client, err := newAPIClient(cmd)
		if err != nil {
			return fmt.Errorf("failed to connect to ate-api-server: %w", err)
		}
		defer client.Close()

		volume, err := client.CreateExternalVolume(ctx, &ateapi.CreateExternalVolumeRequest{
			ExternalVolume: &ateapi.ExternalVolume{
				Metadata:      &ateapi.ResourceMetadata{Atespace: createExternalVolumeAtespaceFlag, Name: args[0]},
				VolumeId:      createExternalVolumeVolumeIDFlag,
				VolumeType:    createExternalVolumeVolumeTypeFlag,
				VolumeContext: createExternalVolumeContextFlag,
			},
		})
		if err != nil {
			return fmt.Errorf("failed to create external volume: %w", err)
		}
		return printer.PrintExternalVolumeTo(cmd.OutOrStdout(), volume, outputFmt)
	},
}

var deleteExternalVolumeCmd = &cobra.Command{
	Use:     "externalvolume <volume-name>",
	Aliases: []string{"external-volume"},
	Short:   "Delete an external volume record, leaving its storage in place",
	Args:    exactlyOneArg("<volume-name>", "atespace"),
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := cmd.Context()
		client, err := newAPIClient(cmd)
		if err != nil {
			return fmt.Errorf("failed to connect to ate-api-server: %w", err)
		}
		defer client.Close()

		_, err = client.DeleteExternalVolume(ctx, &ateapi.DeleteExternalVolumeRequest{
			ExternalVolume: &ateapi.ObjectRef{Atespace: deleteExternalVolumeAtespaceFlag, Name: args[0]},
		})
		if err != nil {
			return fmt.Errorf("failed to delete external volume: %w", err)
		}
		fmt.Fprintf(cmd.OutOrStdout(), "external volume %q deleted\n", args[0])
		return nil
	},
}

func init() {
	getExternalVolumesCmd.Flags().StringVarP(&getExternalVolumeAtespaceFlag, "atespace", "a", "", "get external volumes in a specific atespace (mutually exclusive with --all-atespaces)")
	getExternalVolumesCmd.Flags().BoolVarP(&getExternalVolumeAllAtespacesFlag, "all-atespaces", "A", false, "get external volumes across all atespaces (mutually exclusive with --atespace)")
	getCmd.AddCommand(getExternalVolumesCmd)

	createExternalVolumeCmd.Flags().StringVarP(&createExternalVolumeAtespaceFlag, "atespace", "a", "", "the atespace to register the volume in")
	createExternalVolumeCmd.Flags().StringVar(&createExternalVolumeVolumeIDFlag, "volume-id", "", "the volume's ID in the storage system, as the CSI driver reports it")
	createExternalVolumeCmd.Flags().StringVar(&createExternalVolumeVolumeTypeFlag, "volume-type", "", "the CSI driver name")
	createExternalVolumeCmd.Flags().StringToStringVar(&createExternalVolumeContextFlag, "volume-context", nil, "driver metadata the node plugin needs to mount the volume, as key=value pairs; wrap a pair whose value contains a comma in double quotes, as in '\"mountOptions=nfsvers=4.1,hard\"'")
	createCmd.AddCommand(createExternalVolumeCmd)

	deleteExternalVolumeCmd.Flags().StringVarP(&deleteExternalVolumeAtespaceFlag, "atespace", "a", "", "the external volume's atespace")
	deleteCmd.AddCommand(deleteExternalVolumeCmd)
}

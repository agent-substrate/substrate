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
	"io"

	"github.com/spf13/cobra"
	"go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/cmd/kubectl-ate/internal/printer"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/encoding/protojson"
)

var (
	createCsiDriverConfigFilenameFlag string
	updateCsiDriverConfigFilenameFlag string
	updateCsiDriverConfigGuard        deleteGuardFlags
	deleteCsiDriverConfigGuard        deleteGuardFlags
)

type csiDriverConfigClient interface {
	GetCsiDriverConfig(ctx context.Context, in *ateapi.GetCsiDriverConfigRequest, opts ...grpc.CallOption) (*ateapi.CsiDriverConfig, error)
	ListCsiDriverConfigs(ctx context.Context, in *ateapi.ListCsiDriverConfigsRequest, opts ...grpc.CallOption) (*ateapi.ListCsiDriverConfigsResponse, error)
	CreateCsiDriverConfig(ctx context.Context, in *ateapi.CreateCsiDriverConfigRequest, opts ...grpc.CallOption) (*ateapi.CsiDriverConfig, error)
	UpdateCsiDriverConfig(ctx context.Context, in *ateapi.UpdateCsiDriverConfigRequest, opts ...grpc.CallOption) (*ateapi.CsiDriverConfig, error)
	DeleteCsiDriverConfig(ctx context.Context, in *ateapi.DeleteCsiDriverConfigRequest, opts ...grpc.CallOption) (*ateapi.CsiDriverConfig, error)
}

var getCsiDriverConfigsCmd = &cobra.Command{
	Use:     "csi-driver-config [name ...]",
	Aliases: []string{"csi-driver-configs"},
	Short:   "List all CSI driver configs or get one or more CSI driver configs",
	Long:    "List or get CSI driver configs.",
	Args:    cobra.ArbitraryArgs,
	RunE: func(cmd *cobra.Command, args []string) error {
		apiClient, err := newAPIClient(cmd)
		if err != nil {
			return fmt.Errorf("failed to connect to ate-api-server: %w", err)
		}
		defer apiClient.Close()
		return runGetCsiDriverConfigs(cmd.Context(), apiClient, cmd.OutOrStdout(), outputFmt, args)
	},
}

var createCsiDriverConfigCmd = &cobra.Command{
	Use:     "csi-driver-config -f <manifest>",
	Aliases: []string{"csi-driver-configs"},
	Short:   "Create a CSI driver config from a manifest",
	Long:    `Create a CSI driver config from a single YAML or JSON manifest holding one ateapi.CsiDriverConfig message in protojson form (as printed by "kubectl ate get csi-driver-config <name> -o yaml"). CSI driver configs are global-scoped.`,
	Args:    noArgs("the CSI driver config name comes from the manifest passed to -f/--filename", "filename"),
	RunE: func(cmd *cobra.Command, args []string) error {
		config, err := loadCsiDriverConfigManifest(cmd.InOrStdin(), createCsiDriverConfigFilenameFlag)
		if err != nil {
			return err
		}
		apiClient, err := newAPIClient(cmd)
		if err != nil {
			return fmt.Errorf("failed to connect to ate-api-server: %w", err)
		}
		defer apiClient.Close()
		return runCreateCsiDriverConfig(cmd.Context(), apiClient, cmd.OutOrStdout(), outputFmt, config)
	},
}

var updateCsiDriverConfigCmd = &cobra.Command{
	Use:     "csi-driver-config -f <manifest>",
	Aliases: []string{"csi-driver-configs"},
	Short:   "Replace a CSI driver config from a manifest",
	Long:    `Replace a CSI driver config from a single YAML or JSON manifest holding one ateapi.CsiDriverConfig message in protojson form (as printed by "kubectl ate get csi-driver-config <name> -o yaml"). metadata.uid and metadata.version (or --uid and --version) are required preconditions.`,
	Args:    noArgs("the CSI driver config name comes from the manifest passed to -f/--filename", "filename"),
	RunE: func(cmd *cobra.Command, args []string) error {
		config, err := prepareCsiDriverConfigUpdate(cmd.InOrStdin(), updateCsiDriverConfigFilenameFlag, deleteGuardFlags{
			uidSet:     cmd.Flags().Changed("uid"),
			uid:        updateCsiDriverConfigGuard.uid,
			versionSet: cmd.Flags().Changed("version"),
			version:    updateCsiDriverConfigGuard.version,
		})
		if err != nil {
			return err
		}
		apiClient, err := newAPIClient(cmd)
		if err != nil {
			return fmt.Errorf("failed to connect to ate-api-server: %w", err)
		}
		defer apiClient.Close()
		return runUpdateCsiDriverConfig(cmd.Context(), apiClient, cmd.OutOrStdout(), outputFmt, config)
	},
}

var deleteCsiDriverConfigCmd = &cobra.Command{
	Use:     "csi-driver-config <csi-driver-config-name>",
	Aliases: []string{"csi-driver-configs"},
	Short:   "Delete a CSI driver config",
	Long:    "Delete a CSI driver config. Pass --uid and/or --version to guard the delete against a concurrent replacement.",
	Args:    exactlyOneArg("<csi-driver-config-name>"),
	RunE: func(cmd *cobra.Command, args []string) error {
		opts, err := deleteOptionsFromFlags(deleteGuardFlags{
			uidSet:     cmd.Flags().Changed("uid"),
			uid:        deleteCsiDriverConfigGuard.uid,
			versionSet: cmd.Flags().Changed("version"),
			version:    deleteCsiDriverConfigGuard.version,
		})
		if err != nil {
			return wrapUsageError(err)
		}
		apiClient, err := newAPIClient(cmd)
		if err != nil {
			return fmt.Errorf("failed to connect to ate-api-server: %w", err)
		}
		defer apiClient.Close()
		return runDeleteCsiDriverConfig(cmd.Context(), apiClient, cmd.OutOrStdout(), args[0], opts)
	},
}

func runGetCsiDriverConfigs(ctx context.Context, client csiDriverConfigClient, out io.Writer, format string, names []string) error {
	if len(names) > 0 {
		configs := make([]*ateapi.CsiDriverConfig, 0, len(names))
		for _, name := range names {
			resp, err := client.GetCsiDriverConfig(ctx, &ateapi.GetCsiDriverConfigRequest{CsiDriverConfig: &ateapi.ObjectRef{Name: name}})
			if err != nil {
				return fmt.Errorf("failed to get csi driver config %q: %w", name, err)
			}
			configs = append(configs, resp)
		}
		if len(configs) == 1 {
			return printer.PrintCsiDriverConfigTo(out, configs[0], format)
		}
		return printer.PrintCsiDriverConfigsTo(out, configs, format)
	}
	var allConfigs []*ateapi.CsiDriverConfig
	for pageToken := ""; ; {
		resp, err := client.ListCsiDriverConfigs(ctx, &ateapi.ListCsiDriverConfigsRequest{Options: &ateapi.ListOptions{PageSize: 1000, PageToken: pageToken}})
		if err != nil {
			return fmt.Errorf("failed to list csi driver configs: %w", err)
		}
		allConfigs = append(allConfigs, resp.GetCsiDriverConfigs()...)
		if pageToken = resp.GetNextPageToken(); pageToken == "" {
			break
		}
	}
	return printer.PrintCsiDriverConfigsTo(out, allConfigs, format)
}

func runCreateCsiDriverConfig(ctx context.Context, client csiDriverConfigClient, out io.Writer, format string, config *ateapi.CsiDriverConfig) error {
	resp, err := client.CreateCsiDriverConfig(ctx, &ateapi.CreateCsiDriverConfigRequest{CsiDriverConfig: config})
	if err != nil {
		return fmt.Errorf("failed to create csi driver config: %w", err)
	}
	return printer.PrintCsiDriverConfigTo(out, resp, format)
}

func runUpdateCsiDriverConfig(ctx context.Context, client csiDriverConfigClient, out io.Writer, format string, config *ateapi.CsiDriverConfig) error {
	resp, err := client.UpdateCsiDriverConfig(ctx, &ateapi.UpdateCsiDriverConfigRequest{CsiDriverConfig: config})
	if err != nil {
		return fmt.Errorf("failed to update csi driver config: %w", err)
	}
	return printer.PrintCsiDriverConfigTo(out, resp, format)
}

func runDeleteCsiDriverConfig(ctx context.Context, client csiDriverConfigClient, out io.Writer, name string, opts *ateapi.DeleteOptions) error {
	if _, err := client.DeleteCsiDriverConfig(ctx, &ateapi.DeleteCsiDriverConfigRequest{CsiDriverConfig: &ateapi.ObjectRef{Name: name}, Options: opts}); err != nil {
		return fmt.Errorf("failed to delete csi driver config: %w", err)
	}
	fmt.Fprintf(out, "csi driver config %q deleted\n", name)
	return nil
}

func loadCsiDriverConfigManifest(in io.Reader, path string) (*ateapi.CsiDriverConfig, error) {
	data, err := readFileOrStdin(in, path)
	if err != nil {
		return nil, err
	}
	jsonData, err := manifestToJSON(data)
	if err != nil {
		return nil, fmt.Errorf("failed to parse csi driver config manifest %q: %w", path, err)
	}
	config := &ateapi.CsiDriverConfig{}
	if err := protojson.Unmarshal(jsonData, config); err != nil {
		return nil, fmt.Errorf("failed to parse csi driver config manifest %q: invalid CsiDriverConfig: %w", path, err)
	}
	if got := config.GetMetadata().GetAtespace(); got != "" {
		return nil, fmt.Errorf("csi driver config is global-scoped: metadata.atespace must be empty (got %q)", got)
	}
	if config.GetMetadata().GetName() == "" {
		return nil, fmt.Errorf("failed to parse csi driver config manifest %q: metadata.name is required", path)
	}
	return config, nil
}

func prepareCsiDriverConfigUpdate(in io.Reader, path string, guard deleteGuardFlags) (*ateapi.CsiDriverConfig, error) {
	if _, err := deleteOptionsFromFlags(guard); err != nil {
		return nil, wrapUsageError(err)
	}
	config, err := loadCsiDriverConfigManifest(in, path)
	if err != nil {
		return nil, err
	}
	if guard.uidSet {
		config.Metadata.Uid = guard.uid
	}
	if guard.versionSet {
		config.Metadata.Version = guard.version
	}
	if config.GetMetadata().GetUid() == "" || config.GetMetadata().GetVersion() < 1 {
		name := config.GetMetadata().GetName()
		return nil, fmt.Errorf(`manifest for csi driver config %q lacks the metadata.uid and metadata.version preconditions that "kubectl ate get csi-driver-config %s -o yaml" prints (or pass --uid and --version)`, name, name)
	}
	return config, nil
}

func init() {
	getCmd.AddCommand(getCsiDriverConfigsCmd)

	createCsiDriverConfigCmd.Flags().StringVarP(&createCsiDriverConfigFilenameFlag, "filename", "f", "", "manifest file holding a single JSON or YAML csi-driver-config document, or - for stdin")
	createCmd.AddCommand(createCsiDriverConfigCmd)

	updateCsiDriverConfigCmd.Flags().StringVarP(&updateCsiDriverConfigFilenameFlag, "filename", "f", "", "manifest file holding a single JSON or YAML csi-driver-config document, or - for stdin")
	updateCsiDriverConfigCmd.Flags().StringVar(&updateCsiDriverConfigGuard.uid, "uid", "", "require the CSI driver config to have this UID before updating (overrides metadata.uid)")
	updateCsiDriverConfigCmd.Flags().Int64Var(&updateCsiDriverConfigGuard.version, "version", 0, "require the CSI driver config to be at this version before updating (overrides metadata.version)")
	updateCmd.AddCommand(updateCsiDriverConfigCmd)

	deleteCsiDriverConfigCmd.Flags().StringVar(&deleteCsiDriverConfigGuard.uid, "uid", "", "require the CSI driver config to have this UID before deleting")
	deleteCsiDriverConfigCmd.Flags().Int64Var(&deleteCsiDriverConfigGuard.version, "version", 0, "require the CSI driver config to be at this version before deleting")
	deleteCmd.AddCommand(deleteCsiDriverConfigCmd)
}

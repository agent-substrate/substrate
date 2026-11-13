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
	createExternalVolumeClassFilenameFlag string
	deleteExternalVolumeClassGuard        deleteGuardFlags
)

type externalVolumeClassClient interface {
	GetExternalVolumeClass(ctx context.Context, in *ateapi.GetExternalVolumeClassRequest, opts ...grpc.CallOption) (*ateapi.ExternalVolumeClass, error)
	ListExternalVolumeClasses(ctx context.Context, in *ateapi.ListExternalVolumeClassesRequest, opts ...grpc.CallOption) (*ateapi.ListExternalVolumeClassesResponse, error)
	CreateExternalVolumeClass(ctx context.Context, in *ateapi.CreateExternalVolumeClassRequest, opts ...grpc.CallOption) (*ateapi.ExternalVolumeClass, error)
	DeleteExternalVolumeClass(ctx context.Context, in *ateapi.DeleteExternalVolumeClassRequest, opts ...grpc.CallOption) (*ateapi.ExternalVolumeClass, error)
}

var getExternalVolumeClassesCmd = &cobra.Command{
	Use:     "external-volume-class [name ...]",
	Aliases: []string{"external-volume-classes"},
	Short:   "List all external volume classes or get one or more external volume classes",
	Long:    "List or get external volume classes.",
	Args:    cobra.ArbitraryArgs,
	RunE: func(cmd *cobra.Command, args []string) error {
		apiClient, err := newAPIClient(cmd)
		if err != nil {
			return fmt.Errorf("failed to connect to ate-api-server: %w", err)
		}
		defer apiClient.Close()
		return runGetExternalVolumeClasses(cmd.Context(), apiClient, cmd.OutOrStdout(), outputFmt, args)
	},
}

var createExternalVolumeClassCmd = &cobra.Command{
	Use:     "external-volume-class -f <manifest>",
	Aliases: []string{"external-volume-classes"},
	Short:   "Create an external volume class from a manifest",
	Long:    `Create an external volume class from a single YAML or JSON manifest holding one ateapi.ExternalVolumeClass message in protojson form (as printed by "kubectl ate get external-volume-class <name> -o yaml"). External volume classes are global-scoped and immutable.`,
	Args:    noArgs("the external volume class name comes from the manifest passed to -f/--filename", "filename"),
	RunE: func(cmd *cobra.Command, args []string) error {
		class, err := loadExternalVolumeClassManifest(cmd.InOrStdin(), createExternalVolumeClassFilenameFlag)
		if err != nil {
			return err
		}
		apiClient, err := newAPIClient(cmd)
		if err != nil {
			return fmt.Errorf("failed to connect to ate-api-server: %w", err)
		}
		defer apiClient.Close()
		return runCreateExternalVolumeClass(cmd.Context(), apiClient, cmd.OutOrStdout(), outputFmt, class)
	},
}

var deleteExternalVolumeClassCmd = &cobra.Command{
	Use:     "external-volume-class <external-volume-class-name>",
	Aliases: []string{"external-volume-classes"},
	Short:   "Delete an external volume class",
	Long:    "Delete an external volume class. Pass --uid and/or --version to guard the delete against a concurrent replacement.",
	Args:    exactlyOneArg("<external-volume-class-name>"),
	RunE: func(cmd *cobra.Command, args []string) error {
		opts, err := deleteOptionsFromFlags(deleteGuardFlags{
			uidSet:     cmd.Flags().Changed("uid"),
			uid:        deleteExternalVolumeClassGuard.uid,
			versionSet: cmd.Flags().Changed("version"),
			version:    deleteExternalVolumeClassGuard.version,
		})
		if err != nil {
			return wrapUsageError(err)
		}
		apiClient, err := newAPIClient(cmd)
		if err != nil {
			return fmt.Errorf("failed to connect to ate-api-server: %w", err)
		}
		defer apiClient.Close()
		return runDeleteExternalVolumeClass(cmd.Context(), apiClient, cmd.OutOrStdout(), args[0], opts)
	},
}

func runGetExternalVolumeClasses(ctx context.Context, client externalVolumeClassClient, out io.Writer, format string, names []string) error {
	if len(names) > 0 {
		classes := make([]*ateapi.ExternalVolumeClass, 0, len(names))
		for _, name := range names {
			resp, err := client.GetExternalVolumeClass(ctx, &ateapi.GetExternalVolumeClassRequest{ExternalVolumeClass: &ateapi.ObjectRef{Name: name}})
			if err != nil {
				return fmt.Errorf("failed to get external volume class %q: %w", name, err)
			}
			classes = append(classes, resp)
		}
		if len(classes) == 1 {
			return printer.PrintExternalVolumeClassTo(out, classes[0], format)
		}
		return printer.PrintExternalVolumeClassesTo(out, classes, format)
	}
	var allClasses []*ateapi.ExternalVolumeClass
	for pageToken := ""; ; {
		resp, err := client.ListExternalVolumeClasses(ctx, &ateapi.ListExternalVolumeClassesRequest{Options: &ateapi.ListOptions{PageSize: 1000, PageToken: pageToken}})
		if err != nil {
			return fmt.Errorf("failed to list external volume classes: %w", err)
		}
		allClasses = append(allClasses, resp.GetExternalVolumeClasses()...)
		if pageToken = resp.GetNextPageToken(); pageToken == "" {
			break
		}
	}
	return printer.PrintExternalVolumeClassesTo(out, allClasses, format)
}

func runCreateExternalVolumeClass(ctx context.Context, client externalVolumeClassClient, out io.Writer, format string, class *ateapi.ExternalVolumeClass) error {
	resp, err := client.CreateExternalVolumeClass(ctx, &ateapi.CreateExternalVolumeClassRequest{ExternalVolumeClass: class})
	if err != nil {
		return fmt.Errorf("failed to create external volume class: %w", err)
	}
	return printer.PrintExternalVolumeClassTo(out, resp, format)
}

func runDeleteExternalVolumeClass(ctx context.Context, client externalVolumeClassClient, out io.Writer, name string, opts *ateapi.DeleteOptions) error {
	if _, err := client.DeleteExternalVolumeClass(ctx, &ateapi.DeleteExternalVolumeClassRequest{ExternalVolumeClass: &ateapi.ObjectRef{Name: name}, Options: opts}); err != nil {
		return fmt.Errorf("failed to delete external volume class: %w", err)
	}
	fmt.Fprintf(out, "external volume class %q deleted\n", name)
	return nil
}

func loadExternalVolumeClassManifest(in io.Reader, path string) (*ateapi.ExternalVolumeClass, error) {
	data, err := readFileOrStdin(in, path)
	if err != nil {
		return nil, err
	}
	jsonData, err := manifestToJSON(data)
	if err != nil {
		return nil, fmt.Errorf("failed to parse external volume class manifest %q: %w", path, err)
	}
	class := &ateapi.ExternalVolumeClass{}
	if err := protojson.Unmarshal(jsonData, class); err != nil {
		return nil, fmt.Errorf("failed to parse external volume class manifest %q: invalid ExternalVolumeClass: %w", path, err)
	}
	if got := class.GetMetadata().GetAtespace(); got != "" {
		return nil, fmt.Errorf("external volume class is global-scoped: metadata.atespace must be empty (got %q)", got)
	}
	if class.GetMetadata().GetName() == "" {
		return nil, fmt.Errorf("failed to parse external volume class manifest %q: metadata.name is required", path)
	}
	return class, nil
}

func init() {
	getCmd.AddCommand(getExternalVolumeClassesCmd)
	createExternalVolumeClassCmd.Flags().StringVarP(&createExternalVolumeClassFilenameFlag, "filename", "f", "", "manifest file holding a single JSON or YAML external-volume-class document, or - for stdin")
	createCmd.AddCommand(createExternalVolumeClassCmd)
	deleteExternalVolumeClassCmd.Flags().StringVar(&deleteExternalVolumeClassGuard.uid, "uid", "", "require the external volume class to have this UID before deleting")
	deleteExternalVolumeClassCmd.Flags().Int64Var(&deleteExternalVolumeClassGuard.version, "version", 0, "require the external volume class to be at this version before deleting")
	deleteCmd.AddCommand(deleteExternalVolumeClassCmd)
}

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
	"fmt"
	"io"
	"os"

	"github.com/agent-substrate/substrate/cmd/kubectl-ate/internal/printer"
	"github.com/agent-substrate/substrate/internal/ateclient"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/spf13/cobra"
	"google.golang.org/protobuf/encoding/protojson"
	"sigs.k8s.io/yaml"
)

var (
	getActorTemplateAtespaceFlag     string
	getActorTemplateAllAtespacesFlag bool
	createActorTemplateFilenameFlag  string
	deleteActorTemplateAtespaceFlag  string
)

var getActorTemplatesCmd = &cobra.Command{
	Use:     "actor-template <template-name ...>",
	Aliases: []string{"actor-templates"},
	Short:   "List all actor templates or get one or more actor templates",
	Long:    "List or get actor templates.",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := cmd.Context()

		apiClient, err := ateclient.NewClient(ctx, kubeconfig, k8sContext, endpoint, tokenFile, traceEnabled)
		if err != nil {
			return fmt.Errorf("failed to connect to ate-api-server: %w", err)
		}
		defer apiClient.Close()

		if len(args) > 0 {
			// A template is addressed by (atespace, name), so the atespace is
			// mandatory and "all atespaces" is meaningless here.
			if getActorTemplateAllAtespacesFlag {
				return fmt.Errorf("-A/--all-atespaces cannot be used when getting actor templates; pass --atespace")
			}
			if getActorTemplateAtespaceFlag == "" {
				return fmt.Errorf("--atespace is required when getting actor templates")
			}

			templates := make([]*ateapipb.ActorTemplate, 0, len(args))
			for _, name := range args {
				resp, err := apiClient.GetActorTemplate(ctx, &ateapipb.GetActorTemplateRequest{
					ActorTemplate: &ateapipb.ObjectRef{Atespace: getActorTemplateAtespaceFlag, Name: name},
				})
				if err != nil {
					return fmt.Errorf("failed to get actor template %q: %w", name, err)
				}
				templates = append(templates, resp)
			}
			if len(templates) == 1 {
				return printer.PrintActorTemplateTo(cmd.OutOrStdout(), templates[0], outputFmt)
			}
			return printer.PrintActorTemplatesTo(cmd.OutOrStdout(), templates, outputFmt)
		}

		// Listing requires exactly one of --atespace (one atespace) or -A (all
		// atespaces). There is no default atespace to fall back on.
		if getActorTemplateAllAtespacesFlag && getActorTemplateAtespaceFlag != "" {
			return fmt.Errorf("--atespace and -A/--all-atespaces are mutually exclusive")
		}
		if !getActorTemplateAllAtespacesFlag && getActorTemplateAtespaceFlag == "" {
			return fmt.Errorf("specify --atespace <name> to list one atespace, or -A/--all-atespaces for all")
		}

		var allTemplates []*ateapipb.ActorTemplate
		pageToken := ""
		for {
			resp, err := apiClient.ListActorTemplates(ctx, &ateapipb.ListActorTemplatesRequest{
				PageSize:  100,
				PageToken: pageToken,
				Atespace:  getActorTemplateAtespaceFlag,
			})
			if err != nil {
				return fmt.Errorf("failed to list actor templates: %w", err)
			}
			allTemplates = append(allTemplates, resp.GetActorTemplates()...)

			pageToken = resp.GetNextPageToken()
			if pageToken == "" {
				break
			}
		}

		return printer.PrintActorTemplatesTo(cmd.OutOrStdout(), allTemplates, outputFmt)
	},
}

var createActorTemplateCmd = &cobra.Command{
	Use:   "actor-template -f <manifest>",
	Short: "Create an actor template from a manifest",
	Long: `Create an actor template from a manifest file.

The manifest is a single YAML (or JSON) document holding one ateapipb.ActorTemplate
message in its protojson form, exactly as printed by
"kubectl ate get actor-template <name> -a <atespace> -o yaml".
The template's atespace and name come from the manifest's metadata.
The atespace must already exist.

Actor templates are immutable: there is no update; delete and recreate to change one.`,
	Args: cobra.NoArgs,
	RunE: func(cmd *cobra.Command, args []string) error {
		data, err := readFileOrStdin(cmd.InOrStdin(), createActorTemplateFilenameFlag)
		if err != nil {
			return err
		}
		template, err := actorTemplateFromManifest(data)
		if err != nil {
			return fmt.Errorf("failed to parse actor template manifest %q: %w", createActorTemplateFilenameFlag, err)
		}

		ctx := cmd.Context()
		apiClient, err := ateclient.NewClient(ctx, kubeconfig, k8sContext, endpoint, tokenFile, traceEnabled)
		if err != nil {
			return fmt.Errorf("failed to connect to ate-api-server: %w", err)
		}
		defer apiClient.Close()

		resp, err := apiClient.CreateActorTemplate(ctx, &ateapipb.CreateActorTemplateRequest{ActorTemplate: template})
		if err != nil {
			return fmt.Errorf("failed to create actor template: %w", err)
		}
		return printer.PrintActorTemplateTo(cmd.OutOrStdout(), resp, outputFmt)
	},
}

var deleteActorTemplateCmd = &cobra.Command{
	Use:   "actor-template <template-name>",
	Short: "Delete an actor template",
	Long: `Delete an actor template.

The server also deletes the template's golden actor and golden snapshot.`,
	Args: cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := cmd.Context()
		c, err := ateclient.NewClient(ctx, kubeconfig, k8sContext, endpoint, tokenFile, traceEnabled)
		if err != nil {
			return err
		}
		defer c.Close()

		_, err = c.DeleteActorTemplate(ctx, &ateapipb.DeleteActorTemplateRequest{
			ActorTemplate: &ateapipb.ObjectRef{Atespace: deleteActorTemplateAtespaceFlag, Name: args[0]},
		})
		if err != nil {
			return fmt.Errorf("failed to delete actor template: %w", err)
		}

		fmt.Printf("actor template %q deleted\n", args[0])
		return nil
	},
}

// actorTemplateFromManifest parses a single protojson-shaped YAML or JSON
// document into an ActorTemplate. Parsing is strict: unknown fields are an
// error, so typos don't silently drop configuration.
func actorTemplateFromManifest(data []byte) (*ateapipb.ActorTemplate, error) {
	jsonData, err := yaml.YAMLToJSON(data)
	if err != nil {
		return nil, fmt.Errorf("invalid YAML: %w", err)
	}
	if string(jsonData) == "null" {
		return nil, fmt.Errorf("manifest is empty")
	}
	template := &ateapipb.ActorTemplate{}
	if err := protojson.Unmarshal(jsonData, template); err != nil {
		return nil, err
	}
	return template, nil
}

// readFileOrStdin reads the manifest from path, or from in when path is "-".
func readFileOrStdin(in io.Reader, path string) ([]byte, error) {
	if path == "-" {
		return io.ReadAll(in)
	}
	return os.ReadFile(path)
}

func init() {
	getActorTemplatesCmd.Flags().StringVarP(&getActorTemplateAtespaceFlag, "atespace", "a", "", "Atespace to list/get actor templates in. Required when getting templates; for listing, use this or -A.")
	getActorTemplatesCmd.Flags().BoolVarP(&getActorTemplateAllAtespacesFlag, "all-atespaces", "A", false, "List actor templates across all atespaces (listing only; mutually exclusive with --atespace)")
	getCmd.AddCommand(getActorTemplatesCmd)

	createActorTemplateCmd.Flags().StringVarP(&createActorTemplateFilenameFlag, "filename", "f", "", "Manifest file holding a single protojson-shaped ActorTemplate document; use - for stdin (required)")
	_ = createActorTemplateCmd.MarkFlagRequired("filename")
	createCmd.AddCommand(createActorTemplateCmd)

	deleteActorTemplateCmd.Flags().StringVarP(&deleteActorTemplateAtespaceFlag, "atespace", "a", "", "Atespace the actor template lives in")
	_ = deleteActorTemplateCmd.MarkFlagRequired("atespace")
	deleteCmd.AddCommand(deleteActorTemplateCmd)
}

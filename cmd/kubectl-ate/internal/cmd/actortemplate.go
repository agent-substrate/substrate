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

	"github.com/spf13/cobra"
	"go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/cmd/kubectl-ate/internal/printer"
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
	Use:     "actor-template [template-name ...]",
	Aliases: []string{"actor-templates"},
	Short:   "List all actor templates or get one or more actor templates",
	Long:    "List or get actor templates.",
	Args:    cobra.ArbitraryArgs,
	RunE: func(cmd *cobra.Command, args []string) error {
		if err := validateAtespaceFlags("actor templates", args, getActorTemplateAtespaceFlag, getActorTemplateAllAtespacesFlag); err != nil {
			return err
		}

		ctx := cmd.Context()
		apiClient, err := newAPIClient(cmd)
		if err != nil {
			return fmt.Errorf("failed to connect to ate-api-server: %w", err)
		}
		defer apiClient.Close()

		if len(args) > 0 {
			templates := make([]*ateapi.ActorTemplate, 0, len(args))
			for _, name := range args {
				resp, err := apiClient.GetActorTemplate(ctx, &ateapi.GetActorTemplateRequest{
					ActorTemplate: &ateapi.ObjectRef{Atespace: getActorTemplateAtespaceFlag, Name: name},
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

		var allTemplates []*ateapi.ActorTemplate
		pageToken := ""
		for {
			resp, err := apiClient.ListActorTemplates(ctx, &ateapi.ListActorTemplatesRequest{
				Options:  &ateapi.ListOptions{PageSize: 100, PageToken: pageToken},
				Atespace: getActorTemplateAtespaceFlag,
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

The manifest is a single YAML (or JSON) document holding one ateapi.ActorTemplate
message in its protojson form, exactly as printed by
"kubectl ate get actor-template <name> -a <atespace> -o yaml".
The template's atespace and name come from the manifest's metadata.
The atespace must already exist.

Actor templates are immutable: there is no update; delete and recreate to change one.`,
	Args: noArgs("the template name comes from the manifest passed to -f/--filename", "filename"),
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
		apiClient, err := newAPIClient(cmd)
		if err != nil {
			return fmt.Errorf("failed to connect to ate-api-server: %w", err)
		}
		defer apiClient.Close()

		resp, err := apiClient.CreateActorTemplate(ctx, &ateapi.CreateActorTemplateRequest{ActorTemplate: template})
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
	Args: exactlyOneArg("<template-name>", "atespace"),
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := cmd.Context()
		c, err := newAPIClient(cmd)
		if err != nil {
			return err
		}
		defer c.Close()

		_, err = c.DeleteActorTemplate(ctx, &ateapi.DeleteActorTemplateRequest{
			ActorTemplate: &ateapi.ObjectRef{Atespace: deleteActorTemplateAtespaceFlag, Name: args[0]},
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
func actorTemplateFromManifest(data []byte) (*ateapi.ActorTemplate, error) {
	jsonData, err := yaml.YAMLToJSON(data)
	if err != nil {
		return nil, fmt.Errorf("invalid YAML: %w", err)
	}
	if string(jsonData) == "null" {
		return nil, fmt.Errorf("manifest is empty")
	}
	template := &ateapi.ActorTemplate{}
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
	getActorTemplatesCmd.Flags().StringVarP(&getActorTemplateAtespaceFlag, "atespace", "a", "", "get actor-templates in a specific atespace (mutually exclusive with --all-atespaces)")
	getActorTemplatesCmd.Flags().BoolVarP(&getActorTemplateAllAtespacesFlag, "all-atespaces", "A", false, "get actor-templates across all atespaces (mutually exclusive with --atespace)")
	getCmd.AddCommand(getActorTemplatesCmd)

	createActorTemplateCmd.Flags().StringVarP(&createActorTemplateFilenameFlag, "filename", "f", "", "manifest file holding a single JSON or YAML actor-template document, or - for stdin")
	createCmd.AddCommand(createActorTemplateCmd)

	deleteActorTemplateCmd.Flags().StringVarP(&deleteActorTemplateAtespaceFlag, "atespace", "a", "", "the actor-template's atespace")
	deleteCmd.AddCommand(deleteActorTemplateCmd)
}

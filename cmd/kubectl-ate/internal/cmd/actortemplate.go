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
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/spf13/cobra"
	"go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/cmd/kubectl-ate/internal/printer"
	"google.golang.org/grpc"
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
	Short: "Create actor templates from a manifest",
	Long: `Create one or more actor templates from a manifest file.

The manifest is a JSON file holding one ateapi.ActorTemplate, or a YAML file
holding one or more documents separated by "---". Each document holds one
ateapi.ActorTemplate in its protojson form, exactly as printed by
"kubectl ate get actor-template <name> -a <atespace> -o yaml".
Every document is created; a failure on one does not stop the rest, and all
errors are reported at the end.
Each template's atespace and name come from its own metadata.
The atespace must already exist.

Actor templates are immutable: there is no update; delete and recreate to change one.`,
	Args: noArgs("the template name comes from the manifest passed to -f/--filename", "filename"),
	RunE: func(cmd *cobra.Command, args []string) error {
		data, err := readFileOrStdin(cmd.InOrStdin(), createActorTemplateFilenameFlag)
		if err != nil {
			return err
		}
		documents, err := actorTemplatesFromManifest(data)
		if err != nil {
			return fmt.Errorf("failed to parse actor template manifest %q: %w", createActorTemplateFilenameFlag, err)
		}

		ctx := cmd.Context()
		apiClient, err := newAPIClient(cmd)
		if err != nil {
			return fmt.Errorf("failed to connect to ate-api-server: %w", err)
		}
		defer apiClient.Close()

		runner := &createActorTemplatesRunner{
			creator:   apiClient,
			documents: documents,
			outputFmt: outputFmt,
			stdout:    cmd.OutOrStdout(),
		}
		return runner.Run(ctx)
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

// actorTemplateCreator abstracts CreateActorTemplate RPC calls.
type actorTemplateCreator interface {
	CreateActorTemplate(ctx context.Context, req *ateapi.CreateActorTemplateRequest, opts ...grpc.CallOption) (*ateapi.ActorTemplate, error)
}

// actorTemplateDocument is one document of an actor-template manifest.
type actorTemplateDocument struct {
	// index is the 1-based position of the document in the manifest.
	index    int
	template *ateapi.ActorTemplate
}

// createActorTemplatesRunner creates each template in order, continuing past
// failures and reporting them together, the way kubectl creates the resources
// in a multi-document manifest.
type createActorTemplatesRunner struct {
	creator   actorTemplateCreator
	documents []actorTemplateDocument
	outputFmt string
	stdout    io.Writer
}

func (r *createActorTemplatesRunner) Run(ctx context.Context) error {
	var created []*ateapi.ActorTemplate
	var errs []error
	for _, doc := range r.documents {
		c, err := r.creator.CreateActorTemplate(ctx, &ateapi.CreateActorTemplateRequest{ActorTemplate: doc.template})
		if err != nil {
			errs = append(errs, r.createError(doc, err))
			continue
		}
		created = append(created, c)
	}
	// Print what was created: a manifest holding a single document prints one
	// template and a multi-document manifest prints a list, so a partial
	// failure does not change the output schema.
	switch {
	case len(created) == 0:
	case len(r.documents) == 1:
		errs = append(errs, printer.PrintActorTemplateTo(r.stdout, created[0], r.outputFmt))
	default:
		errs = append(errs, printer.PrintActorTemplatesTo(r.stdout, created, r.outputFmt))
	}
	return errors.Join(errs...)
}

// createError names the template a failure came from, and the document it was
// read from when the manifest holds more than one.
func (r *createActorTemplatesRunner) createError(doc actorTemplateDocument, err error) error {
	metadata := doc.template.GetMetadata()
	what := "actor template"
	switch {
	case metadata.GetAtespace() != "" && metadata.GetName() != "":
		what = fmt.Sprintf("actor template %q in atespace %q", metadata.GetName(), metadata.GetAtespace())
	case metadata.GetName() != "":
		what = fmt.Sprintf("actor template %q", metadata.GetName())
	}
	err = fmt.Errorf("failed to create %s: %w", what, err)
	if len(r.documents) > 1 {
		err = fmt.Errorf("document %d: %w", doc.index, err)
	}
	return err
}

// actorTemplatesFromManifest parses every protojson-shaped YAML or JSON
// document in data into an ActorTemplate. Parsing is strict: unknown fields
// are an error. Empty documents are skipped, the way kubectl skips them.
func actorTemplatesFromManifest(data []byte) ([]actorTemplateDocument, error) {
	docs, err := manifestMessages(data, &ateapi.ActorTemplate{})
	if err != nil {
		return nil, err
	}
	templates := make([]actorTemplateDocument, len(docs))
	for i, doc := range docs {
		templates[i] = actorTemplateDocument{
			index:    doc.Index,
			template: doc.Message.(*ateapi.ActorTemplate),
		}
	}
	return templates, nil
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

	createActorTemplateCmd.Flags().StringVarP(&createActorTemplateFilenameFlag, "filename", "f", "", "manifest file holding one actor-template as JSON, or one or more as YAML documents separated by ---, or - for stdin")
	createCmd.AddCommand(createActorTemplateCmd)

	deleteActorTemplateCmd.Flags().StringVarP(&deleteActorTemplateAtespaceFlag, "atespace", "a", "", "the actor-template's atespace")
	deleteCmd.AddCommand(deleteActorTemplateCmd)
}

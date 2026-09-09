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

var createActorTemplateFilenameFlag string

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
		return printer.PrintActorTemplate(resp, outputFmt)
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
	createActorTemplateCmd.Flags().StringVarP(&createActorTemplateFilenameFlag, "filename", "f", "", "Manifest file holding a single protojson-shaped ActorTemplate document; use - for stdin (required)")
	_ = createActorTemplateCmd.MarkFlagRequired("filename")
	createCmd.AddCommand(createActorTemplateCmd)
}

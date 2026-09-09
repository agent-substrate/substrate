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

	"github.com/agent-substrate/substrate/cmd/kubectl-ate/internal/printer"
	"github.com/agent-substrate/substrate/internal/ateclient"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/spf13/cobra"
)

var templateFlag string
var atespaceFlag string
var sourceTagFlag string

var createActorCmd = &cobra.Command{
	Use:   "actor <actor-name>",
	Short: "Create an actor",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		request, err := buildCreateActorRequest(args[0], atespaceFlag, templateFlag, sourceTagFlag)
		if err != nil {
			return err
		}

		ctx := cmd.Context()
		apiClient, err := ateclient.NewClient(ctx, kubeconfig, k8sContext, endpoint, tokenFile, traceEnabled)
		if err != nil {
			return fmt.Errorf("failed to connect to ate-api-server: %w", err)
		}
		defer apiClient.Close()

		resp, err := apiClient.CreateActor(ctx, request)
		if err != nil {
			return fmt.Errorf("failed to create actor: %w", err)
		}

		return printer.PrintActor(resp, outputFmt)
	},
}

func buildCreateActorRequest(actorName, atespace, template, tag string) (*ateapipb.CreateActorRequest, error) {
	templateRef, err := parseAtespacedName(template, atespace)
	if err != nil {
		return nil, err
	}
	actor := &ateapipb.Actor{
		Metadata: &ateapipb.ResourceMetadata{
			Atespace: atespace,
			Name:     actorName,
		},
		ActorTemplate: templateRef,
	}

	if tag != "" {
		ref, err := parseAtespacedName(tag, atespace)
		if err != nil {
			return nil, err
		}
		actor.SourceTag = ref
	}
	return &ateapipb.CreateActorRequest{Actor: actor}, nil
}

func init() {
	createActorCmd.Flags().StringVar(&templateFlag, "template", "", "The name of the ActorTemplate to derive the actor from, as <atespace>/<template-name>, or just <template-name> to use the actor's own atespace (--atespace)")
	_ = createActorCmd.MarkFlagRequired("template")
	createActorCmd.Flags().StringVarP(&atespaceFlag, "atespace", "a", "", "Atespace to create the actor in")
	_ = createActorCmd.MarkFlagRequired("atespace")
	createActorCmd.Flags().StringVar(&sourceTagFlag, "tag", "", "The name of a Tag to initialize the actor from, as <atespace>/<tag-name>, or just <tag-name> to use the actor's own atespace (--atespace)")
	createCmd.AddCommand(createActorCmd)
}

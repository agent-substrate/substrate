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

var templateRefFlag string
var atespaceFlag string
var sourceSnapshotTagFlag string

var createActorCmd = &cobra.Command{
	Use:   "actor <actor-name>",
	Short: "Create an actor",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		request, err := buildCreateActorRequest(args[0], atespaceFlag, templateRefFlag, sourceSnapshotTagFlag)
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

// buildCreateActorRequest assembles the CreateActor request from the command
// flags.
func buildCreateActorRequest(actorName, atespace, templateName, snapshotTag string) (*ateapipb.CreateActorRequest, error) {
	actor := &ateapipb.Actor{
		Metadata: &ateapipb.ResourceMetadata{
			Atespace: atespace,
			Name:     actorName,
		},
		ActorTemplate: &ateapipb.ObjectRef{
			Atespace: atespace,
			Name:     templateName,
		},
	}

	if snapshotTag != "" {
		ref, err := parseNamespacedName(snapshotTag)
		if err != nil {
			return nil, err
		}
		actor.SourceSnapshotTag = ref
	}
	return &ateapipb.CreateActorRequest{Actor: actor}, nil
}

func init() {
	// TODO: rename "template-ref" to "template" now that the legacy CRD flag is gone.
	createActorCmd.Flags().StringVar(&templateRefFlag, "template-ref", "", "Name of the substrate ActorTemplate resource to derive the actor from, resolved in the actor's atespace (--atespace)")
	_ = createActorCmd.MarkFlagRequired("template-ref")
	createActorCmd.Flags().StringVarP(&atespaceFlag, "atespace", "a", "", "Atespace to create the actor in (required)")
	_ = createActorCmd.MarkFlagRequired("atespace")
	createActorCmd.Flags().StringVar(&sourceSnapshotTagFlag, "snapshot-tag", "", "Initialize from an ActorSnapshot tag in <atespace>/<name> format")
	createCmd.AddCommand(createActorCmd)
}

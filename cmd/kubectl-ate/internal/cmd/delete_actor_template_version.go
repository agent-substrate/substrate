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

	"github.com/agent-substrate/substrate/internal/ateclient"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/spf13/cobra"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

var deleteActorTemplateVersionClearDefault bool

var deleteActorTemplateVersionCmd = &cobra.Command{
	Use:   "actor-template-version <name>",
	Short: "Delete an actor template version",
	Long: `Delete an actor template version.

A version that is its template's defaultVersionOnCreate cannot be deleted;
pass --clear-default to clear the template's default first.`,
	Args: cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := cmd.Context()
		c, err := ateclient.NewClient(ctx, kubeconfig, k8sContext, endpoint, traceEnabled)
		if err != nil {
			return err
		}
		defer c.Close()

		name := args[0]
		if deleteActorTemplateVersionClearDefault {
			version, err := c.ControlClient.GetActorTemplateVersion(ctx, &ateapipb.GetActorTemplateVersionRequest{
				ActorTemplateVersion: &ateapipb.ObjectRef{Name: name},
			})
			if err != nil {
				return err
			}
			template := version.GetActorTemplate().GetName()
			parent, err := c.ControlClient.GetActorTemplate(ctx, &ateapipb.GetActorTemplateRequest{
				ActorTemplate: &ateapipb.ObjectRef{Name: template},
			})
			if err != nil {
				return err
			}
			if parent.GetSpec().GetDefaultVersionOnCreate().GetName() == name {
				if _, err := c.ControlClient.UpdateActorTemplate(ctx, &ateapipb.UpdateActorTemplateRequest{
					ActorTemplate: &ateapipb.ActorTemplate{
						Metadata: &ateapipb.ResourceMetadata{Name: template},
						Spec:     &ateapipb.ActorTemplateSpec{DefaultVersionOnCreate: nil},
					},
					UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"spec.default_version_on_create"}},
				}); err != nil {
					return fmt.Errorf("while clearing default_version_on_create of ActorTemplate %q: %w", template, err)
				}
				fmt.Printf("actor template %q default version cleared\n", template)
			}
		}

		if _, err := c.ControlClient.DeleteActorTemplateVersion(ctx, &ateapipb.DeleteActorTemplateVersionRequest{
			ActorTemplateVersion: &ateapipb.ObjectRef{Name: name},
		}); err != nil {
			return err
		}
		fmt.Printf("actor template version %q deleted\n", name)
		return nil
	},
}

func init() {
	deleteActorTemplateVersionCmd.Flags().BoolVar(&deleteActorTemplateVersionClearDefault, "clear-default", false, "Clear the parent template's defaultVersionOnCreate first if it names this version")
	deleteCmd.AddCommand(deleteActorTemplateVersionCmd)
}

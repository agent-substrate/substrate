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
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/agent-substrate/substrate/internal/ateclient"
	"github.com/agent-substrate/substrate/internal/templateversion"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/spf13/cobra"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
	utilyaml "k8s.io/apimachinery/pkg/util/yaml"
	"sigs.k8s.io/yaml"
)

// applyAPIVersion is the manifest envelope for control-plane resources
// applied through the ate API — deliberately distinct from the
// ate.dev/v1alpha1 CRDs applied with plain kubectl, whose ActorTemplate has
// a different spec shape.
const applyAPIVersion = "api.ate.dev/v1alpha1"

var applyFile string

// applyDoc is one parsed manifest document.
type applyDoc struct {
	template *ateapipb.ActorTemplate
	version  *ateapipb.ActorTemplateVersion
	// maskPaths are the mutable ActorTemplate fields present in the
	// manifest, applied with UpdateActorTemplate after all creates.
	maskPaths []string
}

var applyCmd = &cobra.Command{
	Use:   "apply -f FILENAME",
	Short: "Apply ActorTemplate and ActorTemplateVersion manifests",
	Long: `Apply ActorTemplate and ActorTemplateVersion manifests (apiVersion ` + applyAPIVersion + `).

Templates are created, then their mutable fields (spec.workerSelector,
spec.defaultVersionOnCreate) are applied — after every create, so a template
may name a version defined later in the same file. Versions are immutable:
re-applying an identical version is a no-op, and changing one is an error —
ship the change as a new version instead. Mutable fields absent from the
manifest are left as they are, not cleared.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := cmd.Context()
		docs, err := readApplyDocs(applyFile)
		if err != nil {
			return err
		}
		if len(docs) == 0 {
			return errors.New("no resources found in the manifest")
		}

		apiClient, err := ateclient.NewClient(ctx, kubeconfig, k8sContext, endpoint, traceEnabled)
		if err != nil {
			return fmt.Errorf("failed to connect to ate-api-server: %w", err)
		}
		defer apiClient.Close()

		return applyDocs(ctx, cmd.OutOrStdout(), apiClient.ControlClient, docs)
	},
}

func applyDocs(ctx context.Context, out io.Writer, client ateapipb.ControlClient, docs []*applyDoc) error {
	// Pass 1: creates. Templates are created without their mutable fields
	// (the API forbids default_version_on_create at creation); versions are
	// created or verified unchanged.
	templateExisted := map[string]bool{}
	for _, doc := range docs {
		switch {
		case doc.template != nil:
			name := doc.template.GetMetadata().GetName()
			create := proto.Clone(doc.template).(*ateapipb.ActorTemplate)
			if create.GetSpec() != nil {
				create.Spec.DefaultVersionOnCreate = nil
			}
			_, err := client.CreateActorTemplate(ctx, &ateapipb.CreateActorTemplateRequest{ActorTemplate: create})
			switch {
			case err == nil:
			case status.Code(err) == codes.AlreadyExists:
				templateExisted[name] = true
			default:
				return fmt.Errorf("while creating ActorTemplate %q: %w", name, err)
			}
		case doc.version != nil:
			verdict, err := applyVersion(ctx, client, doc.version)
			if err != nil {
				return err
			}
			fmt.Fprintf(out, "actortemplateversion/%s %s\n", doc.version.GetMetadata().GetName(), verdict)
		}
	}

	// Pass 2: template mutable fields, now that every version in the file
	// exists. A fresh create already carries its worker selector, so only
	// the default (or updates to a pre-existing template) need this.
	for _, doc := range docs {
		if doc.template == nil {
			continue
		}
		name := doc.template.GetMetadata().GetName()
		paths := doc.maskPaths
		if !templateExisted[name] {
			paths = nil
			if doc.template.GetSpec().GetDefaultVersionOnCreate() != nil {
				paths = []string{"spec.default_version_on_create"}
			}
		}
		if len(paths) > 0 {
			if _, err := client.UpdateActorTemplate(ctx, &ateapipb.UpdateActorTemplateRequest{
				ActorTemplate: doc.template,
				UpdateMask:    &fieldmaskpb.FieldMask{Paths: paths},
			}); err != nil {
				return fmt.Errorf("while updating ActorTemplate %q: %w", name, err)
			}
		}
		verdict := "created"
		if templateExisted[name] {
			verdict = "configured"
		}
		fmt.Fprintf(out, "actortemplate/%s %s\n", name, verdict)
	}
	return nil
}

// applyVersion creates a version, or — versions being immutable — verifies
// an existing one matches the manifest after server-style defaulting.
func applyVersion(ctx context.Context, client ateapipb.ControlClient, version *ateapipb.ActorTemplateVersion) (string, error) {
	name := version.GetMetadata().GetName()
	_, err := client.CreateActorTemplateVersion(ctx, &ateapipb.CreateActorTemplateVersionRequest{ActorTemplateVersion: version})
	if err == nil {
		return "created", nil
	}
	if status.Code(err) != codes.AlreadyExists {
		return "", fmt.Errorf("while creating ActorTemplateVersion %q: %w", name, err)
	}

	stored, err := client.GetActorTemplateVersion(ctx, &ateapipb.GetActorTemplateVersionRequest{
		ActorTemplateVersion: &ateapipb.ObjectRef{Name: name},
	})
	if err != nil {
		return "", fmt.Errorf("while reading existing ActorTemplateVersion %q: %w", name, err)
	}
	wantSpec := proto.Clone(version.GetSpec()).(*ateapipb.ActorTemplateVersionSpec)
	templateversion.DefaultSpec(wantSpec)
	if !proto.Equal(stored.GetSpec(), wantSpec) {
		return "", fmt.Errorf("ActorTemplateVersion %q exists with a different spec; versions are immutable, ship the change as a new version", name)
	}
	if stored.GetActorTemplate().GetName() != version.GetActorTemplate().GetName() {
		return "", fmt.Errorf("ActorTemplateVersion %q exists under ActorTemplate %q, not %q",
			name, stored.GetActorTemplate().GetName(), version.GetActorTemplate().GetName())
	}
	return "unchanged", nil
}

func readApplyDocs(filename string) ([]*applyDoc, error) {
	var in io.Reader
	if filename == "-" {
		in = os.Stdin
	} else {
		f, err := os.Open(filename)
		if err != nil {
			return nil, err
		}
		defer f.Close()
		in = f
	}
	return parseApplyDocs(in)
}

func parseApplyDocs(in io.Reader) ([]*applyDoc, error) {
	var docs []*applyDoc
	reader := utilyaml.NewYAMLReader(bufio.NewReader(in))
	for i := 0; ; i++ {
		raw, err := reader.Read()
		if err == io.EOF {
			return docs, nil
		}
		if err != nil {
			return nil, fmt.Errorf("while reading document %d: %w", i+1, err)
		}
		doc, err := parseApplyDoc(raw)
		if err != nil {
			return nil, fmt.Errorf("document %d: %w", i+1, err)
		}
		if doc != nil {
			docs = append(docs, doc)
		}
	}
}

func parseApplyDoc(raw []byte) (*applyDoc, error) {
	jsonBytes, err := yaml.YAMLToJSON(raw)
	if err != nil {
		return nil, fmt.Errorf("invalid YAML: %w", err)
	}
	var body map[string]json.RawMessage
	if err := json.Unmarshal(jsonBytes, &body); err != nil {
		if string(jsonBytes) == "null" {
			return nil, nil // Empty document.
		}
		return nil, fmt.Errorf("expected an object, got: %s", jsonBytes)
	}
	if len(body) == 0 {
		return nil, nil
	}

	var apiVersion, kind string
	if raw, ok := body["apiVersion"]; ok {
		_ = json.Unmarshal(raw, &apiVersion)
	}
	if raw, ok := body["kind"]; ok {
		_ = json.Unmarshal(raw, &kind)
	}
	if apiVersion != applyAPIVersion {
		return nil, fmt.Errorf("unsupported apiVersion %q; kubectl ate apply only handles %s (CRD manifests go through plain kubectl apply)", apiVersion, applyAPIVersion)
	}
	// The envelope is CLI-level routing, not part of the proto shape.
	delete(body, "apiVersion")
	delete(body, "kind")
	payload, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}

	switch kind {
	case "ActorTemplate":
		template := &ateapipb.ActorTemplate{}
		if err := protojson.Unmarshal(payload, template); err != nil {
			return nil, fmt.Errorf("invalid ActorTemplate: %w", err)
		}
		return &applyDoc{template: template, maskPaths: templateMaskPaths(body)}, nil
	case "ActorTemplateVersion":
		version := &ateapipb.ActorTemplateVersion{}
		if err := protojson.Unmarshal(payload, version); err != nil {
			return nil, fmt.Errorf("invalid ActorTemplateVersion: %w", err)
		}
		return &applyDoc{version: version}, nil
	default:
		return nil, fmt.Errorf("unsupported kind %q; expected ActorTemplate or ActorTemplateVersion", kind)
	}
}

// templateMaskPaths maps the mutable spec fields present in the manifest to
// the UpdateActorTemplate mask paths. Absent fields stay off the mask so
// re-applying a partial manifest never clears them.
func templateMaskPaths(body map[string]json.RawMessage) []string {
	specRaw, ok := body["spec"]
	if !ok {
		return nil
	}
	var spec map[string]json.RawMessage
	if err := json.Unmarshal(specRaw, &spec); err != nil {
		return nil // protojson.Unmarshal already rejected the document.
	}
	has := func(keys ...string) bool {
		for _, k := range keys {
			if _, ok := spec[k]; ok {
				return true
			}
		}
		return false
	}
	var paths []string
	// protojson accepts both spellings on input.
	if has("workerSelector", "worker_selector") {
		paths = append(paths, "spec.worker_selector")
	}
	if has("defaultVersionOnCreate", "default_version_on_create") {
		paths = append(paths, "spec.default_version_on_create")
	}
	return paths
}

func init() {
	applyCmd.Flags().StringVarP(&applyFile, "filename", "f", "", "Manifest file to apply, or - for stdin")
	_ = applyCmd.MarkFlagRequired("filename")
	rootCmd.AddCommand(applyCmd)
}

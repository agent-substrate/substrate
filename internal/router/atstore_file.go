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

package router

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"

	v1alpha1 "github.com/agent-substrate/substrate/pkg/api/v1alpha1"
	"k8s.io/apimachinery/pkg/util/yaml"
	sigsyaml "sigs.k8s.io/yaml"
)

// fileATStore implements the atStore interface by reading and decoding YAML or JSON
// files containing ActorTemplates or ActorTemplateLists.
type fileATStore struct {
	filePath string
}

func newFileATStore(filePath string) *fileATStore {
	return &fileATStore{
		filePath: filePath,
	}
}

func (t *fileATStore) readyTemplates(ctx context.Context) ([]*v1alpha1.ActorTemplate, error) {
	data, err := os.ReadFile(t.filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read actor templates YAML file %q: %w", t.filePath, err)
	}

	decoder := yaml.NewYAMLOrJSONDecoder(bytes.NewReader(data), 4096)
	var templates []*v1alpha1.ActorTemplate

	for {
		var obj map[string]interface{}
		if err := decoder.Decode(&obj); err != nil {
			if err == io.EOF {
				break
			}
			return nil, fmt.Errorf("failed to decode YAML document: %w", err)
		}

		if obj == nil {
			continue
		}

		// Check if it's a List (e.g., standard kubectl get -o yaml output)
		if kind, ok := obj["kind"].(string); ok && strings.EqualFold(kind, "List") {
			items, ok := obj["items"].([]interface{})
			if !ok {
				continue
			}

			for _, itemVal := range items {
				itemMap, ok := itemVal.(map[string]interface{})
				if !ok {
					continue
				}

				itemBytes, err := json.Marshal(itemMap)
				if err != nil {
					return nil, fmt.Errorf("failed to marshal item to JSON: %w", err)
				}

				var item v1alpha1.ActorTemplate
				if err := sigsyaml.Unmarshal(itemBytes, &item); err != nil {
					return nil, fmt.Errorf("failed to unmarshal ActorTemplate item: %w", err)
				}

				templates = append(templates, &item)
			}
			continue
		}

		// It's an ActorTemplateList structure
		if kind, ok := obj["kind"].(string); ok && strings.EqualFold(kind, "ActorTemplateList") {
			objBytes, err := json.Marshal(obj)
			if err != nil {
				return nil, fmt.Errorf("failed to marshal ActorTemplateList to JSON: %w", err)
			}

			var list v1alpha1.ActorTemplateList
			if err := sigsyaml.Unmarshal(objBytes, &list); err != nil {
				return nil, fmt.Errorf("failed to unmarshal ActorTemplateList: %w", err)
			}

			for i := range list.Items {
				templates = append(templates, &list.Items[i])
			}
			continue
		}

		// Fallback: single ActorTemplate document
		objBytes, err := json.Marshal(obj)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal document to JSON: %w", err)
		}

		var item v1alpha1.ActorTemplate
		if err := sigsyaml.Unmarshal(objBytes, &item); err != nil {
			return nil, fmt.Errorf("failed to unmarshal document as ActorTemplate: %w", err)
		}

		templates = append(templates, &item)
	}

	slog.InfoContext(ctx, "fileATStore parsed offline templates",
		slog.String("filePath", t.filePath),
		slog.Int("count", len(templates)))
	return templates, nil
}

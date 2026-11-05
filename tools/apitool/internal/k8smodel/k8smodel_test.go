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

package k8smodel_test

import (
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"

	"go.ate.dev/substrate/manifests/ate-install/generated"
	"go.ate.dev/substrate/tools/apitool/internal/k8smodel"
)

const widgets = `
apiVersion: apiextensions.k8s.io/v1
kind: CustomResourceDefinition
metadata:
  name: widgets.test.dev
spec:
  group: test.dev
  names:
    kind: Widget
    plural: widgets
  scope: Namespaced
  versions:
  - name: v1
    served: true
    storage: true
    schema:
      openAPIV3Schema:
        description: Widget does things.
        type: object
        properties:
          apiVersion:
            type: string
          kind:
            type: string
          metadata:
            type: object
          spec:
            description: spec is the desired state.
            type: object
            required:
            - count
            properties:
              count:
                description: count is how many.
                type: integer
                format: int32
              color:
                type: string
                default: red
                enum:
                - red
                - blue
              labels:
                type: object
                additionalProperties:
                  description: Label is one label.
                  type: string
              quantity:
                x-kubernetes-int-or-string: true
              items:
                description: items lists the parts.
                type: array
                items:
                  description: Part is one part.
                  type: object
                  properties:
                    name:
                      type: string
              limits:
                type: object
                additionalProperties:
                  type: object
                  properties:
                    max:
                      type: integer
`

const gadgets = `
apiVersion: apiextensions.k8s.io/v1
kind: CustomResourceDefinition
metadata:
  name: gadgets.test.dev
spec:
  group: test.dev
  names:
    kind: Gadget
    plural: gadgets
  scope: Cluster
  versions:
  - name: v2
    served: true
    storage: true
    schema:
      openAPIV3Schema:
        type: object
  - name: v1
    served: false
    storage: false
    schema:
      openAPIV3Schema:
        type: object
`

func TestBuild(t *testing.T) {
	got, err := k8smodel.Build([]string{widgets, gadgets})
	if err != nil {
		t.Fatalf("Build() error = %v", err)
	}
	want := &k8smodel.API{Resources: []k8smodel.Resource{
		{Group: "test.dev", Version: "v2", Kind: "Gadget", Scope: "Cluster"},
		{
			Group:   "test.dev",
			Version: "v1",
			Kind:    "Widget",
			Scope:   "Namespaced",
			Doc:     "Widget does things.",
			Fields: []k8smodel.Field{{
				Name: "spec",
				Type: "object",
				Doc:  "spec is the desired state.",
				Fields: []k8smodel.Field{
					{Name: "color", Type: "string", Default: `"red"`, Enum: []string{`"red"`, `"blue"`}},
					{Name: "count", Type: "int32", Doc: "count is how many.", Required: true},
					{Name: "items", Type: "[]object", Doc: "items lists the parts.\n\nPart is one part.", Fields: []k8smodel.Field{{Name: "name", Type: "string"}}},
					{Name: "labels", Type: "map[string]string", Doc: "Label is one label."},
					{Name: "limits", Type: "map[string]object", Fields: []k8smodel.Field{{Name: "max", Type: "integer"}}},
					{Name: "quantity", Type: "int-or-string"},
				},
			}},
		},
	}}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("Build() mismatch (-want +got):\n%s", diff)
	}
}

func TestBuild_Errors(t *testing.T) {
	for _, tc := range []struct {
		name, want string
		sources    []string
	}{
		{name: "no sources", want: "no CRDs"},
		{name: "not YAML", sources: []string{"\t"}, want: "while parsing CRD"},
		{name: "two served versions", sources: []string{strings.ReplaceAll(gadgets, "served: false", "served: true")}, want: "serves 2 versions"},
		{name: "no served version", sources: []string{strings.ReplaceAll(gadgets, "served: true", "served: false")}, want: "serves 0 versions"},
		{name: "field without type", sources: []string{strings.ReplaceAll(widgets, "x-kubernetes-int-or-string", "x-kubernetes-preserve-unknown-fields")}, want: "spec.quantity: has no type"},
		{name: "enum on map values", sources: []string{strings.ReplaceAll(widgets, "description: Label is one label.", "enum: [a]")}, want: "spec.labels: sets an enum or default"},
		{name: "default on list items", sources: []string{strings.ReplaceAll(widgets, "description: Part is one part.", "default: {}")}, want: "spec.items: sets an enum or default"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := k8smodel.Build(tc.sources)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Errorf("Build() error = %v, want it to contain %q", err, tc.want)
			}
		})
	}
}

// TestBuild_RealCRDs builds the CRDs the site documents.
func TestBuild_RealCRDs(t *testing.T) {
	sources, err := generated.CRDs()
	if err != nil {
		t.Fatal(err)
	}
	got, err := k8smodel.Build(sources)
	if err != nil {
		t.Fatalf("Build() error = %v", err)
	}
	var kinds []string
	for _, c := range got.Resources {
		kinds = append(kinds, c.Kind)
		if len(c.Fields) == 0 {
			t.Errorf("%s has no fields", c.Kind)
		}
	}
	if diff := cmp.Diff([]string{"CSIDriverConfig", "SandboxConfig", "WorkerPool"}, kinds); diff != "" {
		t.Errorf("kinds mismatch (-want +got):\n%s", diff)
	}
}

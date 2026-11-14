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

package examples_test

import (
	"maps"
	"slices"
	"strings"
	"testing"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/reflect/protoregistry"
	"k8s.io/apiextensions-apiserver/pkg/apis/apiextensions"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/apiextensions-apiserver/pkg/apiserver/validation"
	"sigs.k8s.io/yaml"

	"go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/manifests/ate-install/generated"
	"go.ate.dev/substrate/tools/apitool/internal/examples"
	"go.ate.dev/substrate/tools/apitool/internal/k8smodel"
	"go.ate.dev/substrate/tools/apitool/internal/model"
)

// resources returns the API's resource messages by name.
func resources(t *testing.T) map[string]model.Message {
	t.Helper()
	api, err := model.Build(t.Context(), ateapi.Source)
	if err != nil {
		t.Fatalf("model.Build() error = %v", err)
	}
	rs, err := model.Resources(model.ScopeToService(api, "Control"))
	if err != nil {
		t.Fatalf("model.Resources() error = %v", err)
	}
	byName := map[string]model.Message{}
	for _, r := range rs {
		byName[r.Message.Name] = r.Message
	}
	return byName
}

func resolveGRPCExampleResource(name string, rs map[string]model.Message) (model.Message, bool) {
	if parent, resName, hasDot := strings.Cut(name, "."); hasDot {
		msg, ok := rs[resName]
		if !ok || msg.ResourceAnnotation == nil || !slices.Contains(msg.ResourceAnnotation.Parents, parent) {
			return model.Message{}, false
		}
		return msg, true
	}
	msg, ok := rs[name]
	return msg, ok
}

func TestGRPCResources_OnePerResource(t *testing.T) {
	got, err := examples.GRPCResources()
	if err != nil {
		t.Fatalf("GRPCResources() error = %v", err)
	}
	rs := resources(t)
	for name := range rs {
		if _, ok := got[name]; !ok {
			t.Errorf("resource %s has no example: add grpc/%s.yaml", name, name)
		}
	}
	for name := range got {
		if _, ok := resolveGRPCExampleResource(name, rs); !ok {
			t.Errorf("grpc/%s.yaml names no resource (or valid <Parent>.<Resource> subresource override)", name)
		}
	}
}

func TestGRPCResources_ParsesAsResource(t *testing.T) {
	got, err := examples.GRPCResources()
	if err != nil {
		t.Fatalf("GRPCResources() error = %v", err)
	}
	rs := resources(t)
	for _, name := range slices.Sorted(maps.Keys(got)) {
		msg, ok := resolveGRPCExampleResource(name, rs)
		if !ok {
			continue // Reported by TestGRPCResources_OnePerResource.
		}
		t.Run(name, func(t *testing.T) {
			mt, err := protoregistry.GlobalTypes.FindMessageByName(msg.FullName)
			if err != nil {
				t.Fatalf("FindMessageByName(%s) error = %v", msg.FullName, err)
			}
			j, err := yaml.YAMLToJSONStrict([]byte(got[name]))
			if err != nil {
				t.Fatalf("grpc/%s.yaml doesn't parse: %v", name, err)
			}
			if err := protojson.Unmarshal(j, mt.New().Interface()); err != nil {
				t.Errorf("grpc/%s.yaml doesn't parse as %s: %v", name, msg.FullName, err)
			}
		})
	}
}

// k8sResources returns the Kubernetes resources by kind.
func k8sResources(t *testing.T) map[string]k8smodel.Resource {
	t.Helper()
	sources, err := generated.CRDs()
	if err != nil {
		t.Fatal(err)
	}
	api, err := k8smodel.Build(sources)
	if err != nil {
		t.Fatalf("k8smodel.Build() error = %v", err)
	}
	byKind := map[string]k8smodel.Resource{}
	for _, r := range api.Resources {
		byKind[r.Kind] = r
	}
	return byKind
}

// validators returns a validator of each CRD's served schema by kind.
func validators(t *testing.T) map[string]validation.SchemaValidator {
	t.Helper()
	sources, err := generated.CRDs()
	if err != nil {
		t.Fatal(err)
	}
	byKind := map[string]validation.SchemaValidator{}
	for _, source := range sources {
		var crd apiextensionsv1.CustomResourceDefinition
		if err := yaml.Unmarshal([]byte(source), &crd); err != nil {
			t.Fatal(err)
		}
		for _, v := range crd.Spec.Versions {
			if !v.Served {
				continue
			}
			var schema apiextensions.JSONSchemaProps
			if err := apiextensionsv1.Convert_v1_JSONSchemaProps_To_apiextensions_JSONSchemaProps(v.Schema.OpenAPIV3Schema, &schema, nil); err != nil {
				t.Fatal(err)
			}
			validator, _, err := validation.NewSchemaValidator(&schema)
			if err != nil {
				t.Fatal(err)
			}
			byKind[crd.Spec.Names.Kind] = validator
		}
	}
	return byKind
}

func TestK8sResources_OnePerCRD(t *testing.T) {
	got, err := examples.K8sResources()
	if err != nil {
		t.Fatalf("K8sResources() error = %v", err)
	}
	all := k8sResources(t)
	for kind := range all {
		if _, ok := got[kind]; !ok {
			t.Errorf("CRD %s has no example: add k8s/%s.yaml", kind, kind)
		}
	}
	for kind := range got {
		if _, ok := all[kind]; !ok {
			t.Errorf("k8s/%s.yaml names no CRD", kind)
		}
	}
}

func TestK8sResources_MatchesSchema(t *testing.T) {
	got, err := examples.K8sResources()
	if err != nil {
		t.Fatalf("K8sResources() error = %v", err)
	}
	all := k8sResources(t)
	schemas := validators(t)
	for _, kind := range slices.Sorted(maps.Keys(got)) {
		r, ok := all[kind]
		if !ok {
			continue // Reported by TestK8sResources_OnePerCRD.
		}
		t.Run(kind, func(t *testing.T) {
			var doc map[string]any
			if err := yaml.UnmarshalStrict([]byte(got[kind]), &doc); err != nil {
				t.Fatalf("k8s/%s.yaml doesn't parse: %v", kind, err)
			}
			if want := r.Group + "/" + r.Version; doc["apiVersion"] != want {
				t.Errorf("apiVersion = %v, want %s", doc["apiVersion"], want)
			}
			if doc["kind"] != kind {
				t.Errorf("kind = %v, want %s", doc["kind"], kind)
			}
			for _, err := range validation.ValidateCustomResource(nil, doc, schemas[kind]) {
				t.Error(err)
			}
			// The validator allows unknown fields, so check their names too.
			delete(doc, "apiVersion")
			delete(doc, "kind")
			delete(doc, "metadata")
			checkFields(t, kind, doc, "object", r.Fields)
		})
	}
}

// checkFields reports each key of value, at any depth, that fields doesn't
// declare. typ is value's k8smodel.Field.Type: a list's items and a map's
// values are checked against fields, and a map's own keys are free.
func checkFields(t *testing.T, path string, value any, typ string, fields []k8smodel.Field) {
	t.Helper()
	switch v := value.(type) {
	case []any:
		for _, item := range v {
			checkFields(t, path, item, strings.TrimPrefix(typ, "[]"), fields)
		}
	case map[string]any:
		if elem, ok := strings.CutPrefix(typ, "map[string]"); ok {
			for key, child := range v {
				checkFields(t, path+"."+key, child, elem, fields)
			}
			return
		}
		for key, child := range v {
			i := slices.IndexFunc(fields, func(f k8smodel.Field) bool { return f.Name == key })
			if i < 0 {
				t.Errorf("%s has no field %s", path, key)
				continue
			}
			checkFields(t, path+"."+key, child, fields[i].Type, fields[i].Fields)
		}
	}
}

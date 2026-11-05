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

// Package k8smodel models Substrate's Kubernetes API from its generated
// CustomResourceDefinitions.
package k8smodel

import (
	"cmp"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"sigs.k8s.io/yaml"
)

// API is the Kubernetes API, with its resources sorted by kind.
type API struct {
	Resources []Resource
}

// Resource is the resource one CustomResourceDefinition defines, at the
// version it serves.
type Resource struct {
	// Group is the API group. For example, "ate.dev".
	Group string
	// Version is the served version. For example, "v1".
	Version string
	// Kind is the resource's kind. For example, "WorkerPool".
	Kind string
	// Scope is "Namespaced" or "Cluster".
	Scope string
	// Doc is the schema's description.
	Doc string
	// Fields lists the resource's fields by name, without the apiVersion,
	// kind, and metadata every resource has.
	Fields []Field
}

// Field is one property in a CRD's schema.
type Field struct {
	// Name is the field's serialized name. For example, "replicas".
	Name string
	// Type is ready to render. For example, "string", "int32",
	// "[]object", or "map[string]string".
	Type string
	// Doc is the field's description, followed by the description of its list's
	// items or its map's values.
	Doc string
	// Required is true if the parent's schema lists the field as required.
	Required bool
	// Default is the field's default as JSON, or "" if it has none.
	Default string
	// Enum lists the values the field allows as JSON, or is nil if the
	// schema doesn't list them.
	Enum []string
	// Fields lists the properties of the field's object type, of its list's
	// items, or of its map's values, by name.
	Fields []Field
}

// Build parses sources, one CustomResourceDefinition manifest each.
func Build(sources []string) (*API, error) {
	if len(sources) == 0 {
		return nil, errors.New("no CRDs to model")
	}
	api := &API{}
	for _, source := range sources {
		var crd apiextensionsv1.CustomResourceDefinition
		if err := yaml.Unmarshal([]byte(source), &crd); err != nil {
			return nil, fmt.Errorf("while parsing CRD: %w", err)
		}
		r, err := newResource(crd)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", crd.Name, err)
		}
		api.Resources = append(api.Resources, r)
	}
	slices.SortFunc(api.Resources, func(a, b Resource) int { return cmp.Compare(a.Kind, b.Kind) })
	return api, nil
}

func newResource(crd apiextensionsv1.CustomResourceDefinition) (Resource, error) {
	served := slices.DeleteFunc(slices.Clone(crd.Spec.Versions), func(v apiextensionsv1.CustomResourceDefinitionVersion) bool { return !v.Served })
	if len(served) != 1 {
		return Resource{}, fmt.Errorf("serves %d versions, want 1", len(served))
	}
	version := served[0]
	if version.Schema == nil || version.Schema.OpenAPIV3Schema == nil {
		return Resource{}, fmt.Errorf("version %s has no schema", version.Name)
	}
	schema := version.Schema.OpenAPIV3Schema
	fields, err := newFields(*schema)
	if err != nil {
		return Resource{}, err
	}
	fields = slices.DeleteFunc(fields, func(f Field) bool {
		return f.Name == "apiVersion" || f.Name == "kind" || f.Name == "metadata"
	})
	return Resource{
		Group:   crd.Spec.Group,
		Version: version.Name,
		Kind:    crd.Spec.Names.Kind,
		Scope:   string(crd.Spec.Scope),
		Doc:     schema.Description,
		Fields:  fields,
	}, nil
}

// newFields lists the properties of schema, or of its items or values for a
// list or a map.
func newFields(schema apiextensionsv1.JSONSchemaProps) ([]Field, error) {
	switch {
	case schema.Items != nil && schema.Items.Schema != nil:
		return newFields(*schema.Items.Schema)
	case schema.AdditionalProperties != nil && schema.AdditionalProperties.Schema != nil:
		return newFields(*schema.AdditionalProperties.Schema)
	}
	var fields []Field
	for _, name := range slices.Sorted(maps.Keys(schema.Properties)) {
		prop := schema.Properties[name]
		typ, err := typeName(prop)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", name, err)
		}
		children, err := newFields(prop)
		if err != nil {
			return nil, fmt.Errorf("%s.%w", name, err)
		}
		f := Field{
			Name:     name,
			Type:     typ,
			Doc:      fieldDoc(prop),
			Required: slices.Contains(schema.Required, name),
			Fields:   children,
		}
		if prop.Default != nil {
			f.Default = string(prop.Default.Raw)
		}
		for _, v := range prop.Enum {
			f.Enum = append(f.Enum, string(v.Raw))
		}
		fields = append(fields, f)
	}
	return fields, nil
}

func fieldDoc(schema apiextensionsv1.JSONSchemaProps) string {
	switch {
	case schema.Items != nil && schema.Items.Schema != nil:
		return strings.TrimSpace(schema.Description + "\n\n" + fieldDoc(*schema.Items.Schema))
	case schema.AdditionalProperties != nil && schema.AdditionalProperties.Schema != nil:
		return strings.TrimSpace(schema.Description + "\n\n" + fieldDoc(*schema.AdditionalProperties.Schema))
	default:
		return schema.Description
	}
}

func typeName(schema apiextensionsv1.JSONSchemaProps) (string, error) {
	switch {
	case schema.XIntOrString:
		return "int-or-string", nil
	case schema.Items != nil && schema.Items.Schema != nil:
		elem, err := elemTypeName(*schema.Items.Schema)
		if err != nil {
			return "", err
		}
		return "[]" + elem, nil
	case schema.AdditionalProperties != nil && schema.AdditionalProperties.Schema != nil:
		elem, err := elemTypeName(*schema.AdditionalProperties.Schema)
		if err != nil {
			return "", err
		}
		return "map[string]" + elem, nil
	case schema.Type == "":
		return "", errors.New("has no type")
	case schema.Type == "integer" && schema.Format != "":
		return schema.Format, nil
	default:
		return schema.Type, nil
	}
}

// elemTypeName names the type of a list's items or a map's values. Field
// has no place for their enum or default, so either is an error.
func elemTypeName(schema apiextensionsv1.JSONSchemaProps) (string, error) {
	if len(schema.Enum) > 0 || schema.Default != nil {
		return "", errors.New("sets an enum or default on its items or values")
	}
	return typeName(schema)
}

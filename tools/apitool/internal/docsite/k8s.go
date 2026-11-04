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

package docsite

import (
	"strings"

	"go.ate.dev/substrate/tools/apitool/internal/k8smodel"
)

// k8sFullName names r for anchors and search. For example,
// "ate.dev.v1.WorkerPool".
func k8sFullName(r k8smodel.Resource) string {
	return r.Group + "." + r.Version + "." + r.Kind
}

// k8sAnchor is the anchor of r's page.
func k8sAnchor(r k8smodel.Resource) string {
	return "resource-" + k8sFullName(r)
}

func buildK8sViews(api *k8smodel.API, examples map[string]string) []resourceView {
	views := make([]resourceView, 0, len(api.Resources))
	for _, r := range api.Resources {
		views = append(views, resourceView{
			Name:         r.Kind,
			Doc:          r.Doc,
			APIVersion:   r.Group + "/" + r.Version,
			Scope:        strings.ToLower(r.Scope),
			Anchor:       k8sAnchor(r),
			FieldsAnchor: k8sAnchor(r) + "-fields",
			Attrs:        buildK8sAttrs(r.Fields),
			Example:      trimLicenseHeader(examples[r.Kind]),
		})
	}
	return views
}

func buildK8sAttrs(fields []k8smodel.Field) []attrNode {
	nodes := make([]attrNode, len(fields))
	for i, f := range fields {
		nodes[i] = attrNode{
			Name:     f.Name,
			Type:     f.Type,
			Doc:      f.Doc,
			Tags:     k8sFieldTags(f),
			Children: buildK8sAttrs(f.Fields),
		}
	}
	return nodes
}

func k8sFieldTags(f k8smodel.Field) []string {
	var tags []string
	if f.Required {
		tags = append(tags, "required")
	}
	if f.Default != "" {
		tags = append(tags, "default: "+f.Default)
	}
	if len(f.Enum) > 0 {
		tags = append(tags, "one of: "+strings.Join(f.Enum, ", "))
	}
	return tags
}

func buildK8sTree(api *k8smodel.API) []treeNode {
	nodes := make([]treeNode, 0, len(api.Resources))
	for _, r := range api.Resources {
		nodes = append(nodes, treeNode{Label: r.Kind, Anchor: k8sAnchor(r)})
	}
	return nodes
}

func k8sEntries(api *k8smodel.API) []indexEntry {
	var entries []indexEntry
	for _, r := range api.Resources {
		fullName, anchor := k8sFullName(r), k8sAnchor(r)
		entries = append(entries, indexEntry{Kind: "resource", Name: r.Kind, FullName: fullName, Anchor: anchor})
		entries = append(entries, k8sFieldEntries(r.Fields, fullName, anchor)...)
	}
	return entries
}

// k8sFieldEntries indexes fields at every depth, since a CRD's nested types
// have no sections of their own for the search to find.
func k8sFieldEntries(fields []k8smodel.Field, parent, anchor string) []indexEntry {
	var entries []indexEntry
	for _, f := range fields {
		fullName := parent + "." + f.Name
		entries = append(entries, indexEntry{Kind: "field", Name: f.Name, FullName: fullName, Anchor: anchor})
		entries = append(entries, k8sFieldEntries(f.Fields, fullName, anchor)...)
	}
	return entries
}

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

// Package docsite renders a model.API into an HTML fragment for the project
// website. The fragment needs the stylesheet and script in Assets, and takes
// its colors from CSS variables the site defines.
package docsite

import (
	"bytes"
	"embed"
	"encoding/json"
	"fmt"
	"html/template"
	"maps"
	"slices"
	"strings"

	"google.golang.org/protobuf/reflect/protoreflect"

	"go.ate.dev/substrate/tools/apitool/internal/model"
)

//go:embed template.html.tmpl
var pageTemplate string

// Assets holds the stylesheet and script that Render's fragment needs,
// style.css and search.js.
//
//go:embed style.css search.js
var Assets embed.FS

// indexEntry is one row of the client-side search index. It's marshaled
// with encoding/json's default HTML-escaping (do not disable it): that's
// what keeps a doc comment containing a literal "</script>" from
// prematurely closing the <script> tag it's embedded in.
type indexEntry struct {
	Kind     string `json:"kind"`
	Name     string `json:"name"`
	FullName string `json:"fullName"`
	Anchor   string `json:"anchor"`
}

type pageData struct {
	API           *model.API
	Resources     []resourceView  // one per resourcePage, each with its own attribute tree and methods
	OtherMessages []model.Message // API.Messages minus the resource messages, which get their own Resources view instead
	Index         template.JS     // proto-derived, but already JSON-escaped by buildIndex
}

// resourceView is a resourcePage with everything the template needs
// resolved: anchors, field trees, and proto definitions.
type resourceView struct {
	Page   resourcePage
	Anchor string
	// FieldsAnchor is the anchor of the page's Fields section.
	FieldsAnchor string
	// ParentAnchor links to the parent resource's page, or is "" for a
	// top-level resource or one whose parent is model.GlobalParent.
	ParentAnchor string
	Subresources []resourceLink
	Attrs        []attrNode
	// Example is the resource's example, as JSON, or "" if it has none.
	Example string
	Methods []methodView
}

type resourceLink struct {
	Name      string
	Anchor    string
	Singleton bool
}

type methodView struct {
	model.Method
	InputAnchor  string
	OutputAnchor string
	Params       []attrNode // built from the request message's own fields
	Response     []attrNode // built from the response message's own fields, unless ReturnsResource
	// ReturnsResource is true if the method returns a resource, whose fields
	// its own page documents.
	ReturnsResource bool
	// Sources lists the messages whose proto source is shown beside the
	// method, after its rpc: its request, and its response unless that's a
	// resource, which its own page documents.
	Sources []model.Message
}

// treeNode is one resource page in the site's navigation, with the pages of
// its subresources under it.
type treeNode struct {
	Label    string     `json:"label"`
	Anchor   string     `json:"anchor"`
	Children []treeNode `json:"children,omitempty"`
}

// Page is the rendered API reference.
type Page struct {
	// Body is the HTML fragment, which needs the files in Assets.
	Body []byte
	// Nav lists the resource pages for the site's navigation, as a JSON
	// array of {"label", "anchor", "children"} objects. Each anchor is an
	// element id in Body.
	Nav []byte
}

// Render produces the reference for api, which must be scoped
// to the one service the page documents (e.g. via model.ScopeToService).
// examples holds each resource's example JSON, keyed by message name. A
// resource without one is shown without an example.
func Render(api *model.API, examples map[string]string) (Page, error) {
	sorted := sortedAPI(api)
	pages, err := resourcePages(sorted)
	if err != nil {
		return Page{}, fmt.Errorf("while building resource pages: %w", err)
	}
	anchors := newResourceAnchors(pages)

	index, err := buildIndex(sorted, pages)
	if err != nil {
		return Page{}, fmt.Errorf("while building search index: %w", err)
	}

	tmpl, err := parsePageTemplate(anchors)
	if err != nil {
		return Page{}, fmt.Errorf("while parsing template: %w", err)
	}

	var buf bytes.Buffer
	data := pageData{
		API:           sorted,
		Resources:     buildResourceViews(sorted, pages, examples, anchors),
		OtherMessages: nonResourceMessages(sorted.Messages),
		Index:         template.JS(index),
	}
	if err := tmpl.Execute(&buf, data); err != nil {
		return Page{}, fmt.Errorf("while executing template: %w", err)
	}
	nav, err := json.Marshal(buildResourceTree(pages))
	if err != nil {
		return Page{}, fmt.Errorf("while building navigation: %w", err)
	}
	return Page{Body: buf.Bytes(), Nav: nav}, nil
}

func parsePageTemplate(anchors resourceAnchors) (*template.Template, error) {
	return template.New("apitool").Funcs(template.FuncMap{
		"methodAnchor":  methodAnchor,
		"msgAnchor":     msgAnchor,
		"enumAnchor":    enumAnchor,
		"typeAnchor":    anchors.typeAnchor,
		"fieldAnchor":   anchors.fieldAnchor,
		"highlightJSON": highlightJSON,
		"codeLines":     codeLines,
		"nextAttrID":    newAttrIDFunc(),
		"fullDoc":       fullDoc,
		"summaryDoc":    summaryDoc,
	}).Parse(pageTemplate)
}

func nonResourceMessages(messages []model.Message) []model.Message {
	return slices.DeleteFunc(slices.Clone(messages), func(m model.Message) bool { return m.IsResource() })
}

// viewBuilder resolves resource and method views against the whole API.
type viewBuilder struct {
	pages    []resourcePage
	examples map[string]string
	anchors  resourceAnchors
	messages map[protoreflect.FullName]model.Message
}

func buildResourceViews(api *model.API, pages []resourcePage, examples map[string]string, anchors resourceAnchors) []resourceView {
	b := viewBuilder{pages: pages, examples: examples, anchors: anchors, messages: api.MessagesByFullName()}
	views := make([]resourceView, 0, len(pages))
	for _, page := range pages {
		views = append(views, b.resourceView(page))
	}
	return views
}

func (b viewBuilder) resourceView(page resourcePage) resourceView {
	rv := resourceView{
		Page:         page,
		Anchor:       pageAnchor(page),
		FieldsAnchor: fieldsAnchor(page),
		ParentAnchor: b.anchors.parentAnchor(page),
		Subresources: subresourceLinks(b.pages, page),
		Attrs:        buildAttrTree(b.messages, page.Message.Fields),
		Example:      b.examples[page.Message.Name],
	}
	for _, m := range page.Methods {
		rv.Methods = append(rv.Methods, b.methodView(page, m))
	}
	return rv
}

func (b viewBuilder) methodView(page resourcePage, m model.Method) methodView {
	mv := methodView{
		Method:       m,
		InputAnchor:  b.anchors.typeAnchorOn(page, m.InputName),
		OutputAnchor: b.anchors.typeAnchorOn(page, m.OutputName),
	}
	if req, ok := b.messages[m.InputName]; ok {
		mv.Params = buildAttrTree(b.messages, req.Fields)
		mv.Sources = append(mv.Sources, req)
	}
	if resp, ok := b.messages[m.OutputName]; ok {
		if resp.IsResource() {
			mv.ReturnsResource = true
		} else {
			mv.Response = buildAttrTree(b.messages, resp.Fields)
			mv.Sources = append(mv.Sources, resp)
		}
	}
	return mv
}

// attrNode is one row of a field list, with the fields of its message type,
// or map value type, as Children: what its "Show fields" toggle expands.
// Children is nil for a scalar, an enum, or a message type this API doesn't
// declare, such as google.protobuf.Timestamp.
type attrNode struct {
	Field    model.Field
	Children []attrNode
}

// maxAttrTreeDepth bounds how deep buildAttrTree expands fields.
const maxAttrTreeDepth = 6

// buildAttrTree turns fields into a tree of attrNode, resolving message
// types against messages. A type already on the path from the root isn't
// expanded again, so a recursive message doesn't recurse forever.
func buildAttrTree(messages map[protoreflect.FullName]model.Message, fields []model.Field) []attrNode {
	return buildAttrNodes(messages, fields, map[protoreflect.FullName]bool{}, 0)
}

func buildAttrNodes(messages map[protoreflect.FullName]model.Message, fields []model.Field, ancestors map[protoreflect.FullName]bool, depth int) []attrNode {
	nodes := make([]attrNode, len(fields))
	for i, f := range fields {
		nodes[i] = attrNode{Field: f}

		var childTypeName protoreflect.FullName
		switch {
		case f.Kind == protoreflect.MessageKind && f.TypeFullName != "":
			childTypeName = f.TypeFullName
		case f.MapValueKind == protoreflect.MessageKind && f.MapValueFullName != "":
			childTypeName = f.MapValueFullName
		default:
			continue
		}
		if depth >= maxAttrTreeDepth || ancestors[childTypeName] {
			continue
		}
		child, ok := messages[childTypeName]
		if !ok {
			continue
		}

		nextAncestors := maps.Clone(ancestors)
		nextAncestors[childTypeName] = true
		nodes[i].Children = buildAttrNodes(messages, child.Fields, nextAncestors, depth+1)
	}
	return nodes
}

func subresourceLinks(pages []resourcePage, parent resourcePage) []resourceLink {
	var links []resourceLink
	for _, child := range subresourcePages(pages, parent) {
		links = append(links, resourceLink{Name: child.Message.Name, Anchor: pageAnchor(child), Singleton: child.Singleton})
	}
	return links
}

// buildResourceTree nests pages by parent: top-level resources and those
// whose parent is model.GlobalParent at the root, and every other page
// under each page of its parent resource.
func buildResourceTree(pages []resourcePage) []treeNode {
	var roots []treeNode
	for _, page := range pages {
		if !hasParentPage(page) {
			roots = append(roots, buildTreeNode(pages, page, rootLabel(page), map[string]bool{}))
		}
	}
	return roots
}

// hasParentPage reports whether page nests under another resource's page,
// rather than at the root.
func hasParentPage(page resourcePage) bool {
	return page.Parent != "" && page.Parent != model.GlobalParent
}

func rootLabel(page resourcePage) string {
	if page.Parent == model.GlobalParent {
		return page.Message.Name + " (" + model.GlobalParent + ")"
	}
	return page.Message.Name
}

// buildTreeNode returns page's node. ancestors guards against a cycle of
// parents, which model.Resources doesn't rule out.
func buildTreeNode(pages []resourcePage, page resourcePage, label string, ancestors map[string]bool) treeNode {
	node := treeNode{Label: label, Anchor: pageAnchor(page)}
	if ancestors[page.Message.Name] {
		return node
	}
	ancestors[page.Message.Name] = true
	defer delete(ancestors, page.Message.Name)
	for _, child := range subresourcePages(pages, page) {
		node.Children = append(node.Children, buildTreeNode(pages, child, child.Message.Name, ancestors))
	}
	return node
}

// subresourcePages returns the pages whose parent is parent's resource.
func subresourcePages(pages []resourcePage, parent resourcePage) []resourcePage {
	var out []resourcePage
	for _, page := range pages {
		if page.Parent == parent.Message.Name {
			out = append(out, page)
		}
	}
	return out
}

// resourceAnchors resolves the anchor a link to a message goes to.
type resourceAnchors struct {
	// byName maps a resource's message name to its first page's anchor.
	byName map[string]string
	// byFullName maps a resource's message full name to its first page's
	// Fields section anchor.
	byFullName map[protoreflect.FullName]string
}

func newResourceAnchors(pages []resourcePage) resourceAnchors {
	a := resourceAnchors{byName: map[string]string{}, byFullName: map[protoreflect.FullName]string{}}
	for _, page := range pages {
		if _, ok := a.byFullName[page.Message.FullName]; ok {
			continue
		}
		a.byName[page.Message.Name] = pageAnchor(page)
		a.byFullName[page.Message.FullName] = fieldsAnchor(page)
	}
	return a
}

// typeAnchor is the anchor a link to the message fullName goes to: the
// Fields section of its resource's first page for a resource, or its message
// section otherwise.
func (a resourceAnchors) typeAnchor(fullName protoreflect.FullName) string {
	if anchor, ok := a.byFullName[fullName]; ok {
		return anchor
	}
	return msgAnchor(fullName)
}

// fieldAnchor is the anchor f's type name links to, or "" for a scalar, a
// map, or a type this API doesn't declare.
func (a resourceAnchors) fieldAnchor(f model.Field) string {
	switch {
	case f.TypeFullName == "" || f.TypeIsExternal:
		return ""
	case f.Kind == protoreflect.EnumKind:
		return enumAnchor(f.TypeFullName)
	default:
		return a.typeAnchor(f.TypeFullName)
	}
}

// typeAnchorOn is typeAnchor for a link on page: page's own resource links
// to page's Fields section, not the resource's first page.
func (a resourceAnchors) typeAnchorOn(page resourcePage, fullName protoreflect.FullName) string {
	if fullName == page.Message.FullName {
		return fieldsAnchor(page)
	}
	return a.typeAnchor(fullName)
}

// parentAnchor links to the first page of page's parent resource, or is ""
// for a page at the root of the resource tree.
func (a resourceAnchors) parentAnchor(page resourcePage) string {
	if !hasParentPage(page) {
		return ""
	}
	return a.byName[page.Parent]
}

// sortedAPI returns a copy of api with its services, messages, and enums
// sorted by name. Methods, fields, and enum values keep their declaration
// order.
func sortedAPI(api *model.API) *model.API {
	return &model.API{
		Services: slices.SortedFunc(slices.Values(api.Services), func(a, b model.Service) int { return strings.Compare(string(a.Name), string(b.Name)) }),
		Messages: slices.SortedFunc(slices.Values(api.Messages), func(a, b model.Message) int { return strings.Compare(a.Name, b.Name) }),
		Enums:    slices.SortedFunc(slices.Values(api.Enums), func(a, b model.Enum) int { return strings.Compare(a.Name, b.Name) }),
	}
}

func buildIndex(api *model.API, pages []resourcePage) ([]byte, error) {
	var entries []indexEntry
	for _, page := range pages {
		entries = append(entries, resourcePageEntries(page)...)
	}
	// Resource messages are already indexed above, pointing at their pages.
	for _, msg := range nonResourceMessages(api.Messages) {
		entries = append(entries, messageEntries(msg)...)
	}
	for _, e := range api.Enums {
		entries = append(entries, enumEntries(e)...)
	}
	// See indexEntry on why this keeps json.Marshal's HTML escaping.
	return json.Marshal(entries)
}

func resourcePageEntries(page resourcePage) []indexEntry {
	anchor := pageAnchor(page)
	path := resourcePath(page)
	entries := []indexEntry{{Kind: "resource", Name: page.Message.Name, FullName: string(path), Anchor: anchor}}
	entries = append(entries, fieldEntries(page.Message.Fields, path, anchor)...)
	for _, m := range page.Methods {
		entries = append(entries, indexEntry{Kind: "method", Name: string(m.Name), FullName: string(m.ServiceFullName.Append(m.Name)), Anchor: methodAnchor(m.ServiceFullName, m.Name)})
	}
	return entries
}

func messageEntries(msg model.Message) []indexEntry {
	anchor := msgAnchor(msg.FullName)
	entries := []indexEntry{{Kind: "message", Name: msg.Name, FullName: string(msg.FullName), Anchor: anchor}}
	return append(entries, fieldEntries(msg.Fields, msg.FullName, anchor)...)
}

func fieldEntries(fields []model.Field, parentFullName protoreflect.FullName, anchor string) []indexEntry {
	var entries []indexEntry
	for _, f := range fields {
		entries = append(entries, indexEntry{Kind: "field", Name: string(f.Name), FullName: string(parentFullName.Append(f.Name)), Anchor: anchor})
	}
	return entries
}

func enumEntries(e model.Enum) []indexEntry {
	anchor := enumAnchor(e.FullName)
	entries := []indexEntry{{Kind: "enum", Name: e.Name, FullName: string(e.FullName), Anchor: anchor}}
	for _, v := range e.Values {
		entries = append(entries, indexEntry{Kind: "enum value", Name: string(v.Name), FullName: string(e.FullName.Append(v.Name)), Anchor: anchor})
	}
	return entries
}

func methodAnchor(svcFullName protoreflect.FullName, name protoreflect.Name) string {
	return "method-" + string(svcFullName.Append(name))
}
func msgAnchor(fullName protoreflect.FullName) string  { return "msg-" + string(fullName) }
func enumAnchor(fullName protoreflect.FullName) string { return "enum-" + string(fullName) }
func pageAnchor(page resourcePage) string              { return "resource-" + string(resourcePath(page)) }
func fieldsAnchor(page resourcePage) string            { return pageAnchor(page) + "-fields" }

// resourcePath names page by its resource's full name, with the parent
// inserted before the resource's name for a subresource. For example,
// "substrate.ateapi.v1.Actor", or "substrate.ateapi.v1.Atespace.AccessPolicy".
func resourcePath(page resourcePage) protoreflect.FullName {
	if page.Parent == "" {
		return page.Message.FullName
	}
	pkg := strings.TrimSuffix(string(page.Message.FullName), page.Message.Name)
	return protoreflect.FullName(pkg + page.Parent + "." + page.Message.Name)
}

// newAttrIDFunc returns a template func returning a new id on every call,
// for the children of one attrNode. Ids count up from 1 on every Render, so
// the same API renders the same page.
func newAttrIDFunc() func() string {
	n := 0
	return func() string {
		n++
		return fmt.Sprintf("attr-children-%d", n)
	}
}

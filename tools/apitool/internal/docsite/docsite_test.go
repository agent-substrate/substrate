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

package docsite_test

import (
	"encoding/json"
	"regexp"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/reflect/protoreflect"

	"go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/tools/apitool/internal/docsite"
	"go.ate.dev/substrate/tools/apitool/internal/examples"
	"go.ate.dev/substrate/tools/apitool/internal/model"
)

// fixtureExamples holds an example for each of fixtureAPI's resources.
var fixtureExamples = map[string]string{
	"Widget": `{"widget": 1}`,
	"Gadget": `{"gadget": 2}`,
}

func fixtureAPI(t *testing.T) *model.API {
	do := model.Method{Name: "Do", Comment: "Do it.\n\nIn detail.", InputName: "test.DoRequest", OutputName: "test.DoResponse", Resource: "Widget", ServiceFullName: "test.Svc", ServiceName: "Svc"}
	getGlobalGadget := model.Method{Name: "GetGlobalGadget", InputName: "test.GetGlobalGadgetRequest", OutputName: "test.Gadget", Resource: "Gadget", ServiceFullName: "test.Svc", ServiceName: "Svc"}
	getWidgetGadget := model.Method{Name: "GetWidgetGadget", InputName: "test.GetWidgetGadgetRequest", OutputName: "test.Gadget", Resource: "Gadget", ServiceFullName: "test.Svc", ServiceName: "Svc"}
	return &model.API{
		Services: []model.Service{
			{
				FullName: "test.Svc",
				Name:     "Svc",
				Comment:  "Svc does things.",
				Methods:  []model.Method{do, getGlobalGadget, getWidgetGadget},
			},
		},
		Messages: []model.Message{
			{
				FullName: "test.Foo",
				Name:     "Foo",
				Comment:  `Dangerous: <script>alert(1)</script> & "quotes"`,
				Fields: []model.Field{
					{Name: "scalar_field", TypeDisplay: "string", Kind: protoreflect.StringKind},
					{Name: "message_field", TypeDisplay: "DoRequest", Kind: protoreflect.MessageKind, TypeFullName: "test.DoRequest"},
					{Name: "enum_field", TypeDisplay: "Color", Kind: protoreflect.EnumKind, TypeFullName: "test.Color"},
				},
			},
			{FullName: "test.DoRequest", Name: "DoRequest", Fields: []model.Field{
				{Name: "token", Number: 1, TypeDisplay: "string", Kind: protoreflect.StringKind, Redacted: true},
			}},
			{FullName: "test.DoResponse", Name: "DoResponse"},
			{FullName: "test.Widget", Name: "Widget", ResourceAnnotation: &model.ResourceAnnotation{Scope: ateapi.Scope_SCOPE_ATESPACE, Plural: "Widgets"}},
			{FullName: "test.Gadget", Name: "Gadget", ResourceAnnotation: &model.ResourceAnnotation{Parents: []string{"Global", "Widget"}, Singleton: true, Plural: "Gadgets"}},
		},
		Enums: []model.Enum{
			{
				FullName: "test.Color",
				Name:     "Color",
				Values:   []model.EnumValue{{Name: "RED", Number: 1}},
			},
		},
	}
}

func TestRender_Anchors(t *testing.T) {
	out, err := docsite.Render(fixtureAPI(t), fixtureExamples)
	if err != nil {
		t.Fatalf("Render() error = %v", err)
	}
	html := string(out.Body)

	for _, id := range []string{
		`id="method-test.Svc.Do"`,
		`id="method-test.Svc.GetGlobalGadget"`,
		`id="method-test.Svc.GetWidgetGadget"`,
		`id="resource-test.Widget"`,
		`id="resource-test.Global.Gadget"`,
		`id="resource-test.Widget.Gadget"`,
		`id="resource-test.Widget-fields"`,
		`id="resource-test.Global.Gadget-fields"`,
		`id="resource-test.Widget.Gadget-fields"`,
		`id="msg-test.Foo"`,
		`id="msg-test.DoRequest"`,
		`id="msg-test.DoResponse"`,
		`id="enum-test.Color"`,
	} {
		if !strings.Contains(html, id) {
			t.Errorf("output missing anchor %s", id)
		}
	}
}

func TestRender_ResourceLinks(t *testing.T) {
	out, err := docsite.Render(fixtureAPI(t), fixtureExamples)
	if err != nil {
		t.Fatalf("Render() error = %v", err)
	}
	html := string(out.Body)

	for _, tc := range []struct {
		name, want string
	}{
		{"methods table under the resource header", `<section class="page-section single">`},
		{"method returning a resource links it instead of listing its fields", `<p class="description">The <a href="#resource-test.Global.Gadget-fields">Gadget</a> resource.</p>`},
		{"request heading links its message", `<h3 class="group-title">Request <span class="group-type mono">(<a href="#msg-test.DoRequest">DoRequest</a>)</span></h3>`},
		{"response heading links its message", `<h3 class="group-title">Response <span class="group-type mono">(<a href="#msg-test.DoResponse">DoResponse</a>)</span></h3>`},
		{"methods table shows the summary", `<td><p>Do it.</p>
</td>`},
		{"method section shows the whole doc", `<div class="description"><p>Do it.</p>
<p>In detail.</p>`},
		{"parent page lists its subresource", `<li><a class="mono" href="#resource-test.Widget.Gadget">Gadget</a> <span class="tag">singleton</span></li>`},
		{"subresource page links its parent", `subresource of <a class="mono" href="#resource-test.Widget">Widget</a>`},
		{"global parent is not a link", `subresource of <span class="mono">Global</span>`},
		{"response links to the fields on the method's own page", `<td class="mono"><a href="#resource-test.Global.Gadget-fields">Gadget</a></td>`},
		{"response links to the fields on the other parent's page", `<td class="mono"><a href="#resource-test.Widget.Gadget-fields">Gadget</a></td>`},
		{"definition links a message type", `<a class="tok-type" href="#msg-test.DoRequest">DoRequest</a>`},
		{"definition links a resource type to the fields on its first page", `<a class="tok-type" href="#resource-test.Global.Gadget-fields">Gadget</a>`},
		{"definition leaves a scalar field type unlinked", `<span class="tok-type">string</span> token`},
		{"field links its message type", `<a href="#msg-test.DoRequest">DoRequest</a>`},
		{"field links its enum type", `<a href="#enum-test.Color">Color</a>`},
		{"scalar field type is not a link", `<td class="mono">string</td>`},
	} {
		if !strings.Contains(html, tc.want) {
			t.Errorf("%s: output missing %s", tc.name, tc.want)
		}
	}
}

func TestRender_Nav(t *testing.T) {
	out, err := docsite.Render(fixtureAPI(t), fixtureExamples)
	if err != nil {
		t.Fatalf("Render() error = %v", err)
	}

	type node struct {
		Label    string `json:"label"`
		Anchor   string `json:"anchor"`
		Children []node `json:"children"`
	}
	var got []node
	if err := json.Unmarshal(out.Nav, &got); err != nil {
		t.Fatalf("json.Unmarshal(Nav) error = %v", err)
	}
	want := []node{
		{Label: "Gadget (Global)", Anchor: "resource-test.Global.Gadget"},
		{Label: "Widget", Anchor: "resource-test.Widget", Children: []node{
			{Label: "Gadget", Anchor: "resource-test.Widget.Gadget"},
		}},
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("Nav mismatch (-want +got):\n%s", diff)
	}
}

func TestRender_Definitions(t *testing.T) {
	out, err := docsite.Render(fixtureAPI(t), fixtureExamples)
	if err != nil {
		t.Fatalf("Render() error = %v", err)
	}
	// Drop the highlighting spans and type links, leaving each definition's
	// and example's plain text.
	html := regexp.MustCompile(`</?(span|a)( [^>]*)?>`).ReplaceAllString(string(out.Body), "")

	for _, tc := range []struct {
		name, definition string
		count            int
	}{
		{"example beside a resource's fields", "{&#34;widget&#34;: 1}", 1},
		{"example on each of a resource's pages", "{&#34;gadget&#34;: 2}", 2},
		{"resource definition not beside a method returning it", "message Gadget {}", 0},
		{"resource definition not beside its fields", "message Widget {}", 0},
		{"rpc beside its method", "rpc Do(DoRequest) returns (DoResponse);", 1},
		{"request beside its method", "message DoRequest {  string token = 1 [debug_redact = true];}", 1},
		{"non-resource response beside its method", "message DoResponse {}", 1},
	} {
		if got := strings.Count(html, "<pre>"+tc.definition+"</pre>"); got != tc.count {
			t.Errorf("%s: %q rendered %d times, want %d", tc.name, tc.definition, got, tc.count)
		}
	}
}

func TestRender_MissingExample(t *testing.T) {
	out, err := docsite.Render(fixtureAPI(t), map[string]string{"Widget": `{}`})
	if err != nil {
		t.Fatalf("Render() error = %v", err)
	}
	html := string(out.Body)
	if got := strings.Count(html, "<h2>Example</h2>"); got != 1 {
		t.Errorf("example cards = %d, want 1, Widget's only", got)
	}
	if want := `<section class="page-section single" id="resource-test.Widget.Gadget-fields">`; !strings.Contains(html, want) {
		t.Errorf("output missing %s, Gadget's fields without an example", want)
	}
}

// TestRender_EscapesComments is a regression test for the search index's
// safety property: the dangerous comment text must never appear as a raw,
// executable <script> close inside the page - only inside the one
// legitimate embedded <script> block, the JSON index, which is
// tool-authored, not proto-derived.
func TestRender_EscapesComments(t *testing.T) {
	out, err := docsite.Render(fixtureAPI(t), fixtureExamples)
	if err != nil {
		t.Fatalf("Render() error = %v", err)
	}
	html := string(out.Body)

	if strings.Contains(html, "<script>alert(1)</script>") {
		t.Error("output contains the dangerous comment text unescaped")
	}
	if !strings.Contains(html, "&amp;") {
		t.Error("output does not escape '&' from the comment")
	}
	if got := strings.Count(html, "</script>"); got != 1 {
		t.Errorf("literal </script> count = %d, want 1 (the JSON index close only)", got)
	}
}

func TestRender_SearchIndexRoundTrips(t *testing.T) {
	out, err := docsite.Render(fixtureAPI(t), fixtureExamples)
	if err != nil {
		t.Fatalf("Render() error = %v", err)
	}
	html := string(out.Body)

	const openTag = `<script id="apitool-index" type="application/json">`
	start := strings.Index(html, openTag)
	if start < 0 {
		t.Fatal("search index <script> tag not found in output")
	}
	rest := html[start+len(openTag):]
	end := strings.Index(rest, "</script>")
	if end < 0 {
		t.Fatal("search index <script> tag not closed in output")
	}

	var entries []struct {
		Kind     string `json:"kind"`
		Name     string `json:"name"`
		FullName string `json:"fullName"`
		Anchor   string `json:"anchor"`
	}
	if err := json.Unmarshal([]byte(rest[:end]), &entries); err != nil {
		t.Fatalf("json.Unmarshal(search index) error = %v", err)
	}

	// 3 resource pages + 3 methods + 3 messages + 4 fields (Foo's and
	// DoRequest's) + 1 enum + 1 enum value.
	if want := 15; len(entries) != want {
		t.Fatalf("len(entries) = %d, want %d", len(entries), want)
	}

	want := []struct {
		Kind, Name, FullName, Anchor string
	}{
		{"message", "Foo", "test.Foo", "msg-test.Foo"},
		{"field", "scalar_field", "test.Foo.scalar_field", "msg-test.Foo"},
		{"enum value", "RED", "test.Color.RED", "enum-test.Color"},
		{"resource", "Gadget", "test.Global.Gadget", "resource-test.Global.Gadget"},
		{"method", "GetWidgetGadget", "test.Svc.GetWidgetGadget", "method-test.Svc.GetWidgetGadget"},
	}
	for _, w := range want {
		found := false
		for _, e := range entries {
			if e.Kind == w.Kind && e.Name == w.Name && e.FullName == w.FullName && e.Anchor == w.Anchor {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("search index missing entry %+v", w)
		}
	}
}

// TestRender_RealAPISmoke exercises model.Build + docsite.Render together
// at real scale, against ateapi.proto's actual Control service.
func TestRender_RealAPISmoke(t *testing.T) {
	api, err := model.Build(t.Context(), ateapi.Source)
	if err != nil {
		t.Fatalf("model.Build() error = %v", err)
	}
	ex, err := examples.ByResource()
	if err != nil {
		t.Fatalf("examples.ByResource() error = %v", err)
	}
	out, err := docsite.Render(model.ScopeToService(api, "Control"), ex)
	if err != nil {
		t.Fatalf("Render() error = %v", err)
	}
	if !strings.Contains(string(out.Body), "substrate.ateapi.v1.Actor") {
		t.Error("output does not mention substrate.ateapi.v1.Actor")
	}
}

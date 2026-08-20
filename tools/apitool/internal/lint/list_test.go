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

package lint_test

import (
	"testing"

	"github.com/agent-substrate/substrate/tools/apitool/internal/lint"
	"github.com/agent-substrate/substrate/tools/apitool/internal/model"
)

func listAPI(reqFields, respFields []model.Field) *model.API {
	return &model.API{
		Services: []model.Service{{
			Name: "Control",
			Methods: []model.Method{
				{Name: "ListWidgets", ServiceName: "Control", InputName: "test.ListWidgetsRequest", OutputName: "test.ListWidgetsResponse"},
			},
		}},
		Messages: []model.Message{
			{FullName: "test.ListWidgetsRequest", Name: "ListWidgetsRequest", Fields: reqFields},
			{FullName: "test.ListWidgetsResponse", Name: "ListWidgetsResponse", Fields: respFields},
		},
	}
}

func TestListMethodShape(t *testing.T) {
	compliantReq := []model.Field{{Name: "page_size"}, {Name: "page_token"}}
	compliantResp := []model.Field{
		{Name: "widgets", Repeated: true, TypeKind: "message", TypeFullName: "test.Widget"},
		{Name: "next_page_token"},
	}

	tests := []struct {
		name           string
		reqFields      []model.Field
		respFields     []model.Field
		wantViolations int
	}{
		{"compliant", compliantReq, compliantResp, 0},
		{
			name:           "missing page_size and page_token",
			reqFields:      nil,
			respFields:     compliantResp,
			wantViolations: 2,
		},
		{
			name:      "missing repeated field and next_page_token",
			reqFields: compliantReq,
			respFields: []model.Field{
				{Name: "widget", TypeKind: "message", TypeFullName: "test.Widget"}, // not repeated
			},
			// no repeated message field found, no next_page_token, plus "widget"
			// itself is a stray field since it doesn't qualify as either.
			wantViolations: 3,
		},
		{
			name:      "repeated scalar field doesn't satisfy the rule",
			reqFields: compliantReq,
			respFields: []model.Field{
				{Name: "ids", Repeated: true}, // repeated, but not a message
				{Name: "next_page_token"},
			},
			// no repeated message field found, plus "ids" is itself a stray field.
			wantViolations: 2,
		},
		{
			name:      "repeated field misnamed - doesn't match the method's plural",
			reqFields: compliantReq,
			respFields: []model.Field{
				{Name: "items", Repeated: true, TypeKind: "message", TypeFullName: "test.Widget"}, // want "widgets"
				{Name: "next_page_token"},
			},
			wantViolations: 1,
		},
		{
			name:           "atespace field is allowed on the request",
			reqFields:      append([]model.Field{{Name: "atespace"}}, compliantReq...),
			respFields:     compliantResp,
			wantViolations: 0,
		},
		{
			name:           "stray field on the request - sorting/filtering is not supported",
			reqFields:      append(append([]model.Field{}, compliantReq...), model.Field{Name: "filter"}),
			respFields:     compliantResp,
			wantViolations: 1,
		},
		{
			name:           "stray field on the response",
			reqFields:      compliantReq,
			respFields:     append(append([]model.Field{}, compliantResp...), model.Field{Name: "total_count"}),
			wantViolations: 1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			findings, err := lint.ListMethodShape.Check(listAPI(tt.reqFields, tt.respFields))
			if err != nil {
				t.Fatalf("Check() error = %v", err)
			}
			if len(findings) != tt.wantViolations {
				t.Errorf("Check() = %+v, want %d finding(s)", findings, tt.wantViolations)
			}
		})
	}
}

// TestListMethodShape_IgnoresNonListMethods confirms non-"List*" methods
// aren't checked.
func TestListMethodShape_IgnoresNonListMethods(t *testing.T) {
	api := &model.API{
		Services: []model.Service{{
			Name: "Control",
			Methods: []model.Method{
				{Name: "GetWidget", ServiceName: "Control", InputName: "test.GetWidgetRequest", OutputName: "test.Widget"},
			},
		}},
		Messages: []model.Message{
			{FullName: "test.GetWidgetRequest", Name: "GetWidgetRequest"},
			{FullName: "test.Widget", Name: "Widget"},
		},
	}
	findings, err := lint.ListMethodShape.Check(api)
	if err != nil {
		t.Fatalf("Check() error = %v", err)
	}
	if len(findings) != 0 {
		t.Errorf("Check() = %+v, want no findings for a non-List method", findings)
	}
}

func TestListResponseNameMatchesMethod(t *testing.T) {
	tests := []struct {
		name         string
		responseName string
		wantFinding  bool
	}{
		{"compliant", "ListWidgetsResponse", false},
		{"wrong name", "WidgetsResponse", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			api := listAPI(nil, nil)
			api.Messages[1].Name = tt.responseName // test.ListWidgetsResponse
			findings, err := lint.ListResponseNameMatchesMethod.Check(api)
			if err != nil {
				t.Fatalf("Check() error = %v", err)
			}
			if got := len(findings) > 0; got != tt.wantFinding {
				t.Errorf("Check() findings = %+v, want a finding: %v", findings, tt.wantFinding)
			}
		})
	}
}

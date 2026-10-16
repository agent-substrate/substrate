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

package model_test

import (
	"testing"

	"github.com/google/go-cmp/cmp"

	"go.ate.dev/substrate/tools/apitool/internal/model"
)

func TestDVTags(t *testing.T) {
	tests := []struct {
		name    string
		comment string
		want    []model.DVTag
		wantErr bool
	}{
		{
			name:    "no tags",
			comment: "volumes the actor mounts.",
		},
		{
			name:    "tag without a value",
			comment: "+k8s:required",
			want:    []model.DVTag{{Name: "required"}},
		},
		{
			name:    "tag with a value",
			comment: "+k8s:maxItems=32",
			want:    []model.DVTag{{Name: "maxItems", Value: "32"}},
		},
		{
			name:    "tag with a note",
			comment: "+k8s:maxItems=32 # matches ateapi.ActorTemplate.volumes",
			want:    []model.DVTag{{Name: "maxItems", Value: "32", Note: "matches ateapi.ActorTemplate.volumes"}},
		},
		{
			name:    "tag applied to each item",
			comment: "+k8s:eachVal=+k8s:maxLength=253",
			want:    []model.DVTag{{Name: "eachVal=+k8s:maxLength", Value: "253"}},
		},
		{
			name:    "chained tag without a value",
			comment: `+k8s:alpha(since: "0.0")=+k8s:monotonic # TODO: get rid of alpha prefix`,
			want:    []model.DVTag{{Name: `alpha(since: "0.0")=+k8s:monotonic`, Note: "TODO: get rid of alpha prefix"}},
		},
		{
			name:    "chained tag with a value",
			comment: `+k8s:alpha(since: "0.0")=+k8s:maxItems=32 # matches ateapi.ActorTemplate.volumes`,
			want:    []model.DVTag{{Name: `alpha(since: "0.0")=+k8s:maxItems`, Value: "32", Note: "matches ateapi.ActorTemplate.volumes"}},
		},
		{
			name:    "chained tag with a quoted value",
			comment: `+k8s:ifEnabled(Foo)=+k8s:validateError="a=b # c" # matches ateapi.Foo.bar`,
			want:    []model.DVTag{{Name: `ifEnabled("Foo")=+k8s:validateError`, Value: "a=b # c", Note: "matches ateapi.Foo.bar"}},
		},
		{
			name:    "prose and tags",
			comment: "volumes the actor mounts.\n\n+k8s:optional\n+k8s:maxItems=32\nnot a +k8s:tag mid-line",
			want:    []model.DVTag{{Name: "optional"}, {Name: "maxItems", Value: "32"}},
		},
		{
			name:    "comment without a space before it",
			comment: "+k8s:maxItems=32# matches ateapi.ActorTemplate.volumes",
			want:    []model.DVTag{{Name: "maxItems", Value: "32", Note: "matches ateapi.ActorTemplate.volumes"}},
		},
		{
			name:    "int in hex",
			comment: "+k8s:maxItems=0x20",
			want:    []model.DVTag{{Name: "maxItems", Value: "32"}},
		},
		{
			name:    "quoted value holding '=' and '#'",
			comment: `+k8s:validateError="a=b # c" # matches ateapi.Foo.bar`,
			want:    []model.DVTag{{Name: "validateError", Value: "a=b # c", Note: "matches ateapi.Foo.bar"}},
		},
		{
			name:    "args in canonical form",
			comment: "+k8s:item(key:`a=b`,count:1)=+k8s:maxLength=8",
			want:    []model.DVTag{{Name: `item(key: "a=b", count: 1)=+k8s:maxLength`, Value: "8"}},
		},
		{
			name:    "chained tag with args",
			comment: "+k8s:ifEnabled(Foo)=+k8s:eachVal=+k8s:maxLength=5",
			want:    []model.DVTag{{Name: `ifEnabled("Foo")=+k8s:eachVal=+k8s:maxLength`, Value: "5"}},
		},
		{
			name:    "identifier value",
			comment: "+k8s:format=k8s-short-name",
			want:    []model.DVTag{{Name: "format", Value: "k8s-short-name"}},
		},
		{
			name:    "bool value",
			comment: "+k8s:enabled=true",
			want:    []model.DVTag{{Name: "enabled", Value: "true"}},
		},
		{
			name:    "negative int",
			comment: "+k8s:minimum=-1",
			want:    []model.DVTag{{Name: "minimum", Value: "-1"}},
		},
		{
			name:    "backtick-quoted value",
			comment: "+k8s:validateError=`a=b`",
			want:    []model.DVTag{{Name: "validateError", Value: "a=b"}},
		},
		{
			name:    "arg without a value",
			comment: "+k8s:dependentRequired(other)",
			want:    []model.DVTag{{Name: `dependentRequired("other")`}},
		},
		{
			name:    "'#' inside a quoted arg",
			comment: `+k8s:item(key: "a#b")=+k8s:required # matches ateapi.Foo.bar`,
			want:    []model.DVTag{{Name: `item(key: "a#b")=+k8s:required`, Note: "matches ateapi.Foo.bar"}},
		},
		{
			name:    "indented tag line",
			comment: "\t  +k8s:maxItems=32",
			want:    []model.DVTag{{Name: "maxItems", Value: "32"}},
		},
		{
			name:    "empty note",
			comment: "+k8s:maxItems=32 #",
			want:    []model.DVTag{{Name: "maxItems", Value: "32"}},
		},
		{
			name:    "int past int64",
			comment: "+k8s:maxItems=99999999999999999999",
			wantErr: true,
		},
		{
			name:    "malformed tag",
			comment: "+k8s:maxLength=\"unterminated",
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := model.DVTags(tt.comment)
			if (err != nil) != tt.wantErr {
				t.Fatalf("DVTags() error = %v, wantErr %v", err, tt.wantErr)
			}
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Errorf("DVTags() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

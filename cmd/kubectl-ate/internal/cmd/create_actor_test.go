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
	"testing"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/testing/protocmp"
)

func TestBuildCreateActorRequest(t *testing.T) {
	tests := []struct {
		name        string
		templateRef string
		tag         string
		want        *ateapipb.Actor
		wantErr     bool
	}{
		{
			name:        "bare template name defaults to the actor's atespace",
			templateRef: "counter",
			want: &ateapipb.Actor{
				Metadata:      &ateapipb.ResourceMetadata{Atespace: "demo", Name: "my-counter"},
				ActorTemplate: &ateapipb.ObjectRef{Atespace: "demo", Name: "counter"},
			},
		},
		{
			name:        "bare tag name defaults to the actor's atespace",
			templateRef: "counter",
			tag:         "before-upgrade",
			want: &ateapipb.Actor{
				Metadata:      &ateapipb.ResourceMetadata{Atespace: "demo", Name: "my-counter"},
				ActorTemplate: &ateapipb.ObjectRef{Atespace: "demo", Name: "counter"},
				SourceTag:     &ateapipb.ObjectRef{Atespace: "demo", Name: "before-upgrade"},
			},
		},
		{
			name:        "qualified tag in a different atespace",
			templateRef: "counter",
			tag:         "other-atespace/before-upgrade",
			want: &ateapipb.Actor{
				Metadata:      &ateapipb.ResourceMetadata{Atespace: "demo", Name: "my-counter"},
				ActorTemplate: &ateapipb.ObjectRef{Atespace: "demo", Name: "counter"},
				SourceTag:     &ateapipb.ObjectRef{Atespace: "other-atespace", Name: "before-upgrade"},
			},
		},
		{
			name:        "qualified template in a different atespace",
			templateRef: "shared-templates/counter",
			want: &ateapipb.Actor{
				Metadata:      &ateapipb.ResourceMetadata{Atespace: "demo", Name: "my-counter"},
				ActorTemplate: &ateapipb.ObjectRef{Atespace: "shared-templates", Name: "counter"},
			},
		},
		{name: "malformed template ref", templateRef: "a/b/c", wantErr: true},
		{name: "malformed tag", templateRef: "counter", tag: "a/b/c", wantErr: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := buildCreateActorRequest("my-counter", "demo", test.templateRef, test.tag)
			if (err != nil) != test.wantErr {
				t.Fatalf("buildCreateActorRequest error = %v, wantErr %t", err, test.wantErr)
			}
			if test.wantErr {
				return
			}
			want := &ateapipb.CreateActorRequest{Actor: test.want}
			if diff := cmp.Diff(want, got, protocmp.Transform()); diff != "" {
				t.Errorf("request mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

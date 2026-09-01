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
		snapshotTag string
		want        *ateapipb.Actor
		wantErr     bool
	}{
		{
			name:        "template ref",
			templateRef: "counter",
			want: &ateapipb.Actor{
				Metadata:      &ateapipb.ResourceMetadata{Atespace: "demo", Name: "my-counter"},
				ActorTemplate: &ateapipb.ObjectRef{Atespace: "demo", Name: "counter"},
			},
		},
		{
			name:        "template ref with snapshot tag",
			templateRef: "counter",
			snapshotTag: "demo/before-upgrade",
			want: &ateapipb.Actor{
				Metadata:          &ateapipb.ResourceMetadata{Atespace: "demo", Name: "my-counter"},
				ActorTemplate:     &ateapipb.ObjectRef{Atespace: "demo", Name: "counter"},
				SourceSnapshotTag: &ateapipb.ObjectRef{Atespace: "demo", Name: "before-upgrade"},
			},
		},
		{name: "malformed snapshot tag", templateRef: "counter", snapshotTag: "before-upgrade", wantErr: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := buildCreateActorRequest("my-counter", "demo", test.templateRef, test.snapshotTag)
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

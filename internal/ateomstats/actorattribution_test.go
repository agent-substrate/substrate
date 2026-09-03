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

package ateomstats

import (
	"testing"

	"github.com/google/go-cmp/cmp"

	"github.com/agent-substrate/substrate/internal/proto/ateompb"
	"github.com/agent-substrate/substrate/internal/resources"
)

// fullAttribution is what every fully-populated request below should produce. The
// five values are deliberately distinct strings: the failure this test exists to
// catch is a field being wired to the wrong source, which same-looking
// placeholders would hide.
var fullAttribution = resources.ActorAttribution{
	Ref:              resources.ActorRef{Atespace: "atespace-a", Name: "actor-b"},
	UID:              "uid-c",
	TemplateAtespace: "template-ns-d",
	TemplateName:     "template-name-e",
}

func TestActorAttributionFromRequest(t *testing.T) {
	tests := []struct {
		name string
		req  attributionSource
		want resources.ActorAttribution
	}{
		{
			name: "run request",
			req: &ateompb.RunWorkloadRequest{
				Atespace:              "atespace-a",
				ActorName:             "actor-b",
				ActorUid:              "uid-c",
				ActorTemplateAtespace: "template-ns-d",
				ActorTemplateName:     "template-name-e",
			},
			want: fullAttribution,
		},
		{
			// Restore has to yield the same attribution as Run for the same actor:
			// a sample taken after a restore must be attributable to the actor
			// it was taken before the checkpoint.
			name: "restore request",
			req: &ateompb.RestoreWorkloadRequest{
				Atespace:              "atespace-a",
				ActorName:             "actor-b",
				ActorUid:              "uid-c",
				ActorTemplateAtespace: "template-ns-d",
				ActorTemplateName:     "template-name-e",
			},
			want: fullAttribution,
		},
		{
			name: "checkpoint request",
			req: &ateompb.CheckpointWorkloadRequest{
				Atespace:              "atespace-a",
				ActorName:             "actor-b",
				ActorUid:              "uid-c",
				ActorTemplateAtespace: "template-ns-d",
				ActorTemplateName:     "template-name-e",
			},
			want: fullAttribution,
		},
		{
			name: "terminate request",
			req: &ateompb.TerminateWorkloadRequest{
				Atespace:              "atespace-a",
				ActorName:             "actor-b",
				ActorUid:              "uid-c",
				ActorTemplateAtespace: "template-ns-d",
				ActorTemplateName:     "template-name-e",
			},
			want: fullAttribution,
		},
		{
			name: "empty run request",
			req:  &ateompb.RunWorkloadRequest{},
			want: resources.ActorAttribution{},
		},
		{
			name: "empty terminate request",
			req:  &ateompb.TerminateWorkloadRequest{},
			want: resources.ActorAttribution{},
		},
		{
			// The callers hold s.lock and are not defensive about the request
			// pointer; the generated getters are nil-safe, and the zero value
			// is the honest answer. Pinned so a hand-written getter or a switch
			// to direct field access does not turn this into a panic.
			name: "nil run request",
			req:  (*ateompb.RunWorkloadRequest)(nil),
			want: resources.ActorAttribution{},
		},
		{
			name: "nil restore request",
			req:  (*ateompb.RestoreWorkloadRequest)(nil),
			want: resources.ActorAttribution{},
		},
		{
			name: "nil checkpoint request",
			req:  (*ateompb.CheckpointWorkloadRequest)(nil),
			want: resources.ActorAttribution{},
		},
		{
			name: "nil terminate request",
			req:  (*ateompb.TerminateWorkloadRequest)(nil),
			want: resources.ActorAttribution{},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := ActorAttributionFromRequest(tc.req)
			if diff := cmp.Diff(tc.want, got); diff != "" {
				t.Errorf("ActorAttributionFromRequest() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

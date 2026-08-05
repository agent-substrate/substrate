//go:build linux

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

package main

import (
	"context"
	"testing"

	"github.com/google/go-cmp/cmp"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/agent-substrate/substrate/internal/ateomstats"
	"github.com/agent-substrate/substrate/internal/proto/ateompb"
	"github.com/agent-substrate/substrate/internal/resources"
)

// TestActorBootParamsAttribution pins the mapping from actorBootParams onto the
// attribution the service retains. The three loose fields are the ones most
// likely to get crossed, since actorBootParams names them differently than
// ActorAttribution does; the distinct placeholders below make a swap visible.
func TestActorBootParamsAttribution(t *testing.T) {
	tests := []struct {
		name string
		p    actorBootParams
		want ateomstats.ActorAttribution
	}{
		{
			name: "fully populated",
			p: actorBootParams{
				actorRef:     resources.ActorRef{Atespace: "atespace-a", Name: "actor-b"},
				actorUID:     "uid-c",
				templateNS:   "template-ns-d",
				templateName: "template-name-e",
			},
			want: ateomstats.ActorAttribution{
				Ref:               resources.ActorRef{Atespace: "atespace-a", Name: "actor-b"},
				UID:               "uid-c",
				TemplateNamespace: "template-ns-d",
				TemplateName:      "template-name-e",
			},
		},
		{
			name: "zero params",
			p:    actorBootParams{},
			want: ateomstats.ActorAttribution{},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if diff := cmp.Diff(tc.want, tc.p.actorAttribution()); diff != "" {
				t.Errorf("actorBootParams.actorAttribution() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

// TestActorBootParamsAttributionMatchesRequest checks the two hops the
// attribution makes — request to actorBootParams (in RunWorkload /
// RestoreWorkload) and actorBootParams to ActorAttribution — compose back into
// what the caller sent. The two hops are written in different files, so this is
// the assertion that catches them drifting apart.
func TestActorBootParamsAttributionMatchesRequest(t *testing.T) {
	req := &ateompb.RunWorkloadRequest{
		Atespace:               "atespace-a",
		ActorName:              "actor-b",
		ActorUid:               "uid-c",
		ActorTemplateNamespace: "template-ns-d",
		ActorTemplateName:      "template-name-e",
	}

	// Mirrors the actorBootParams literal in RunWorkload and RestoreWorkload.
	p := actorBootParams{
		actorRef:     resources.ActorRef{Atespace: req.GetAtespace(), Name: req.GetActorName()},
		actorUID:     req.GetActorUid(),
		templateNS:   req.GetActorTemplateNamespace(),
		templateName: req.GetActorTemplateName(),
	}

	if diff := cmp.Diff(ateomstats.ActorAttributionFromRequest(req), p.actorAttribution()); diff != "" {
		t.Errorf("attribution via actorBootParams differs from attribution via request (-request +params):\n%s", diff)
	}
}

// TestGetWorkloadStatsUnimplemented pins the stub's advertised contract. The
// guest agent read replaces this body; until then a caller that gets any other code
// back would be reading numbers that are not there.
//
// The retention this stub will eventually read — s.activeActor, set by
// RunWorkload / RestoreWorkload and cleared by CheckpointWorkload — has no unit
// test, for the same reason as on the gVisor side: those three RPCs reach for
// netlink, cloud-hypervisor, and the worker pod's netns within a few lines of
// entry and cannot be driven from `go test`. Its mapping is covered above and
// in internal/ateomstats; the transitions are verified end to end once
// GetWorkloadStats returns real data.
func TestGetWorkloadStatsUnimplemented(t *testing.T) {
	s := &AteomService{}

	resp, err := s.GetWorkloadStats(context.Background(), &ateompb.GetWorkloadStatsRequest{ActorUid: "uid-c"})
	if resp != nil {
		t.Errorf("GetWorkloadStats() returned response %v, want nil", resp)
	}
	if got := status.Code(err); got != codes.Unimplemented {
		t.Errorf("GetWorkloadStats() error code = %v, want %v (err: %v)", got, codes.Unimplemented, err)
	}
}

// TestAteomServiceStartsAvailable checks that a freshly constructed service
// retains no attribution, mirroring the gVisor ateom's test of the same name.
// GetWorkloadStats's FAILED_PRECONDITION-when-available behavior is built on
// this: a non-nil zero value here would make an idle ateom report an empty
// actor's usage instead of refusing.
func TestAteomServiceStartsAvailable(t *testing.T) {
	if s := (&AteomService{}); s.activeActor != nil {
		t.Errorf("new AteomService.activeActor = %v, want nil", s.activeActor)
	}
}

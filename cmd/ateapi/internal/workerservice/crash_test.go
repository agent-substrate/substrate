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

package workerservice

import (
	"context"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	"google.golang.org/grpc/codes"

	"go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/cmd/ateapi/internal/ateletauth/ateletauthtest"
	"go.ate.dev/substrate/cmd/ateapi/internal/store/storetest"
	"go.ate.dev/substrate/internal/apierror"
	"go.ate.dev/substrate/internal/resources"
)

func crashRequest(actorUID string) *ateapi.ReportActorCrashedRequest {
	return &ateapi.ReportActorCrashedRequest{
		Worker:   &ateapi.ObjectRef{Name: testWorkerName},
		Actor:    &ateapi.ObjectRef{Atespace: suspendAtespace, Name: suspendActor},
		ActorUid: actorUID,
		Message:  "cloud-hypervisor exited: signal: killed",
	}
}

func TestReportActorCrashed(t *testing.T) {
	st, cleanup := storetest.SetupTestStore(t)
	defer cleanup()
	seedReportedWorker(t, st, testNode, &ateapi.WorkerResources{Actors: 1})
	actor := seedHostedActor(t, st, testWorkerName)
	uid := actor.GetMetadata().GetUid()
	control := &fakeControl{crashResp: actor}
	s := New(st, control, testAteletSPIFFEID, nil, "", nil)

	if _, err := s.ReportActorCrashed(ateletauthtest.ContextWith(ateletauthtest.CertOn(t, testNode)), crashRequest(uid)); err != nil {
		t.Fatalf("ReportActorCrashed() failed: %v", err)
	}
	want := []crashCall{{
		ActorRef:   resources.ActorRef{Atespace: suspendAtespace, Name: suspendActor},
		ActorUID:   uid,
		WorkerName: testWorkerName,
		Message:    "cloud-hypervisor exited: signal: killed",
	}}
	if diff := cmp.Diff(want, control.crashCalls); diff != "" {
		t.Errorf("crash calls mismatch (-want +got):\n%s", diff)
	}
}

func TestReportActorCrashed_Refused(t *testing.T) {
	st, cleanup := storetest.SetupTestStore(t)
	defer cleanup()
	seedReportedWorker(t, st, testNode, &ateapi.WorkerResources{Actors: 1})
	actor := seedHostedActor(t, st, testWorkerName)
	uid := actor.GetMetadata().GetUid()
	authed := ateletauthtest.ContextWith(ateletauthtest.CertOn(t, testNode))

	noMessage := crashRequest(uid)
	noMessage.Message = ""
	longMessage := crashRequest(uid)
	longMessage.Message = strings.Repeat("x", 4097)

	tests := []struct {
		name string
		ctx  context.Context
		req  *ateapi.ReportActorCrashedRequest
		want codes.Code
	}{
		{"unauthenticated", ateletauthtest.ContextWith(nil), crashRequest(uid), codes.Unauthenticated},
		{"worker on another node", ateletauthtest.ContextWith(ateletauthtest.CertOn(t, "some-other-node")), crashRequest(uid), codes.NotFound},
		{"stale actor incarnation", authed, crashRequest("3b9f1e77-2c4d-4a80-91be-6d5c8f0a7e21"), codes.NotFound},
		{"no message", authed, noMessage, codes.InvalidArgument},
		{"message too long", authed, longMessage, codes.InvalidArgument},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			control := &fakeControl{}
			s := New(st, control, testAteletSPIFFEID, nil, "", nil)
			_, err := s.ReportActorCrashed(tc.ctx, tc.req)
			if got := apierror.Code(err); got != tc.want {
				t.Fatalf("code = %v (err %v), want %v", got, err, tc.want)
			}
			if len(control.crashCalls) != 0 {
				t.Errorf("refused report still reached the crash: %v", control.crashCalls)
			}
		})
	}
}

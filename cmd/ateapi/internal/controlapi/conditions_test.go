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

package controlapi

import (
	"testing"
	"time"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestSetCondition(t *testing.T) {
	statusTrue := ateapipb.ConditionStatus_CONDITION_STATUS_TRUE
	statusFalse := ateapipb.ConditionStatus_CONDITION_STATUS_FALSE

	// Seed timestamp well in the past so a moved transition time is
	// distinguishable from a preserved one.
	past := timestamppb.New(time.Now().Add(-time.Hour))
	existingReady := func(status ateapipb.ConditionStatus) []*ateapipb.Condition {
		return []*ateapipb.Condition{{
			Type:               conditionReady,
			Status:             status,
			Reason:             "Waiting",
			Message:            "not yet",
			LastTransitionTime: past,
		}}
	}

	tests := []struct {
		name                string
		conds               []*ateapipb.Condition
		condType            string
		status              ateapipb.ConditionStatus
		reason, message     string
		wantLen             int
		wantTransitionMoved bool
	}{
		{
			name:                "insert into empty",
			condType:            conditionReady,
			status:              statusFalse,
			reason:              "Waiting",
			message:             "not yet",
			wantLen:             1,
			wantTransitionMoved: true,
		},
		{
			name:                "same status updates reason and message only",
			conds:               existingReady(statusFalse),
			condType:            conditionReady,
			status:              statusFalse,
			reason:              "StillWaiting",
			message:             "still not yet",
			wantLen:             1,
			wantTransitionMoved: false,
		},
		{
			name:                "status flip moves transition time",
			conds:               existingReady(statusFalse),
			condType:            conditionReady,
			status:              statusTrue,
			reason:              "Ready",
			message:             "done",
			wantLen:             1,
			wantTransitionMoved: true,
		},
		{
			name:                "second type appends rather than overwrites",
			conds:               existingReady(statusTrue),
			condType:            conditionFailed,
			status:              statusTrue,
			reason:              reasonGoldenActorCrashed,
			message:             "boom",
			wantLen:             2,
			wantTransitionMoved: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			conds := tc.conds
			setCondition(&conds, tc.condType, tc.status, tc.reason, tc.message)

			if len(conds) != tc.wantLen {
				t.Fatalf("got %d conditions, want %d", len(conds), tc.wantLen)
			}
			cond := findCondition(conds, tc.condType)
			if cond == nil {
				t.Fatalf("condition %q not found", tc.condType)
			}
			if cond.GetStatus() != tc.status || cond.GetReason() != tc.reason || cond.GetMessage() != tc.message {
				t.Errorf("got %v/%q/%q, want %v/%q/%q",
					cond.GetStatus(), cond.GetReason(), cond.GetMessage(),
					tc.status, tc.reason, tc.message)
			}
			if got, want := conditionIsTrue(conds, tc.condType), tc.status == statusTrue; got != want {
				t.Errorf("conditionIsTrue = %v, want %v", got, want)
			}
			if cond.GetLastTransitionTime() == nil {
				t.Fatal("last_transition_time not set")
			}
			if moved := cond.GetLastTransitionTime().AsTime().After(past.AsTime()); moved != tc.wantTransitionMoved {
				t.Errorf("transition time moved = %v, want %v", moved, tc.wantTransitionMoved)
			}
			// Conditions of other types must survive untouched.
			for _, orig := range tc.conds {
				if orig.GetType() == tc.condType {
					continue
				}
				if got := findCondition(conds, orig.GetType()); got == nil || got.GetStatus() != orig.GetStatus() {
					t.Errorf("pre-existing condition %q was dropped or modified", orig.GetType())
				}
			}
		})
	}
}

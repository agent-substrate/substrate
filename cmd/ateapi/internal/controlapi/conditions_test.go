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

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestSetCondition(t *testing.T) {
	statusTrue := ateapipb.ConditionStatus_CONDITION_STATUS_TRUE
	statusFalse := ateapipb.ConditionStatus_CONDITION_STATUS_FALSE

	var conds []*ateapipb.Condition
	setCondition(&conds, conditionReady, statusFalse, "Waiting", "not yet")
	if len(conds) != 1 {
		t.Fatalf("got %d conditions, want 1", len(conds))
	}
	if conditionIsTrue(conds, conditionReady) {
		t.Error("Ready is true, want false")
	}
	firstTransition := findCondition(conds, conditionReady).GetLastTransitionTime()
	if firstTransition == nil {
		t.Fatal("last_transition_time not set on insert")
	}

	// Same status: reason and message update, transition time does not.
	earlier := timestamppb.New(firstTransition.AsTime().Add(-1))
	findCondition(conds, conditionReady).LastTransitionTime = earlier
	setCondition(&conds, conditionReady, statusFalse, "StillWaiting", "still not yet")
	cond := findCondition(conds, conditionReady)
	if cond.GetReason() != "StillWaiting" || cond.GetMessage() != "still not yet" {
		t.Errorf("reason/message = %q/%q, want updated", cond.GetReason(), cond.GetMessage())
	}
	if !cond.GetLastTransitionTime().AsTime().Equal(earlier.AsTime()) {
		t.Error("last_transition_time moved without a status flip")
	}

	// Status flip: transition time moves.
	setCondition(&conds, conditionReady, statusTrue, "Ready", "done")
	if got := findCondition(conds, conditionReady).GetLastTransitionTime().AsTime(); !got.After(earlier.AsTime()) {
		t.Error("last_transition_time did not move on status flip")
	}
	if !conditionIsTrue(conds, conditionReady) {
		t.Error("Ready is not true after flip")
	}

	// A second type appends rather than overwrites.
	setCondition(&conds, conditionFailed, statusTrue, reasonGoldenActorCrashed, "boom")
	if len(conds) != 2 {
		t.Fatalf("got %d conditions, want 2", len(conds))
	}
	if !conditionIsTrue(conds, conditionFailed) || !conditionIsTrue(conds, conditionReady) {
		t.Error("expected both Ready and Failed true")
	}
}

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
	"strings"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// ActorTemplate conditions. Both are terminal: the reconciler derives its
// progress from the golden actor itself and only records the outcome.
const (
	conditionReady  = "Ready"
	conditionFailed = "Failed"
)

// setCondition upserts the condition by type, mirroring Kubernetes
// meta.SetStatusCondition: reason and message always update, but
// last_transition_time only moves when the status value flips.
func setCondition(conds *[]*ateapipb.Condition, condType string, condStatus ateapipb.ConditionStatus, reason, message string) {
	existing := findCondition(*conds, condType)
	if existing == nil {
		*conds = append(*conds, &ateapipb.Condition{
			Type:               condType,
			Status:             condStatus,
			Reason:             reason,
			Message:            message,
			LastTransitionTime: timestamppb.Now(),
		})
		return
	}
	if existing.GetStatus() != condStatus {
		existing.LastTransitionTime = timestamppb.Now()
	}
	existing.Status = condStatus
	existing.Reason = reason
	existing.Message = message
}

func findCondition(conds []*ateapipb.Condition, condType string) *ateapipb.Condition {
	for _, cond := range conds {
		if cond.GetType() == condType {
			return cond
		}
	}
	return nil
}

func conditionIsTrue(conds []*ateapipb.Condition, condType string) bool {
	cond := findCondition(conds, condType)
	return cond.GetStatus() == ateapipb.ConditionStatus_CONDITION_STATUS_TRUE
}

// conditionsSummary renders conditions as "Type=Status,..." for logging.
func conditionsSummary(conds []*ateapipb.Condition) string {
	if len(conds) == 0 {
		return "<none>"
	}
	parts := make([]string, 0, len(conds))
	for _, cond := range conds {
		status := strings.TrimPrefix(cond.GetStatus().String(), "CONDITION_STATUS_")
		parts = append(parts, cond.GetType()+"="+status)
	}
	return strings.Join(parts, ",")
}

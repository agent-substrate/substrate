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
	"github.com/agent-substrate/substrate/internal/proto/ateletpb"
	"github.com/agent-substrate/substrate/internal/resources"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

// promotionStep is one set step of a SnapshotPromotionPolicy: the store
// option it promotes the snapshot to, and the policy field it came from.
type promotionStep struct {
	field string
	to    ateletpb.SnapshotStoreOption
	step  *ateletpb.SnapshotPromotionStep
}

// promotionSteps returns the policy's set steps in the order they run.
func promotionSteps(policy *ateletpb.SnapshotPromotionPolicy) []promotionStep {
	var steps []promotionStep
	if s := policy.GetDurableOnly(); s != nil {
		steps = append(steps, promotionStep{"durable_only", ateletpb.SnapshotStoreOption_SNAPSHOT_STORE_OPTION_DURABLE_ONLY, s})
	}
	return steps
}

// checkpointStoreOption returns the store option Checkpoint commits the
// snapshot at before it returns: LOCAL_ONLY, promoted through the policy's
// zero-TTL steps up to the first step that waits.
func checkpointStoreOption(req *ateletpb.CheckpointRequest) ateletpb.SnapshotStoreOption {
	opt := ateletpb.SnapshotStoreOption_SNAPSHOT_STORE_OPTION_LOCAL_ONLY
	for _, s := range promotionSteps(req.GetPromotionPolicy()) {
		if s.step.GetTtlSeconds() != 0 {
			break
		}
		opt = s.to
	}
	return opt
}

// hasBackgroundPromotion reports whether the policy has a step that would run
// after Checkpoint returns: any step that waits, and every step after it.
func hasBackgroundPromotion(policy *ateletpb.SnapshotPromotionPolicy) bool {
	for _, s := range promotionSteps(policy) {
		if s.step.GetTtlSeconds() != 0 {
			return true
		}
	}
	return false
}

// validatePromotionPolicy checks a CheckpointRequest's promotion policy: TTLs
// cannot be negative, and a policy with any step needs the snapshot's durable
// destination.
func validatePromotionPolicy(policy *ateletpb.SnapshotPromotionPolicy, snap *ateletpb.Snapshot) field.ErrorList {
	var errs field.ErrorList
	steps := promotionSteps(policy)
	for _, s := range steps {
		if ttl := s.step.GetTtlSeconds(); ttl < 0 {
			errs = append(errs, field.Invalid(field.NewPath("promotion_policy", s.field, "ttl_seconds"), ttl, "must not be negative"))
		}
	}
	if len(steps) > 0 {
		uri := snap.GetObject().GetSnapshotUri()
		if _, err := resources.ParseSnapshotURI(uri); err != nil {
			errs = append(errs, field.Invalid(field.NewPath("snapshot", "object", "snapshot_uri"), uri, err.Error()))
		}
	}
	return errs
}

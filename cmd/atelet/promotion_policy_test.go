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
	"testing"

	"github.com/agent-substrate/substrate/internal/apierror"
	"github.com/agent-substrate/substrate/internal/proto/ateletpb"
	"google.golang.org/grpc/codes"
)

// durableOnlyPolicy uploads the snapshot during Checkpoint and keeps no local
// copy, as a suspend asks.
func durableOnlyPolicy() *ateletpb.SnapshotPromotionPolicy {
	return &ateletpb.SnapshotPromotionPolicy{DurableOnly: promotionStepAfter(0)}
}

// localAndDurablePolicy uploads the snapshot during Checkpoint and keeps the
// local copy.
func localAndDurablePolicy() *ateletpb.SnapshotPromotionPolicy {
	return &ateletpb.SnapshotPromotionPolicy{LocalAndDurable: promotionStepAfter(0)}
}

func promotionStepAfter(ttlSeconds int64) *ateletpb.SnapshotPromotionStep {
	return &ateletpb.SnapshotPromotionStep{TtlSeconds: ttlSeconds}
}

// TestCheckpointStoreOption pins the store option Checkpoint commits for each
// shape of policy, and whether any step is left for the background.
func TestCheckpointStoreOption(t *testing.T) {
	const (
		localOnly       = ateletpb.SnapshotStoreOption_SNAPSHOT_STORE_OPTION_LOCAL_ONLY
		localAndDurable = ateletpb.SnapshotStoreOption_SNAPSHOT_STORE_OPTION_LOCAL_AND_DURABLE
		durableOnly     = ateletpb.SnapshotStoreOption_SNAPSHOT_STORE_OPTION_DURABLE_ONLY
	)
	tests := []struct {
		name           string
		policy         *ateletpb.SnapshotPromotionPolicy
		want           ateletpb.SnapshotStoreOption
		wantBackground bool
	}{
		{"unset", nil, localOnly, false},
		{"no steps", &ateletpb.SnapshotPromotionPolicy{}, localOnly, false},
		{"local_and_durable 0s", localAndDurablePolicy(), localAndDurable, false},
		{"durable_only 0s", durableOnlyPolicy(), durableOnly, false},
		{
			"local_and_durable 0s, durable_only 0s",
			&ateletpb.SnapshotPromotionPolicy{LocalAndDurable: promotionStepAfter(0), DurableOnly: promotionStepAfter(0)},
			durableOnly, false,
		},
		{
			"local_and_durable 0s, durable_only 3600s",
			&ateletpb.SnapshotPromotionPolicy{LocalAndDurable: promotionStepAfter(0), DurableOnly: promotionStepAfter(3600)},
			localAndDurable, true,
		},
		{
			"local_and_durable 600s",
			&ateletpb.SnapshotPromotionPolicy{LocalAndDurable: promotionStepAfter(600)},
			localOnly, true,
		},
		{
			"local_and_durable 600s, durable_only 0s",
			&ateletpb.SnapshotPromotionPolicy{LocalAndDurable: promotionStepAfter(600), DurableOnly: promotionStepAfter(0)},
			localOnly, true,
		},
		{
			"durable_only 600s",
			&ateletpb.SnapshotPromotionPolicy{DurableOnly: promotionStepAfter(600)},
			localOnly, true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req := &ateletpb.CheckpointRequest{PromotionPolicy: tc.policy}
			if got := checkpointStoreOption(req); got != tc.want {
				t.Errorf("checkpointStoreOption = %v, want %v", got, tc.want)
			}
			if got := hasBackgroundPromotion(tc.policy); got != tc.wantBackground {
				t.Errorf("hasBackgroundPromotion = %v, want %v", got, tc.wantBackground)
			}
		})
	}
}

// TestCheckpointRejectsBackgroundPromotion confirms Checkpoint refuses a step
// it would have to run after returning, before it touches its (here nil)
// dependencies or the actor's on-node state: reaching either fails some other
// way, so the workload is never checkpointed.
func TestCheckpointRejectsBackgroundPromotion(t *testing.T) {
	useTempNodeDirs(t)
	s := &AteomHerder{}
	for name, policy := range map[string]*ateletpb.SnapshotPromotionPolicy{
		"local_and_durable waits": {LocalAndDurable: promotionStepAfter(600)},
		"durable_only waits":      {DurableOnly: promotionStepAfter(600)},
		"durable_only waits after a synchronous local_and_durable": {
			LocalAndDurable: promotionStepAfter(0),
			DurableOnly:     promotionStepAfter(3600),
		},
	} {
		t.Run(name, func(t *testing.T) {
			req := validCheckpointRequest()
			req.PromotionPolicy = policy
			_, err := s.Checkpoint(t.Context(), req)
			if code := apierror.Code(err); code != codes.Unimplemented {
				t.Errorf("Checkpoint returned %v (code %v), want Unimplemented", err, code)
			}
		})
	}
}

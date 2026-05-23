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

package router

import (
	"context"
	"time"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"golang.org/x/sync/singleflight"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/apimachinery/pkg/util/wait"
)

// ActorResumer coordinates safe, deduplicated resumption of actors.
type ActorResumer struct {
	apiClient ateapipb.ControlClient
	flight    singleflight.Group
}

func NewActorResumer(apiClient ateapipb.ControlClient) *ActorResumer {
	return &ActorResumer{
		apiClient: apiClient,
	}
}

// ResumeActor ensures the requested actor is running. It deduplicates concurrent
// requests within the process and retries when needed.
func (r *ActorResumer) ResumeActor(ctx context.Context, actorID string) (*ateapipb.Actor, error) {
	ch := r.flight.DoChan(actorID, func() (interface{}, error) {
		// We detach the context from the first caller using a fixed background timeout.
		// This guarantees that if Caller 1 disconnects or times out, the underlying
		// resume operation continues running for Caller 2 and Caller 3 without failing.
		bgCtx, bgCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer bgCancel()

		backoff := wait.Backoff{
			Steps:    7,
			Duration: 200 * time.Millisecond,
			Factor:   1.5,
			Jitter:   0.2,
		}

		var resumeResp *ateapipb.ResumeActorResponse

		err := wait.ExponentialBackoffWithContext(bgCtx, backoff, func(ctx context.Context) (bool, error) {
			var err error
			resumeResp, err = r.apiClient.ResumeActor(ctx, &ateapipb.ResumeActorRequest{
				ActorId: actorID,
			})
			if err == nil {
				return true, nil
			}

			if status.Code(err) == codes.Aborted {
				return false, nil // Concurrent resume call, retry.
			}
			if status.Code(err) == codes.NotFound {
				return false, notFoundErr
			}
			return false, err
		})

		if err != nil {
			return nil, err
		}

		return resumeResp.GetActor(), nil
	})

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case res := <-ch:
		if res.Err != nil {
			return nil, res.Err
		}
		return res.Val.(*ateapipb.Actor), nil
	}
}

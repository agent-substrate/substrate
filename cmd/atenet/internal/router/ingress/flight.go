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

package ingress

import (
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// resumeFlight is one in-flight, per-actor resume that concurrent requests
// share. It replaces a singleflight.Group entry so callers can observe the
// flight mid-run: parked exposes the park transition, which singleflight's
// result-only channel cannot. Its lifecycle is park (at most once, flight
// goroutine only) followed by ActorResumer.publish (exactly once).
type resumeFlight struct {
	// parked is closed at most once, by park, at the flight's first retryable
	// error — the moment the flight stops resolving and starts waiting. A
	// caller woken by it (or attaching after it) must hold a parking-lot slot
	// to keep waiting. Never closed on the fast path.
	parked chan struct{}
	// parkedSignaled guards the parked close. Only the flight goroutine calls
	// park, so a plain bool suffices.
	parkedSignaled bool
	// done is closed exactly once, by publish, after result is written and the
	// flight is deleted from the registry. The write-result-then-close order
	// is what lets every caller read result without further synchronization;
	// the delete-then-close order is what keeps a completed flight unjoinable
	// (the next request for the actor starts a fresh flight).
	done chan struct{}
	// result is the shared outcome, written exactly once before done closes.
	result *resumeCallResult
}

func newResumeFlight() *resumeFlight {
	return &resumeFlight{parked: make(chan struct{}), done: make(chan struct{})}
}

// park signals the flight's park transition: it stopped resolving and started
// waiting, so from here on callers must hold parking-lot slots to keep
// waiting. Idempotent; only the flight goroutine may call it.
func (f *resumeFlight) park() {
	if f.parkedSignaled {
		return
	}
	f.parkedSignaled = true
	close(f.parked)
}

// callerResult classifies f's completed outcome for one caller. It must only
// be called after f.done is closed.
func (f *resumeFlight) callerResult(reqID uint64) (*ateapipb.Actor, ResumeOutcome, error) {
	res := f.result
	if res == nil {
		return nil, ResumeOutcomeNone, status.Error(codes.Internal, "resume call returned nil result")
	}

	// On error, return ResumeOutcomeNone ("none") so the failure is tagged
	// under the 'outcome' label rather than misreported as an activation.
	if res.err != nil {
		return nil, ResumeOutcomeNone, res.err
	}

	// Disambiguate the shared-flight resume outcome:
	// - ResumeOutcomeNone ("none"): resumed == false, actor was already active/running.
	// - ResumeOutcomeTriggered ("triggered"): Cold activation leader (resumed == true, caller's reqID == leaderID).
	// - ResumeOutcomeJoined ("joined"): Cold activation joiner (resumed == true, caller's reqID != leaderID).
	outcome := ResumeOutcomeNone
	if res.resumed {
		if res.leaderID == reqID {
			outcome = ResumeOutcomeTriggered
		} else {
			outcome = ResumeOutcomeJoined
		}
	}

	return res.actor, outcome, nil
}

// publish completes f with result and makes it unjoinable. The order is
// load-bearing: result before done (the channel close is what makes the write
// visible to callers), and registry delete before done (so no caller can
// attach to a completed flight — the next request for the actor starts a
// fresh one, preserving forget-on-completion semantics).
func (r *ActorResumer) publish(f *resumeFlight, key string, result *resumeCallResult) {
	f.result = result
	r.mu.Lock()
	delete(r.flights, key)
	r.mu.Unlock()
	close(f.done)
}

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

// Package ateomsuspend asks the control plane, through the node-local atelet,
// to suspend an actor an ateom hosts.
//
// This is the upward half of a suspend: the request travels ateom -> atelet ->
// ateapi, and the suspend the control plane decides to run travels back down
// the ordinary path as a Checkpoint into the ateom that asked. A caller must
// therefore hold no lock that Checkpoint needs while a request is in flight.
package ateomsuspend

import (
	"context"
	"crypto/tls"
	"fmt"
	"log/slog"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/apimachinery/pkg/util/wait"

	"github.com/agent-substrate/substrate/internal/ateletdial"
	"github.com/agent-substrate/substrate/internal/proto/ateletpb"
)

// requestTimeout bounds one attempt. It is generous because the control plane
// answers only once the suspend it decided to run has finished, which includes
// checkpointing the actor and uploading its snapshot.
const requestTimeout = 5 * time.Minute

// How long a request keeps trying to reach a control plane that is only
// momentarily out of reach: about fifteen seconds over six attempts.
//
// Short on purpose. An idleness signal is a fact about the past, and it decays:
// the longer a request is retried, the likelier it is to land on an actor that
// has since picked up work, and suspend cancels in-flight requests rather than
// draining them. The budget is therefore sized to ride out a blip -- an atelet
// restarting, an ateapi rolling -- and not to wait out an outage.
const (
	retryInterval = 500 * time.Millisecond
	retrySteps    = 6
	retryFactor   = 2.0
	retryJitter   = 0.2
)

func retryBackoff() wait.Backoff {
	return wait.Backoff{
		Steps:    retrySteps,
		Duration: retryInterval,
		Factor:   retryFactor,
		Jitter:   retryJitter,
	}
}

// Config is what an ateom needs to reach the atelet on its node.
type Config struct {
	SocketPath           string
	CredentialBundlePath string
	TrustBundlePath      string
	// AteletSPIFFEID is the identity the node-local atelet must present. It
	// names atelet's namespace, not this worker's, so it is configured rather
	// than derived from the downward API.
	AteletSPIFFEID string
}

// Requester asks for suspends of the actors one ateom hosts.
type Requester struct {
	socketPath string
	tlsConfig  *tls.Config
}

// NewRequester loads the ateom's credentials once, at startup, so a
// misconfiguration surfaces there rather than at the first request.
func NewRequester(cfg Config) (*Requester, error) {
	tlsConfig, err := ateletdial.TLSConfig(cfg.CredentialBundlePath, cfg.TrustBundlePath, cfg.AteletSPIFFEID)
	if err != nil {
		return nil, fmt.Errorf("suspend requester: %w", err)
	}
	return &Requester{socketPath: cfg.SocketPath, tlsConfig: tlsConfig}, nil
}

// Actor names the actor to suspend. The UID pins the request to the
// incarnation the ateom is hosting, so a request cannot outlive its actor and
// suspend a recreation that took the same name.
type Actor struct {
	Atespace string
	Name     string
	UID      string
}

// RequestSuspend asks that the named actor be suspended, and reports what the
// control plane answered.
//
// It retries a request that could not be delivered, within the budget above.
// The caller may have only one chance to ask -- an actor that pushes an
// idleness signal does not necessarily push it again -- so a transient failure
// to reach the control plane must not be the reason a slot goes unreclaimed.
// The caller's own context bounds the whole thing.
//
// A refusal is not retried. The control plane declining -- the actor is no
// longer assigned to this worker, or a resume, pause, or delete won the race --
// is an answer, and asking again cannot change it.
func (r *Requester) RequestSuspend(ctx context.Context, actor Actor) error {
	// A fresh connection per request picks up rotated worker credentials and
	// re-verifies atelet. Requests are rare enough that the handshake costs
	// nothing next to the suspend it asks for. Retries share it: gRPC
	// reconnects underneath, reloading the credentials as it does.
	conn, err := ateletdial.Dial(r.socketPath, r.tlsConfig)
	if err != nil {
		return fmt.Errorf("dial atelet: %w", err)
	}
	defer conn.Close()
	return requestWithRetry(ctx, conn, actor, retryBackoff())
}

// requestWithRetry sends the request until it is answered, the budget runs
// out, or ctx ends. The connection and backoff are parameters so the loop can
// be exercised without a socket or certificates.
func requestWithRetry(ctx context.Context, conn grpc.ClientConnInterface, actor Actor, backoff wait.Backoff) error {
	var lastErr error
	err := wait.ExponentialBackoffWithContext(ctx, backoff, func(context.Context) (bool, error) {
		err := requestOnce(ctx, conn, actor)
		switch {
		case err == nil:
			return true, nil
		case retryable(err):
			lastErr = err // remember it in case the budget runs out
			slog.WarnContext(ctx, "Retrying an actor suspend request",
				slog.String("actor_atespace", actor.Atespace),
				slog.String("actor_name", actor.Name),
				slog.Any("err", err))
			return false, nil
		default:
			return false, err
		}
	})
	// The budget ran out while the control plane was still out of reach.
	// Report what kept failing rather than the generic wait error, which says
	// only that time passed.
	if wait.Interrupted(err) && lastErr != nil {
		return fmt.Errorf("giving up on suspend request after %d attempts: %w", backoff.Steps, lastErr)
	}
	return err
}

// retryable reports whether err is the control plane being briefly out of
// reach or briefly busy, rather than the control plane answering.
//
// Unavailable is atelet or ateapi restarting, and ResourceExhausted is
// momentary backpressure. Aborted is contention on the actor itself, which
// ateapi returns asking to be retried. It has two sources: another operation
// holds the actor's lease, or a write lost to a concurrent one. The lease is
// the likely one here, because a suspend takes it before it does anything
// else, and it is worth waiting out -- whatever holds it finishes, and the
// retry re-runs every precondition against fresh state, so a pause or delete
// that won the race is refused rather than overridden.
//
// Everything else is left alone. NotFound and FailedPrecondition are
// decisions; PermissionDenied, Unauthenticated, and InvalidArgument are bugs
// or misconfiguration that another attempt only repeats. DeadlineExceeded is
// deliberately not retried either: an attempt reaches it only by outliving
// requestTimeout, which means the suspend itself is slow rather than
// undelivered, and re-sending would multiply a five-minute wait.
func retryable(err error) bool {
	switch status.Code(err) {
	case codes.Unavailable, codes.Aborted, codes.ResourceExhausted:
		return true
	default:
		return false
	}
}

// requestOnce is the call itself, over an already-open connection.
func requestOnce(ctx context.Context, conn grpc.ClientConnInterface, actor Actor) error {
	callCtx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()
	_, err := ateletpb.NewAteomSupportClient(conn).RequestActorSuspend(callCtx, &ateletpb.RequestActorSuspendRequest{
		ActorAtespace: actor.Atespace,
		ActorName:     actor.Name,
		ActorUid:      actor.UID,
	})
	return err
}

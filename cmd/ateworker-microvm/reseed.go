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
	"errors"
	"fmt"
	"log/slog"
	"time"
)

// reseedTimeout bounds the whole reseed, across attempts. It needs to cover a whole
// resume wave, not a single quiet guest.
const reseedTimeout = 60 * time.Second

// A restored guest sometimes leaves a reseed request unanswered for seconds while a
// new connection answers at once. A healthy guest answers in milliseconds, so each
// attempt is short and every attempt after the first goes out on a fresh connection.
const (
	reseedAttemptTimeout = 1500 * time.Millisecond
	// The least time between attempts, so one that fails at once (an agent error, a
	// refused dial) doesn't turn the budget into a busy loop.
	reseedRetryPause = 100 * time.Millisecond
)

// randomDevReseeder is the part of *kata.AgentClient that reseedGuestCRNG
// uses, so tests can fake it.
type randomDevReseeder interface {
	ReseedRandomDev(ctx context.Context, data []byte) error
}

// agentReseeder is a fresh kata-agent connection that a retry dials and closes.
type agentReseeder interface {
	randomDevReseeder
	Close() error
}

// agentDialer returns a dial func for fresh connections to the agent at vsockPath. A
// guest found stopped while dialing ends in errGuestStopped (see dialAgentRetry).
func agentDialer(vsockPath string) func(context.Context) (agentReseeder, error) {
	return func(ctx context.Context) (agentReseeder, error) {
		ac, err := dialAgentRetry(ctx, vsockPath, reseedAttemptTimeout)
		if err != nil {
			// A nil *kata.AgentClient in the interface would not compare equal to nil.
			return nil, err
		}
		return ac, nil
	}
}

// Outcomes of one reseed attempt, as logged.
const (
	reseedOutcomeOK        = "ok"
	reseedOutcomeTimeout   = "timeout"
	reseedOutcomeError     = "error"
	reseedOutcomeDialError = "dial_error"
	reseedOutcomeCanceled  = "canceled"
)

// reseedGuestCRNG mixes fresh, per-restore entropy into the restored guest's kernel
// CRNG through the kata-agent (see the call site in restoreMemoryFidelity for why a restore
// needs this). The nonce is a throwaway; its only job is to differ between restores so
// that clones of one snapshot diverge instead of producing identical randomness. The
// caller owns ac: it stays open for log forwarding and guest stats. Retries dial fresh
// connections with dial. actorUID only labels the log lines.
func reseedGuestCRNG(ctx context.Context, actorUID string, ac randomDevReseeder, dial func(context.Context) (agentReseeder, error)) error {
	nonce, err := newReseedNonce()
	if err != nil {
		return err
	}
	return reseedWithRetries(ctx, actorUID, ac, dial, nonce, reseedAttemptTimeout, reseedTimeout)
}

// reseedWithRetries sends nonce in attempts of at most attemptTimeout each until one
// succeeds or budget runs out. Attempt 1 uses first, which the caller owns and which is
// never closed here. Every later attempt dials a fresh connection inside its own
// attemptTimeout and closes it when the attempt ends. Re-sending is safe: mixing more
// entropy into the CRNG is idempotent, and a late answer to an abandoned attempt is
// ignored.
func reseedWithRetries(ctx context.Context, actorUID string, first randomDevReseeder, dial func(context.Context) (agentReseeder, error),
	nonce []byte, attemptTimeout, budget time.Duration) error {
	bctx, cancel := context.WithTimeout(ctx, budget)
	defer cancel()
	start := time.Now()
	for attempt := 1; ; attempt++ {
		sent := time.Now()
		outcome, err := reseedAttempt(bctx, attempt, first, dial, nonce, attemptTimeout)
		took := time.Since(sent)
		if err == nil {
			slog.InfoContext(ctx, "Guest CRNG reseeded", slog.String("id", actorUID),
				slog.Int("attempts", attempt), slog.Duration("total", time.Since(start)))
			return nil
		}
		slog.InfoContext(ctx, "Guest CRNG reseed attempt failed", slog.String("id", actorUID),
			slog.Int("attempt", attempt), slog.Duration("took", took), slog.String("outcome", outcome), slog.Any("err", err))
		switch {
		case ctx.Err() != nil:
			// The caller gave up.
			return errors.Join(ctx.Err(), err)
		case errors.Is(err, errGuestStopped):
			// The VMM is gone, so no later attempt can succeed. Wrapped with %v so the
			// caller still sees a retryable error, as before (see restoreGuestReadyError).
			return fmt.Errorf("guest stopped during the CRNG reseed: %v", err)
		}
		if took < reseedRetryPause {
			select {
			case <-bctx.Done():
			case <-time.After(reseedRetryPause - took):
			}
		}
		if bctx.Err() != nil {
			err = errors.Join(bctx.Err(), err)
			slog.WarnContext(ctx, "Guest CRNG reseed gave up", slog.String("id", actorUID),
				slog.Int("attempts", attempt), slog.Duration("total", time.Since(start)), slog.Any("err", err))
			return fmt.Errorf("no successful answer in %v over %d attempts: %w", budget, attempt, err)
		}
	}
}

// reseedAttempt makes one attempt, bounded by attemptTimeout and by ctx: on first for
// attempt 1, otherwise on a connection it dials and closes.
func reseedAttempt(ctx context.Context, attempt int, first randomDevReseeder,
	dial func(context.Context) (agentReseeder, error), nonce []byte, attemptTimeout time.Duration) (string, error) {
	actx, cancel := context.WithTimeout(ctx, attemptTimeout)
	defer cancel()
	r := first
	if attempt > 1 {
		c, err := dial(actx)
		if err != nil {
			return attemptOutcome(ctx, actx, reseedOutcomeDialError), fmt.Errorf("dialing a fresh kata-agent connection: %w", err)
		}
		defer func() { _ = c.Close() }()
		r = c
	}
	if err := r.ReseedRandomDev(actx, nonce); err != nil {
		return attemptOutcome(ctx, actx, reseedOutcomeError), err
	}
	return reseedOutcomeOK, nil
}

// attemptOutcome names a failed attempt: canceled when the caller's ctx was canceled,
// timeout when the attempt's deadline (or the budget) passed, otherwise failed.
func attemptOutcome(ctx, actx context.Context, failed string) string {
	switch {
	case errors.Is(ctx.Err(), context.Canceled):
		return reseedOutcomeCanceled
	case actx.Err() != nil:
		return reseedOutcomeTimeout
	default:
		return failed
	}
}

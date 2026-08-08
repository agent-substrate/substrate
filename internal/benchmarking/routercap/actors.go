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

// The pool of ate actors the load is aimed at: creating them, warming every one
// before the ladder starts, and suspending and deleting them afterwards.

package routercap

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Every actor is created and resumed during setup and left running for the
// whole arm, so the ladder measures steady-state routing to a warm actor. A
// cold resume costs seconds and would swamp the router's own microseconds.
const (
	// actorTemplateNamespace and actorTemplateName address the existing glutton
	// workload, installed by benchmarking/workloads.
	actorTemplateNamespace = "benchmark-workloads"
	actorTemplateName      = "glutton"
	// actorDomain is the suffix of the Host header the router routes on.
	actorDomain = "actors.resources.substrate.ate.dev"
)

// Actor is one warmed actor and the Host header that addresses it.
type Actor struct {
	Atespace string `json:"atespace"`
	Name     string `json:"name"`
	Host     string `json:"host"`
}

func (a Actor) ref() *ateapipb.ObjectRef {
	return &ateapipb.ObjectRef{Atespace: a.Atespace, Name: a.Name}
}

// ActorPool creates, warms and tears down the actors the ladder addresses.
type ActorPool struct {
	API      ateapipb.ControlClient
	Atespace string
	// Concurrency bounds setup and teardown parallelism; a cold boot is ~4s
	// per actor, so serial warming would dominate the run.
	Concurrency int
	// CallTimeout bounds one lifecycle RPC. A cold boot is the slow one.
	CallTimeout time.Duration
	Log         *slog.Logger

	mu     sync.Mutex
	actors []Actor
}

func (p *ActorPool) concurrency() int {
	if p.Concurrency > 0 {
		return p.Concurrency
	}
	return 16
}

func (p *ActorPool) timeout() time.Duration {
	if p.CallTimeout > 0 {
		return p.CallTimeout
	}
	return 60 * time.Second
}

func (p *ActorPool) log() *slog.Logger {
	if p.Log != nil {
		return p.Log
	}
	return slog.Default()
}

// Actors returns the warmed pool.
func (p *ActorPool) Actors() []Actor {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]Actor(nil), p.actors...)
}

// EnsureAtespace creates the run's atespace, treating AlreadyExists as success
// so a re-run against a half-cleaned cluster repairs rather than fails.
func (p *ActorPool) EnsureAtespace(ctx context.Context) error {
	cctx, cancel := context.WithTimeout(ctx, p.timeout())
	defer cancel()
	_, err := p.API.CreateAtespace(cctx, &ateapipb.CreateAtespaceRequest{
		Atespace: &ateapipb.Atespace{Metadata: &ateapipb.ResourceMetadata{Name: p.Atespace}},
	})
	if err != nil && status.Code(err) != codes.AlreadyExists {
		return fmt.Errorf("create atespace %q: %w", p.Atespace, err)
	}
	return nil
}

// Purge deletes every actor already in the run's atespace: each leftover from
// an earlier arm holds a worker pod, and the pool is sized one actor per pod,
// so n leftovers make the next Warm fail with "no free workers available".
// Undeletable actors are reported, not fatal — Warm's all-or-nothing check
// decides whether enough slots came back.
func (p *ActorPool) Purge(ctx context.Context) error {
	var stale []Actor
	for token := ""; ; {
		cctx, cancel := context.WithTimeout(ctx, p.timeout())
		resp, err := p.API.ListActors(cctx, &ateapipb.ListActorsRequest{
			Atespace: p.Atespace, PageSize: 1000, PageToken: token,
		})
		cancel()
		if err != nil {
			return fmt.Errorf("listing actors in %q: %w", p.Atespace, err)
		}
		for _, a := range resp.GetActors() {
			if name := a.GetMetadata().GetName(); name != "" {
				stale = append(stale, Actor{Atespace: p.Atespace, Name: name})
			}
		}
		if token = resp.GetNextPageToken(); token == "" {
			break
		}
	}
	if len(stale) == 0 {
		return nil
	}

	start := time.Now()
	var failed atomic.Int64
	_ = p.forEachAll(ctx, stale, func(ctx context.Context, a Actor) error {
		if err := p.suspend(ctx, a); err != nil {
			p.log().Warn("purge: suspend failed", "actor", a.Name, "err", err)
		}
		if err := p.delete(ctx, a); err != nil {
			p.log().Warn("purge: delete failed", "actor", a.Name, "err", err)
			failed.Add(1)
		}
		return nil
	})
	p.log().Info("purged actors left by an earlier arm",
		"found", len(stale), "undeletable", failed.Load(),
		"elapsed", time.Since(start).Round(time.Millisecond))
	return nil
}

// Warm creates n actors, resumes each with Boot set, and leaves every one
// running. All-or-nothing: a partial pool concentrates load onto fewer worker
// pods than the per-worker connection-rate guard was sized for.
func (p *ActorPool) Warm(ctx context.Context, n int) error {
	if n <= 0 {
		return fmt.Errorf("actor count must be positive, got %d", n)
	}
	if err := p.EnsureAtespace(ctx); err != nil {
		return err
	}
	if err := p.Purge(ctx); err != nil {
		return err
	}

	start := time.Now()
	actors := make([]Actor, n)
	for i := range actors {
		name := "rc-" + uuid.NewString()
		actors[i] = Actor{
			Atespace: p.Atespace,
			Name:     name,
			Host:     name + "." + p.Atespace + "." + actorDomain,
		}
	}

	var slowest atomic.Int64
	// forEachAll, not forEach: abandoned resumes wedge actors in
	// STATUS_RESUMING and cost the NEXT arm their worker slots. In-flight
	// resumes finish; the error is still returned and warming stays
	// all-or-nothing (see benchmarking/routercap/RESULTS.md).
	err := p.forEachAll(ctx, actors, func(ctx context.Context, a Actor) error {
		t := time.Now()
		if err := p.create(ctx, a); err != nil {
			return err
		}
		if err := p.resume(ctx, a, true); err != nil {
			return err
		}
		d := time.Since(t).Nanoseconds()
		for {
			hi := slowest.Load()
			if d <= hi || slowest.CompareAndSwap(hi, d) {
				break
			}
		}
		return nil
	})
	if err != nil {
		// Anything already created is cleaned up: leaving warm actors behind
		// would silently consume worker pods on the next attempt.
		p.mu.Lock()
		p.actors = actors
		p.mu.Unlock()
		p.Teardown(context.WithoutCancel(ctx))
		return fmt.Errorf("warming %d actors: %w", n, err)
	}

	p.mu.Lock()
	p.actors = actors
	p.mu.Unlock()
	p.log().Info("actor pool warm",
		"actors", n, "elapsed", time.Since(start).Round(time.Millisecond),
		"slowest_cold_boot", time.Duration(slowest.Load()).Round(time.Millisecond))
	return nil
}

// Teardown suspends and deletes every actor. Best effort and always attempted:
// actors left running hold worker pods that the next arm needs, and the run
// exits far more often through a signal than through a clean finish.
func (p *ActorPool) Teardown(ctx context.Context) {
	actors := p.Actors()
	if len(actors) == 0 {
		return
	}
	start := time.Now()
	var failed atomic.Int64
	_ = p.forEach(ctx, actors, func(ctx context.Context, a Actor) error {
		if err := p.suspend(ctx, a); err != nil {
			p.log().Warn("suspend actor failed", "actor", a.Name, "err", err)
			failed.Add(1)
		}
		if err := p.delete(ctx, a); err != nil {
			p.log().Warn("delete actor failed", "actor", a.Name, "err", err)
			failed.Add(1)
		}
		return nil
	})
	p.mu.Lock()
	p.actors = nil
	p.mu.Unlock()
	p.log().Info("actor pool torn down",
		"actors", len(actors), "failures", failed.Load(), "elapsed", time.Since(start).Round(time.Millisecond))
}

// forEachAll runs fn over every actor with bounded concurrency and returns the
// first error, letting the rest run to completion. Use it whenever fn starts
// something server-side that outlives the RPC and would strand if abandoned.
func (p *ActorPool) forEachAll(ctx context.Context, actors []Actor, fn func(context.Context, Actor) error) error {
	sem := make(chan struct{}, p.concurrency())
	var wg sync.WaitGroup
	var mu sync.Mutex
	var firstErr error

	for _, a := range actors {
		// Cancellation of the *parent* context still stops new work being
		// launched; a peer's failure does not cancel work already launched.
		select {
		case <-ctx.Done():
			wg.Wait()
			mu.Lock()
			defer mu.Unlock()
			if firstErr != nil {
				return firstErr
			}
			return ctx.Err()
		case sem <- struct{}{}:
		}
		wg.Add(1)
		go func(a Actor) {
			defer wg.Done()
			defer func() { <-sem }()
			if err := fn(ctx, a); err != nil {
				mu.Lock()
				if firstErr == nil {
					firstErr = err
				}
				mu.Unlock()
			}
		}(a)
	}
	wg.Wait()
	mu.Lock()
	defer mu.Unlock()
	return firstErr
}

// forEach runs fn over every actor with bounded concurrency, returning the
// first error and canceling the rest.
func (p *ActorPool) forEach(ctx context.Context, actors []Actor, fn func(context.Context, Actor) error) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	sem := make(chan struct{}, p.concurrency())
	var wg sync.WaitGroup
	var mu sync.Mutex
	var firstErr error

	for _, a := range actors {
		select {
		case <-ctx.Done():
			wg.Wait()
			mu.Lock()
			defer mu.Unlock()
			if firstErr != nil {
				return firstErr
			}
			return ctx.Err()
		case sem <- struct{}{}:
		}
		wg.Add(1)
		go func(a Actor) {
			defer wg.Done()
			defer func() { <-sem }()
			if err := fn(ctx, a); err != nil {
				mu.Lock()
				if firstErr == nil {
					firstErr = err
					cancel()
				}
				mu.Unlock()
			}
		}(a)
	}
	wg.Wait()
	mu.Lock()
	defer mu.Unlock()
	return firstErr
}

func (p *ActorPool) create(ctx context.Context, a Actor) error {
	cctx, cancel := context.WithTimeout(ctx, p.timeout())
	defer cancel()
	_, err := p.API.CreateActor(cctx, &ateapipb.CreateActorRequest{
		Actor: &ateapipb.Actor{
			Metadata:               &ateapipb.ResourceMetadata{Atespace: a.Atespace, Name: a.Name},
			ActorTemplateNamespace: actorTemplateNamespace,
			ActorTemplateName:      actorTemplateName,
		},
	})
	if err != nil && status.Code(err) != codes.AlreadyExists {
		return fmt.Errorf("create actor %s: %w", a.Name, err)
	}
	return nil
}

func (p *ActorPool) resume(ctx context.Context, a Actor, boot bool) error {
	cctx, cancel := context.WithTimeout(ctx, p.timeout())
	defer cancel()
	if _, err := p.API.ResumeActor(cctx, &ateapipb.ResumeActorRequest{Actor: a.ref(), Boot: boot}); err != nil {
		return fmt.Errorf("resume actor %s (boot=%v): %w", a.Name, boot, err)
	}
	return nil
}

func (p *ActorPool) suspend(ctx context.Context, a Actor) error {
	return p.settle(ctx, func(ctx context.Context) error {
		_, err := p.API.SuspendActor(ctx, &ateapipb.SuspendActorRequest{Actor: a.ref()})
		return err
	})
}

func (p *ActorPool) delete(ctx context.Context, a Actor) error {
	return p.settle(ctx, func(ctx context.Context) error {
		_, err := p.API.DeleteActor(ctx, &ateapipb.DeleteActorRequest{Actor: a.ref()})
		return err
	})
}

// settleTimeout bounds how long a teardown step waits for an actor to leave a
// transitional status. Sized off the ~3.8s cold resume with room for a queue
// behind it.
const settleTimeout = 90 * time.Second

// settle runs a teardown RPC, retrying on FailedPrecondition: teardown
// routinely races the router's in-flight ResumeActor backlog, so the refusal
// means "not yet", not "no". NotFound is success.
func (p *ActorPool) settle(ctx context.Context, call func(context.Context) error) error {
	deadline := time.Now().Add(settleTimeout)
	for attempt := 0; ; attempt++ {
		cctx, cancel := context.WithTimeout(ctx, p.timeout())
		err := call(cctx)
		cancel()
		switch {
		case err == nil, status.Code(err) == codes.NotFound:
			return nil
		case status.Code(err) != codes.FailedPrecondition:
			return err
		case time.Now().After(deadline):
			return fmt.Errorf("after %v still in a transitional status: %w", settleTimeout, err)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff(attempt)):
		}
	}
}

// backoff ramps 250ms to 2s, capped so parallel retries do not themselves
// become load on ate-api-server.
func backoff(attempt int) time.Duration {
	d := 250 * time.Millisecond << min(attempt, 3)
	return min(d, 2*time.Second)
}

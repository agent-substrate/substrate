// Copyright 2026 The Agent Substrate Authors
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

package lifecyclehook

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"strings"
	"sync"

	"go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/api/hook/v1"
	"go.ate.dev/substrate/internal/apierror"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"k8s.io/apimachinery/pkg/util/validation"
)

// CreateOutcome holds the hooks that must receive a compensating OnDeleteActor
// if the CreateActor call does not commit.
type CreateOutcome struct {
	d               *Dispatcher
	targets         []targetRunner
	meta            *ateapi.ResourceMetadata
	callerPrincipal string
}

// Rollback sends a compensating OnDeleteActor once to every hook that allowed
// the create and to every hook that errored or timed out. Safe to call on a
// nil receiver.
func (o *CreateOutcome) Rollback(ctx context.Context) {
	if o == nil || o.d == nil || len(o.targets) == 0 {
		return
	}
	targets := o.targets
	o.targets = nil
	o.d.rollbackActor(ctx, targets, o.meta, o.callerPrincipal)
}

// Hooks is the control-plane lifecycle hook interface for Actor create and
// delete. OnDeleteActor is a notification and cannot fail the deletion.
type Hooks interface {
	OnCreateActor(ctx context.Context, actor *ateapi.Actor, tmpl *ateapi.ActorTemplate, callerPrincipal string) (*CreateOutcome, error)
	OnDeleteActor(ctx context.Context, atespace, name, uid, callerPrincipal string)
	Close() error
}

type targetRunner struct {
	cfg    HookTarget
	client hook.LifecycleHookClient
	conn   *grpc.ClientConn
}

// Option configures a Dispatcher.
type Option func(*dispatcherOptions)

type dispatcherOptions struct {
	tlsOpts       TLSOptions
	clientsByHook map[string]hook.LifecycleHookClient
}

// WithTLSOptions sets the server-level certificate paths for hook targets.
func WithTLSOptions(opts TLSOptions) Option {
	return func(o *dispatcherOptions) {
		o.tlsOpts = opts
	}
}

// WithHookClient injects a pre-built LifecycleHookClient for a named hook target in tests.
func WithHookClient(hookName string, client hook.LifecycleHookClient) Option {
	return func(o *dispatcherOptions) {
		if o.clientsByHook == nil {
			o.clientsByHook = make(map[string]hook.LifecycleHookClient)
		}
		o.clientsByHook[hookName] = client
	}
}

// Dispatcher fans out lifecycle hook RPCs in parallel across configured targets.
type Dispatcher struct {
	cfg     *Config
	targets []targetRunner
	conns   []*grpc.ClientConn
}

// NewDispatcher validates cfg and dials all hook targets.
func NewDispatcher(cfg *Config, opts ...Option) (*Dispatcher, error) {
	if err := cfg.NormalizeAndValidate(); err != nil {
		return nil, err
	}
	var dOpts dispatcherOptions
	for _, opt := range opts {
		opt(&dOpts)
	}

	d := &Dispatcher{cfg: cfg}
	for _, target := range cfg.Hooks {
		runner := targetRunner{cfg: target}
		if injected, ok := dOpts.clientsByHook[target.Name]; ok {
			runner.client = injected
		} else {
			creds, err := buildTransportCredentials(target.TLS, dOpts.tlsOpts)
			if err != nil {
				_ = d.Close()
				return nil, fmt.Errorf("hook %q TLS setup: %w", target.Name, err)
			}
			conn, err := grpc.NewClient(
				normalizeDialTarget(target.Address),
				grpc.WithTransportCredentials(creds),
				grpc.WithStatsHandler(otelgrpc.NewClientHandler()),
			)
			if err != nil {
				_ = d.Close()
				return nil, fmt.Errorf("dialing hook %q (%s): %w", target.Name, target.Address, err)
			}
			conn.Connect()
			runner.conn = conn
			runner.client = hook.NewLifecycleHookClient(conn)
			d.conns = append(d.conns, conn)
		}
		d.targets = append(d.targets, runner)
	}
	return d, nil
}

// Close closes all gRPC client connections.
func (d *Dispatcher) Close() error {
	var errs []error
	for _, conn := range d.conns {
		if err := conn.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

type createResult struct {
	target targetRunner
	// allowed is set when the hook answered allowed=true, whether or not its
	// mutations passed prefix validation.
	allowed bool
	// unanswered is set when the hook errored or missed the deadline.
	unanswered bool
	mutations  map[string]string
	err        error
}

// OnCreateActor calls every hook in parallel, validates and merges annotation
// mutations into actor, and returns a CreateOutcome populated with all
// rollback targets even when err != nil.
//
// A hook blocks the create only by answering allowed=false, or by returning a
// key outside its allowed prefixes or with invalid syntax. A hook that errors
// or misses the deadline is logged and skipped.
func (d *Dispatcher) OnCreateActor(ctx context.Context, actor *ateapi.Actor, tmpl *ateapi.ActorTemplate, callerPrincipal string) (*CreateOutcome, error) {
	if d == nil || len(d.targets) == 0 {
		return &CreateOutcome{}, nil
	}

	req := &hook.OnCreateActorRequest{
		Actor:           actor,
		ActorTemplate:   tmpl,
		CallerPrincipal: callerPrincipal,
	}

	results := make([]createResult, len(d.targets))
	waveCtx, cancel := context.WithTimeout(ctx, d.cfg.WaitDuration())
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(len(d.targets))
	for i, target := range d.targets {
		go func(idx int, t targetRunner) {
			defer wg.Done()
			resp, err := t.client.OnCreateActor(waveCtx, proto.CloneOf(req))
			results[idx] = evaluateCreateResponse(waveCtx, t, resp, err)
		}(i, target)
	}
	wg.Wait()

	merged, rollbackTargets, err := mergeCreateResults(results)
	outcome := &CreateOutcome{
		d:               d,
		targets:         rollbackTargets,
		meta:            actor.GetMetadata(),
		callerPrincipal: callerPrincipal,
	}
	if err != nil {
		return outcome, err
	}
	applyMutations(&actor.Metadata.Annotations, merged)
	return outcome, nil
}

// OnDeleteActor notifies every hook in parallel. Errors and timeouts are logged.
func (d *Dispatcher) OnDeleteActor(ctx context.Context, atespace, name, uid, callerPrincipal string) {
	if d == nil || len(d.targets) == 0 {
		return
	}
	req := &hook.OnDeleteActorRequest{
		Atespace:        atespace,
		Name:            name,
		Uid:             uid,
		CallerPrincipal: callerPrincipal,
	}
	waveCtx, cancel := context.WithTimeout(ctx, d.cfg.WaitDuration())
	defer cancel()
	d.notifyDelete(waveCtx, d.targets, req, "OnDeleteActor")
}

func (d *Dispatcher) rollbackActor(ctx context.Context, targets []targetRunner, meta *ateapi.ResourceMetadata, callerPrincipal string) {
	if len(targets) == 0 || meta == nil {
		return
	}
	req := &hook.OnDeleteActorRequest{
		Atespace:        meta.GetAtespace(),
		Name:            meta.GetName(),
		Uid:             meta.GetUid(),
		CallerPrincipal: callerPrincipal,
	}
	rbCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), d.cfg.WaitDuration())
	defer cancel()
	d.notifyDelete(rbCtx, targets, req, "compensating OnDeleteActor")
}

func (d *Dispatcher) notifyDelete(ctx context.Context, targets []targetRunner, req *hook.OnDeleteActorRequest, what string) {
	var wg sync.WaitGroup
	wg.Add(len(targets))
	for _, target := range targets {
		go func(t targetRunner) {
			defer wg.Done()
			if _, err := t.client.OnDeleteActor(ctx, req); err != nil {
				slog.WarnContext(ctx, what+" hook failed; skipping", "hook", t.cfg.Name, "err", err)
			}
		}(target)
	}
	wg.Wait()
}

func evaluateCreateResponse(ctx context.Context, t targetRunner, resp *hook.OnCreateActorResponse, callErr error) createResult {
	if callErr != nil {
		slog.WarnContext(ctx, "OnCreateActor hook failed; skipping", "hook", t.cfg.Name, "err", callErr)
		return createResult{target: t, unanswered: true}
	}
	if !resp.GetAllowed() {
		msg := resp.GetStatusMessage()
		if msg == "" {
			msg = "rejected by lifecycle hook"
		}
		return createResult{
			target: t,
			err:    apierror.PermissionDenied("lifecycle hook %q denied request: %s", t.cfg.Name, msg),
		}
	}
	mutations := resp.GetMutatedAnnotations()
	for k := range mutations {
		if !hasAllowedPrefix(k, t.cfg.AllowedAnnotationPrefixes) {
			return createResult{
				target:  t,
				allowed: true,
				err:     apierror.Internal("lifecycle hook %q returned unpermitted annotation key %q", t.cfg.Name, k),
			}
		}
		if errs := validation.IsQualifiedName(k); len(errs) > 0 {
			return createResult{
				target:  t,
				allowed: true,
				err:     apierror.Internal("lifecycle hook %q returned invalid annotation key %q: %s", t.cfg.Name, k, strings.Join(errs, "; ")),
			}
		}
	}
	return createResult{target: t, allowed: true, mutations: mutations}
}

func hasAllowedPrefix(key string, prefixes []string) bool {
	for _, p := range prefixes {
		if strings.HasPrefix(key, p) {
			return true
		}
	}
	return false
}

// mergeCreateResults returns the merged mutations, the targets a rollback must
// notify (every hook that allowed or did not answer), and the first error.
func mergeCreateResults(results []createResult) (map[string]string, []targetRunner, error) {
	var rollbackTargets []targetRunner
	for _, r := range results {
		if r.allowed || r.unanswered {
			rollbackTargets = append(rollbackTargets, r.target)
		}
	}
	for _, r := range results {
		if r.err != nil {
			return nil, rollbackTargets, r.err
		}
	}
	var merged map[string]string
	for _, r := range results {
		if len(r.mutations) == 0 {
			continue
		}
		if merged == nil {
			merged = make(map[string]string, len(r.mutations))
		}
		maps.Copy(merged, r.mutations)
	}
	return merged, rollbackTargets, nil
}

func applyMutations(dst *map[string]string, mutations map[string]string) {
	if len(mutations) == 0 {
		return
	}
	if *dst == nil {
		*dst = make(map[string]string, len(mutations))
	}
	for k, v := range mutations {
		if v == "" {
			delete(*dst, k)
		} else {
			(*dst)[k] = v
		}
	}
	if len(*dst) == 0 {
		*dst = nil
	}
}
